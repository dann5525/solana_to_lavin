use std::{sync::mpsc::Receiver, time::Duration};
use anyhow::Result;
use lapin::{
    options::{BasicPublishOptions, QueueDeclareOptions},
    types::FieldTable,
    BasicProperties, Connection, ConnectionProperties,
};
use quic_geyser_common::channel_message::ChannelMessage;

use tokio::time::sleep;

/// Example of a run_lavin_mq_loop with reconnection logic.
/// If the connection or publish fails, we log it, sleep, and try again.
pub async fn run_lavin_mq_loop(amqp_url: &str, mq_rx: Receiver<ChannelMessage>) -> Result<()> {
    'outer: loop {
        // 1) Connect to AMQP
        let conn = match Connection::connect(amqp_url, ConnectionProperties::default()).await {
            Ok(c) => c,
            Err(e) => {
                log::error!("Error connecting to AMQP: {e}, retrying in 5s...");
                sleep(Duration::from_secs(5)).await;
                continue 'outer;
            }
        };

        // 2) Create channel
        let channel = match conn.create_channel().await {
            Ok(ch) => ch,
            Err(e) => {
                log::error!("Error creating channel: {e}, retrying in 5s...");
                sleep(Duration::from_secs(5)).await;
                continue 'outer;
            }
        };

        // 3) Declare both queues
        for queue_name in ["transactions", "accountChanges", "blockMeta"].iter() {
            if let Err(e) = channel
                .queue_declare(
                    queue_name,
                    QueueDeclareOptions::default(),
                    FieldTable::default(),
                )
                .await
            {
                log::error!("Error declaring queue {}: {e}, retrying in 5s...", queue_name);
                sleep(Duration::from_secs(5)).await;
                continue 'outer;
            }
        }

        log::info!("Connected to AMQP and declared queues successfully.");

        // 4) Process messages
        while let Ok(msg) = mq_rx.recv() {
            match msg {
                ChannelMessage::Transaction(tx) => {
                    let payload = match serde_json::to_vec(&tx) {
                        Ok(p) => p,
                        Err(serde_err) => {
                            log::error!("Failed to serialize transaction: {serde_err}");
                            continue;
                        }
                    };

                    if let Err(e) = publish_message(&channel, "transactions", &payload).await {
                        log::error!("AMQP publish error for transaction: {e}");
                        sleep(Duration::from_secs(5)).await;
                        continue 'outer;
                    }
                }
                ChannelMessage::Account(account_data, slot, is_startup) => {
                    // Create a structure to serialize account data with metadata
                    let account_message = serde_json::json!({
                        "account": {
                            "pubkey": account_data.pubkey.to_string(),
                            "lamports": account_data.account.lamports,
                            "owner": account_data.account.owner.to_string(),
                            "executable": account_data.account.executable,
                            "rentEpoch": account_data.account.rent_epoch,
                            "data": account_data.account.data,
                        },
                        "slot": slot,
                        "isStartup": is_startup,
                        "writeVersion": account_data.write_version,
                    });

                    let payload = match serde_json::to_vec(&account_message) {
                        Ok(p) => p,
                        Err(serde_err) => {
                            log::error!("Failed to serialize account data: {serde_err}");
                            continue;
                        }
                    };

                    if let Err(e) = publish_message(&channel, "accountChanges", &payload).await {
                        log::error!("AMQP publish error for account change: {e}");
                        sleep(Duration::from_secs(5)).await;
                        continue 'outer;
                    }
                }

                ChannelMessage::BlockMeta(block_meta) => {
                    let payload = match serde_json::to_vec(&block_meta) {
                        Ok(p) => p,
                        Err(serde_err) => {
                            log::error!("Failed to serialize block metadata: {serde_err}");
                            continue;
                        }
                    };

                    if let Err(e) = publish_message(&channel, "blockMeta", &payload).await {
                        log::error!("AMQP publish error for block metadata: {e}");
                        sleep(Duration::from_secs(5)).await;
                        continue 'outer;
                    }
                }
                // Handle other message types if needed
                other => {
                    log::debug!("Received other ChannelMessage type: {:?}", other);
                }
            }
        }

        log::warn!("mq_rx closed, shutting down lavin MQ loop");
        break 'outer;
    }

    Ok(())
}

async fn publish_message(channel: &lapin::Channel, queue: &str, payload: &[u8]) -> Result<()> {
    let confirm = channel
        .basic_publish(
            "",
            queue,
            BasicPublishOptions::default(),
            payload,
            BasicProperties::default(),
        )
        .await?
        .await?;

    if confirm.is_nack() {
        return Err(anyhow::anyhow!("Broker did not acknowledge message"));
    }

    Ok(())
}
