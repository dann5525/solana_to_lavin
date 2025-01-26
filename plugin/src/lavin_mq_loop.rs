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
    // We'll label the outer loop so we can "continue" to it to reconnect.
    'outer: loop {
        // 1) Attempt to connect
        let conn = match Connection::connect(amqp_url, ConnectionProperties::default()).await {
            Ok(c) => c,
            Err(e) => {
                log::error!("Error connecting to AMQP: {e}, retrying in 5s...");
                sleep(Duration::from_secs(5)).await;
                continue 'outer; // Retry the outer loop
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

        // 3) Declare queue
        //    If this fails, we log and try reconnecting.
        if let Err(e) = channel
            .queue_declare(
                "transactions",
                QueueDeclareOptions::default(),
                FieldTable::default(),
            )
            .await
        {
            log::error!("Error declaring queue: {e}, retrying in 5s...");
            sleep(Duration::from_secs(5)).await;
            continue 'outer;
        }

        log::info!("Connected to AMQP and declared queue successfully.");

        // 4) Now read messages from mq_rx in a loop.
        //    If mq_rx is dropped, we break out and shut down.
        while let Ok(msg) = mq_rx.recv() {
            match msg {
                ChannelMessage::Transaction(tx) => {
                    // serialize transaction
                    let payload = match serde_json::to_vec(&tx) {
                        Ok(p) => p,
                        Err(serde_err) => {
                            log::error!("Failed to serialize transaction: {serde_err}");
                            continue; // skip this message
                        }
                    };

                    // Publish
                    let publish_result = channel
                        .basic_publish(
                            "",
                            "transactions",
                            BasicPublishOptions::default(),
                            &payload,
                            BasicProperties::default(),
                        )
                        .await;

                    // If publishing fails, log it, wait, and reconnect in outer loop
                    let confirm = match publish_result {
                        Ok(confirms) => match confirms.await {
                            Ok(confirm) => confirm,
                            Err(e) => {
                                log::error!("AMQP publish error awaiting confirm: {e}");
                                sleep(Duration::from_secs(5)).await;
                                continue 'outer; // reconnect
                            }
                        },
                        Err(e) => {
                            log::error!("AMQP publish error: {e}");
                            sleep(Duration::from_secs(5)).await;
                            continue 'outer; // reconnect
                        }
                    };

                    // If we got a NACK, also attempt reconnect.
                    if confirm.is_nack() {
                        log::error!("Broker did not ack transaction, reconnecting in 5s...");
                        sleep(Duration::from_secs(5)).await;
                        continue 'outer;
                    }
                }
                // Handle other variants if you have them
                other => {
                    // e.g. ChannelMessage::Account(...), etc.
                    log::debug!("Received a non-transaction ChannelMessage: {:?}", other);
                }
            }
        }

        // If we get here, it means `mq_rx` closed (sender is dropped).
        log::warn!("mq_rx closed, shutting down lavin MQ loop");
        // We can exit the function gracefully
        break 'outer;
    }

    Ok(())
}
