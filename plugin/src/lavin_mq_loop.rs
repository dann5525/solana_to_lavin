use std::sync::mpsc::Receiver;
use quic_geyser_common::channel_message::ChannelMessage;
use anyhow::Result;
use lapin::{
    options::{BasicPublishOptions, QueueDeclareOptions},
    publisher_confirm::Confirmation,
    types::FieldTable,
    BasicProperties, Connection, ConnectionProperties,
};

pub async fn run_lavin_mq_loop(amqp_url: &str, mq_rx: Receiver<ChannelMessage>) -> Result<()> {
    // 1) Connect
    let conn = Connection::connect(amqp_url, ConnectionProperties::default()).await?;
    let channel = conn.create_channel().await?;

    // 2) Declare queue
    channel
        .queue_declare(
            "transactions",
            QueueDeclareOptions::default(),
            FieldTable::default(),
        )
        .await?;

    // 3) Loop reading from mq_rx
    while let Ok(msg) = mq_rx.recv() {
        if let ChannelMessage::Transaction(tx) = msg {
            // serialize transaction
            let payload = serde_json::to_vec(&tx)?;
            let confirm = channel
                .basic_publish(
                    "",
                    "transactions",
                    BasicPublishOptions::default(),
                    &payload,
                    BasicProperties::default(),
                )
                .await?
                .await?;

            if confirm.is_nack() {
                eprintln!("Broker did not ack transaction!");
            }
        } else {
            // handle other message variants or ignore them
        }
    }
    eprintln!("mq_rx closed, shutting down lavin MQ loop");
    Ok(())
}

