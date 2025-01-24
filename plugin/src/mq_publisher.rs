use lapin::{
    options::{BasicPublishOptions, ExchangeDeclareOptions},
    types::FieldTable,
    BasicProperties, Connection, ExchangeKind, Result,
};
use lapin::ConnectionProperties;


#[derive(Debug)]
pub struct MQPublisher {
    channel: lapin::Channel,
    exchange_name: String,
}

impl MQPublisher {
    pub async fn new(amqp_uri: &str, exchange_name: &str) -> anyhow::Result<Self> {
        // Use Connection directly
        let conn = Connection::connect(amqp_uri, ConnectionProperties::default()).await?;

        let channel = conn.create_channel().await?;

        channel
            .exchange_declare(
                exchange_name,
                ExchangeKind::Direct,
                ExchangeDeclareOptions::default(),
                FieldTable::default(),
            )
            .await?;

        Ok(Self {
            channel,
            exchange_name: exchange_name.to_string(),
        })
    }

    pub async fn publish_message(&self, routing_key: &str, data: Vec<u8>) -> Result<()> {
        self.channel
            .basic_publish(
                &self.exchange_name,
                routing_key,
                BasicPublishOptions::default(),
                &data,
                BasicProperties::default(),
            )
            .await?;
        Ok(())
    }
}
