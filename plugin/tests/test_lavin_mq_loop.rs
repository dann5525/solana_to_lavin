use std::thread;
use std::sync::mpsc;
use quic_geyser_common::{
    channel_message::ChannelMessage,
    types::{
        slot_identifier::SlotIdentifier,
        transaction::{Transaction, TransactionMeta},
    },
    // If your "Message" struct is from quic_geyser_common::types::transaction::Message,
    // you might need to import it too:
    //   transaction::Message,
};
use solana_sdk::{hash::Hash, message::v0::{LoadedAddresses, Message as SolanaMsg}};
use quic_geyser_plugin::lavin_mq_loop::run_lavin_mq_loop;

#[test]
fn test_lavin_mq_loop_independent() {
    // 1) Create an MPSC channel
    let (tx, rx) = mpsc::channel::<ChannelMessage>();

    // 2) Spawn a thread that runs the Lavin MQ loop
    let handle = thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            // Hardcode your AMQP URL or read from test config
            let amqp_url ="amqps://dan:6b7dc305-0f23-48c6-beba-1ee20e7a2edd@polar-ram.lmq.cloudamqp.com/botcloud ";
            if let Err(e) = run_lavin_mq_loop(amqp_url, rx).await {
                eprintln!("MQ loop error: {e:?}");
            }
        });
    });

    // 3) Build a test transaction *without* using Default::
    //    Fill each field explicitly.
    let dummy_tx = Transaction {
        slot_identifier: SlotIdentifier { slot: 123 },
        signatures: vec![],  // or add real signatures
        // Construct your "message" field with actual data:
        message: SolanaMsg {
            // e.g. a minimal header
            header: solana_sdk::message::MessageHeader {
                num_required_signatures: 1,
                num_readonly_signed_accounts: 0,
                num_readonly_unsigned_accounts: 0,
            },
            account_keys: vec![],
            recent_blockhash: Hash::default(),
            instructions: vec![],
            address_table_lookups: vec![],
        },
        is_vote: false,
        transasction_meta: TransactionMeta {
            error: None,
            fee: 0,
            pre_balances: vec![],
            post_balances: vec![],
            inner_instructions: None,
            log_messages: None,
            rewards: None,
            loaded_addresses: LoadedAddresses {
                writable: vec![],
                readonly: vec![],
            },
            return_data: None,
            compute_units_consumed: None,
        },
        index: 99,
    };

    // Create a ChannelMessage::Transaction
    let channel_msg = ChannelMessage::Transaction(Box::new(dummy_tx));

    // 4) Send it to the MQ loop
    tx.send(channel_msg).expect("Failed sending test transaction to MQ loop");

    // Let the MQ loop stop by dropping the sender
    drop(tx);

    // 5) Join the thread so the test doesn't exit prematurely
    handle.join().unwrap();
    println!("Test completed OK");
}

