// tests/test_lavin_mq_loop_independent.rs (example test file)

use std::sync::mpsc;
use std::thread;

// IMPORTANT: Ensure these crates are declared in your Cargo.toml
// e.g. quic_geyser_common = { path = "../quic_geyser_common" }
use quic_geyser_common::{
    channel_message::ChannelMessage,
    types::{
        slot_identifier::SlotIdentifier,
        transaction::{Transaction, TransactionMeta},
    },
};

use solana_sdk::{
    hash::Hash,
    // Make sure your version of solana_sdk is >= 1.14
    message::v0::{LoadedAddresses, Message as SolanaMsg},
};

// This requires that quic_geyser_plugin has a pub mod lavin_mq_loop in its lib.rs
use quic_geyser_plugin::lavin_mq_loop::run_lavin_mq_loop;

#[test]
fn test_lavin_mq_loop_independent() {
    let (tx, rx) = mpsc::channel::<ChannelMessage>();

    let handle = thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            // ensure your AMQP URL is valid
            let amqp_url = "amqps://dan:04828406-73c5-4d05-8f25-a3a564bf00ac@polar-ram.lmq.cloudamqp.com/botcloud";
            if let Err(e) = run_lavin_mq_loop(amqp_url, rx).await {
                eprintln!("MQ loop error: {e:?}");
            }
        });
    });

    // Build dummy test transaction
    // Make sure the field name is spelled exactly as your struct says (transasction_meta vs transaction_meta)
    let dummy_tx = Transaction {
        slot_identifier: SlotIdentifier { slot: 123 },
        signatures: vec![], 
        message: SolanaMsg {
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
            // If your struct has these fields optional,
            // ensure they're declared as `Option<Vec<_>>`.
            // Or if your struct has them as `Option<...>` use None:
            pre_token_balances: None,
            post_token_balances: None,
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

    // Send it along
    let channel_msg = ChannelMessage::Transaction(Box::new(dummy_tx));
    tx.send(channel_msg).expect("Failed to send test transaction");

    drop(tx); // closes channel so loop can exit gracefully

    handle.join().unwrap();
    println!("Test completed OK");
}
