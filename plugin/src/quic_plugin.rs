use crate::config::Config;
use crate::lavin_mq_loop::run_lavin_mq_loop;
use agave_geyser_plugin_interface::geyser_plugin_interface::{
    GeyserPlugin, GeyserPluginError, ReplicaAccountInfoVersions, ReplicaBlockInfoVersions,
    ReplicaEntryInfoVersions, ReplicaTransactionInfoVersions, Result as PluginResult, SlotStatus,
};
use quic_geyser_block_builder::block_builder::start_block_building_thread;
use quic_geyser_common::{
    channel_message::{AccountData, ChannelMessage},
    plugin_error::QuicGeyserError,
    types::{
        block_meta::BlockMeta,
        slot_identifier::SlotIdentifier,
        transaction::{Transaction, TransactionMeta, TransactionTokenBalanceSerializable}
        
    },
};
use quic_geyser_server::quic_server::QuicServer;
use solana_sdk::{
    account::Account, clock::Slot,  commitment_config::CommitmentConfig, message::v0::Message,
    pubkey::Pubkey,
};

#[derive(Debug, Default)]
pub struct QuicGeyserPlugin {
    quic_server: Option<QuicServer>,
    block_builder_channel: Option<std::sync::mpsc::Sender<ChannelMessage>>,
    rpc_server_message_channel: Option<std::sync::mpsc::Sender<ChannelMessage>>,
    // Add these fields:
    mq_sender: Option<std::sync::mpsc::Sender<ChannelMessage>>,
    mq_thread_handle: Option<std::thread::JoinHandle<()>>,
}

impl GeyserPlugin for QuicGeyserPlugin {
    fn name(&self) -> &'static str {
        "quic_geyser_plugin"
    }

    fn on_load(&mut self, config_file: &str, _is_reload: bool) -> PluginResult<()> {
        log::info!("loading quic_geyser plugin");
        let config = match Config::load_from_file(config_file) {
            Ok(config) => config,
            Err(e) => {
                log::error!("Error loading config file: {}", e);
                return Err(e);
            }
        };
        
        let compression_type = config.quic_plugin.compression_parameters.compression_type;
        let enable_block_builder = config.quic_plugin.enable_block_builder;
        let build_blocks_with_accounts = config.quic_plugin.build_blocks_with_accounts;
        log::info!("Quic plugin config correctly loaded");
        solana_logger::setup_with_default(&config.quic_plugin.log_level);
        let quic_server = QuicServer::new(config.quic_plugin).map_err(|_| {
            GeyserPluginError::Custom(Box::new(QuicGeyserError::ErrorConfiguringServer))
        })?;
        if enable_block_builder {
            // disable block building for now
            let (sx, rx) = std::sync::mpsc::channel();
            start_block_building_thread(
                rx,
                quic_server.data_channel_sender.clone(),
                compression_type,
                build_blocks_with_accounts,
            );
            self.block_builder_channel = Some(sx);
        }

        self.quic_server = Some(quic_server);

        // --- Start the MQ server thread
        let (mq_tx, mq_rx) = std::sync::mpsc::channel::<ChannelMessage>();
        self.mq_sender = Some(mq_tx);

        let amqp_url = std::env::var("AMQP_URL").unwrap_or_else(|_| config.amqp_url.clone());


        let handle = std::thread::spawn(move || {
            // Build a single-threaded tokio runtime
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Failed to build tokio runtime for MQ loop");

            rt.block_on(async move {
                // Suppose this function is your tested code
                if let Err(e) = run_lavin_mq_loop(&amqp_url, mq_rx).await {
                    // Proper error handling: log and exit
                    log::error!("Lavin MQ loop error: {e:?}");
                }
            });
        });
        self.mq_thread_handle = Some(handle);

        log::info!("geyser plugin loaded ok ()");
        Ok(())
    }

    fn on_unload(&mut self) {
        self.quic_server = None;
    }

    fn update_account(
        &self,
        account: ReplicaAccountInfoVersions,
        slot: Slot,
        is_startup: bool,
    ) -> PluginResult<()> {
        let Some(quic_server) = &self.quic_server else {
            return Ok(());
        };

        if !quic_server.quic_plugin_config.allow_accounts
            || (is_startup && !quic_server.quic_plugin_config.allow_accounts_at_startup)
        {
            return Ok(());
        }
        let ReplicaAccountInfoVersions::V0_0_3(account_info) = account else {
            return Err(GeyserPluginError::AccountsUpdateError {
                msg: "Unsupported account info version".to_string(),
            });
        };
        let account = Account {
            lamports: account_info.lamports,
            data: account_info.data.to_vec(),
            owner: Pubkey::try_from(account_info.owner).expect("valid pubkey"),
            executable: account_info.executable,
            rent_epoch: account_info.rent_epoch,
        };
        let pubkey: Pubkey = Pubkey::try_from(account_info.pubkey).expect("valid pubkey");

        let channel_message = ChannelMessage::Account(
            AccountData {
                pubkey,
                account,
                write_version: account_info.write_version,
            },
            slot,
            is_startup,
        );

        if let Some(block_channel) = &self.block_builder_channel {
            let _ = block_channel.send(channel_message.clone());
        }

        if let Some(rpc_server_message_channel) = &self.rpc_server_message_channel {
            let _ = rpc_server_message_channel.send(channel_message.clone());
        }

        quic_server
            .send_message(channel_message)
            .map_err(|e| GeyserPluginError::Custom(Box::new(e)))?;
        Ok(())
    }

    fn notify_end_of_startup(&self) -> PluginResult<()> {
        Ok(())
    }

    fn update_slot_status(
        &self,
        slot: Slot,
        parent: Option<u64>,
        status: SlotStatus,
    ) -> PluginResult<()> {
        // Todo
        let Some(quic_server) = &self.quic_server else {
            return Ok(());
        };
        let commitment_level = match status {
            SlotStatus::Processed => CommitmentConfig::processed(),
            SlotStatus::Rooted => CommitmentConfig::finalized(),
            SlotStatus::Confirmed => CommitmentConfig::confirmed(),
        };
        let slot_message = ChannelMessage::Slot(slot, parent.unwrap_or_default(), commitment_level);

        if let Some(block_channel) = &self.block_builder_channel {
            let _ = block_channel.send(slot_message.clone());
        }

        if let Some(rpc_server_message_channel) = &self.rpc_server_message_channel {
            let _ = rpc_server_message_channel.send(slot_message.clone());
        }

        quic_server
            .send_message(slot_message)
            .map_err(|e| GeyserPluginError::Custom(Box::new(e)))?;
        Ok(())
    }

    fn notify_transaction(
        &self,
        transaction: ReplicaTransactionInfoVersions,
        slot: Slot,
    ) -> PluginResult<()> {
        let Some(quic_server) = &self.quic_server else {
            return Ok(());
        };

        let ReplicaTransactionInfoVersions::V0_0_2(solana_transaction) = transaction else {
            return Err(GeyserPluginError::TransactionUpdateError {
                msg: "Unsupported transaction version".to_string(),
            });
        };

        let message = solana_transaction.transaction.message();
        let mut account_keys = vec![];

        for index in 0.. {
            let account = message.account_keys().get(index);
            match account {
                Some(account) => account_keys.push(*account),
                None => break,
            }
        }
        let pump_pubkeys = [
            "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P",
            "EEZZatWNPPsihctMcbmSSSHc5VjMbiSNGBKhyCprzYVo",
            "whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc",
            "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
            "LBUZKhRxPF3XUpBCjp4YzTKgLccjZhTSDM9YuVaPwxo",
        ].map(|key| Pubkey::try_from(key).expect("Valid pubkey"));
        
        if !pump_pubkeys.iter().any(|key| account_keys.contains(key)) {
            return Ok(());
        }

        let v0_message = Message {
            header: *message.header(),
            account_keys,
            recent_blockhash: *message.recent_blockhash(),
            instructions: message.instructions().to_vec(),
            address_table_lookups: message.message_address_table_lookups().to_vec(),
        };

        let status_meta = solana_transaction.transaction_status_meta;


       

        let transaction = Transaction {
            slot_identifier: SlotIdentifier { slot },
            signatures: solana_transaction.transaction.signatures().to_vec(),
            message: v0_message,
            is_vote: solana_transaction.is_vote,
            transaction_meta: TransactionMeta {
                error: match &status_meta.status {
                    Ok(_) => None,
                    Err(e) => Some(e.clone()),
                },
                fee: status_meta.fee,
                pre_balances: status_meta.pre_balances.clone(),
                post_balances: status_meta.post_balances.clone(),
                post_token_balances: Some(
                    status_meta
                        .post_token_balances
                        .as_ref()
                        .unwrap_or(&Vec::new())
                        .iter()
                        .map(|b| TransactionTokenBalanceSerializable {
                            token_amount: b
                                .ui_token_amount
                                .amount
                                .parse::<u64>()
                                .unwrap_or_default(),
                            account_index: b.account_index,
                            mint: b.mint.clone(),
                            owner: b.owner.clone(),
                            program_id: b.program_id.clone(),
                        })
                        .collect::<Vec<TransactionTokenBalanceSerializable>>(),
                ),

                pre_token_balances: Some(
                    status_meta
                        .pre_token_balances
                        .as_ref()
                        .unwrap_or(&Vec::new())
                        .iter()
                        .map(|b| TransactionTokenBalanceSerializable {
                            token_amount: b
                                .ui_token_amount
                                .amount
                                .parse::<u64>()
                                .unwrap_or_default(),
                            account_index: b.account_index,
                            mint: b.mint.clone(),
                            owner: b.owner.clone(),
                            program_id: b.program_id.clone(),
                        })
                        .collect::<Vec<TransactionTokenBalanceSerializable>>(),
                ),
                inner_instructions: status_meta.inner_instructions.clone(),
                log_messages: status_meta.log_messages.clone(),
                rewards: status_meta.rewards.clone(),
                loaded_addresses: status_meta.loaded_addresses.clone(),
                return_data: status_meta.return_data.clone(),
                compute_units_consumed: status_meta.compute_units_consumed,
            },
            index: solana_transaction.index as u64,
        };

         // **Check** if the transaction has an error, and skip if so:
         if transaction.transaction_meta.error.is_some() {
            log::info!("Skipping transaction with error: {:?}", transaction.transaction_meta.error);
            return Ok(()); 
            // or do whatever "stop" logic is appropriate
        }

        let transaction_message = ChannelMessage::Transaction(Box::new(transaction));

        if let Some(block_channel) = &self.block_builder_channel {
            let _ = block_channel.send(transaction_message.clone());
        }

        if let Some(mq_tx) = &self.mq_sender {
            // try_send if you want non-blocking, or send if you can block
            if let Err(send_err) = mq_tx.send(transaction_message.clone()) {
                // robust error handling:
                log::error!("Failed to send transaction to MQ server: {send_err}");
            }
        }

        quic_server
            .send_message(transaction_message)
            .map_err(|e| GeyserPluginError::Custom(Box::new(e)))?;
        Ok(())
    }

    fn notify_entry(&self, _entry: ReplicaEntryInfoVersions) -> PluginResult<()> {
        // Not required
        Ok(())
    }

    fn notify_block_metadata(&self, blockinfo: ReplicaBlockInfoVersions) -> PluginResult<()> {
        let Some(quic_server) = &self.quic_server else {
            return Ok(());
        };

        let ReplicaBlockInfoVersions::V0_0_3(blockinfo) = blockinfo else {
            return Err(GeyserPluginError::AccountsUpdateError {
                msg: "Unsupported account info version".to_string(),
            });
        };

        let block_meta = BlockMeta {
            parent_slot: blockinfo.parent_slot,
            slot: blockinfo.slot,
            parent_blockhash: blockinfo.parent_blockhash.to_string(),
            blockhash: blockinfo.blockhash.to_string(),
            rewards: blockinfo.rewards.to_vec(),
            block_height: blockinfo.block_height,
            executed_transaction_count: blockinfo.executed_transaction_count,
            entries_count: blockinfo.entry_count,
            block_time: blockinfo.block_time.unwrap_or_default() as u64,
        };

        let block_meta_message = ChannelMessage::BlockMeta(block_meta);

        if let Some(block_channel) = &self.block_builder_channel {
            let _ = block_channel.send(block_meta_message.clone());
        }

        if let Some(rpc_server_message_channel) = &self.rpc_server_message_channel {
            let _ = rpc_server_message_channel.send(block_meta_message.clone());
        }

        quic_server
            .send_message(block_meta_message)
            .map_err(|e| GeyserPluginError::Custom(Box::new(e)))?;
        Ok(())
    }

    fn account_data_notifications_enabled(&self) -> bool {
        true
    }

    fn transaction_notifications_enabled(&self) -> bool {
        true
    }

    fn entry_notifications_enabled(&self) -> bool {
        false
    }
}

#[no_mangle]
#[allow(improper_ctypes_definitions)]
/// # Safety
///
/// This function returns the Plugin pointer as trait GeyserPlugin.
pub unsafe extern "C" fn _create_plugin() -> *mut dyn GeyserPlugin {
    let plugin = QuicGeyserPlugin::default();
    let plugin: Box<dyn GeyserPlugin> = Box::new(plugin);
    Box::into_raw(plugin)
}
