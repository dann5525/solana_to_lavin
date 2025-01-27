use serde::{Deserialize, Serialize};
use solana_sdk::{
    message::v0::{LoadedAddresses, Message},
    signature::Signature,
    transaction::TransactionError,
    transaction_context::TransactionReturnData,

};
use solana_transaction_status::Rewards;

use super::slot_identifier::SlotIdentifier;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct InnerInstructionsSerializable {
    pub index: u8,
    pub instructions: Vec<InnerInstructionSerializable>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct InnerInstructionSerializable {
    pub stack_height: Option<u32>,
    pub instruction: CompiledInstructionSerializable,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct CompiledInstructionSerializable {
    pub program_id_index: u8,
    pub accounts: Vec<u8>,
    pub data: Vec<u8>,
}

// 1) Convert a single InnerInstruction
impl From<&solana_transaction_status::InnerInstruction> for InnerInstructionSerializable {
    fn from(ix: &solana_transaction_status::InnerInstruction) -> Self {
        Self {
            stack_height: ix.stack_height,
            instruction: CompiledInstructionSerializable {
                program_id_index: ix.instruction.program_id_index,
                accounts: ix.instruction.accounts.clone(),
                data: ix.instruction.data.clone(),
            },
        }
    }
}

// 2) Convert an InnerInstructions (contains index + Vec<InnerInstruction>)
impl From<&solana_transaction_status::InnerInstructions> for InnerInstructionsSerializable {
    fn from(i: &solana_transaction_status::InnerInstructions) -> Self {
        Self {
            index: i.index as u8, // or cast as needed
            instructions: i
                .instructions
                .iter()
                .map(|ix| ix.into())
                .collect::<Vec<InnerInstructionSerializable>>(),
        }
    }
}



#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TransactionTokenBalanceSerializable {
    pub account_index: u8,
    pub mint: String,
    pub token_amount: u64,
    pub owner: String,
    pub program_id: String,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[repr(C)]
#[serde(rename_all = "camelCase")]
pub struct TransactionMeta {
    pub error: Option<TransactionError>,
    pub fee: u64,
    pub pre_balances: Vec<u64>,
    pub post_balances: Vec<u64>,
    pub pre_token_balances: Option<Vec<TransactionTokenBalanceSerializable>>,
    pub post_token_balances: Option<Vec<TransactionTokenBalanceSerializable>>,
    pub inner_instructions: Option<Vec<InnerInstructionsSerializable>>,
    pub log_messages: Option<Vec<String>>,
    pub rewards: Option<Rewards>,
    pub loaded_addresses: LoadedAddresses,
    pub return_data: Option<TransactionReturnData>,
    pub compute_units_consumed: Option<u64>,
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
#[repr(C)]
#[serde(rename_all = "camelCase")]
pub struct Transaction {
    pub slot_identifier: SlotIdentifier,
    pub signatures: Vec<Signature>,
    pub message: Message,
    pub is_vote: bool,
    pub transaction_meta: TransactionMeta,
    pub index: u64,
}

