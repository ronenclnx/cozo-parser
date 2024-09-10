/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::Arc;

use miette::{bail, Result};
use crate::compile::program::ReturnMutation;

use crate::data::tuple::TupleT;
use crate::data::value::DataValue;
// use crate::fts::TokenizerCache;
// use crate::runtime::callback::{CallbackOp};
// use crate::runtime::db::NamedRows;
// use crate::runtime::relation::RelationId;
use crate::storage::temp::TempTx;
use crate::storage::StoreTx;

pub struct SessionTx<'a> {
    pub(crate) store_tx: Box<dyn StoreTx<'a> + 'a>,
    pub(crate) temp_store_tx: TempTx,
    // pub(crate) relation_store_id: Arc<AtomicU64>,
    // pub(crate) temp_store_id: AtomicU32,
    // pub(crate) tokenizers: Arc<TokenizerCache>,
}

// // pub const CURRENT_STORAGE_VERSION: [u8; 1] = [0x00];

// // fn storage_version_key() -> Vec<u8> {
// //     let storage_version_tuple = vec![DataValue::Null, DataValue::from("STORAGE_VERSION")];
// //     storage_version_tuple.encode_as_key(RelationId::SYSTEM)
// // }

// // const STATUS_STR: &str = "status";
// // const OK_STR: &str = "OK";

// // impl<'a> SessionTx<'a> {
// // }
