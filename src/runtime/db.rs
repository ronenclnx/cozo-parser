/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};
use std::default::Default;
use std::fmt::{Debug, Formatter};
use std::iter;
use std::path::Path;
#[allow(unused_imports)]
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
#[allow(unused_imports)]
use std::thread;
#[allow(unused_imports)]
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[allow(unused_imports)]
use crossbeam::channel::{bounded, unbounded, Receiver, Sender};
use crossbeam::sync::ShardedLock;
use either::{Left, Right};
use itertools::Itertools;
use miette::Report;
#[allow(unused_imports)]
use miette::{bail, ensure, miette, Diagnostic, IntoDiagnostic, Result, WrapErr};
use serde_json::json;
use smartstring::{LazyCompact, SmartString};
use thiserror::Error;

// use crate::data::functions::current_validity;
use crate::data::json::JsonValue;
// use crate::data::program::{InputProgram, QueryAssertion, RelationOp, ReturnMutation};
// use crate::data::relation::ColumnDef;
use crate::data::tuple::{Tuple, TupleT};
use crate::data::value::{DataValue};
// use crate::data::value::{ValidityTs, LARGEST_UTF_CHAR};
// use crate::fixed_rule::DEFAULT_FIXED_RULES;
// use crate::fts::TokenizerCache;
use crate::parse::sys::SysOp;
use crate::parse::{parse_expressions, parse_script, CozoScript, SourceSpan};
use crate::compile::{CompiledProgram, CompiledRule, CompiledRuleSet};
use crate::query::ra::{
    FilteredRA, InnerJoin, NegJoin, RelAlgebra, ReorderRA,
    StoredRA, StoredWithValidityRA, TempStoreRA, UnificationRA,
};
#[allow(unused_imports)]
use crate::runtime::callback::{
    CallbackCollector, CallbackDeclaration, CallbackOp, EventCallbackRegistry,
};
use crate::runtime::relation::{
    extend_tuple_from_v, AccessLevel, InsufficientAccessLevel, RelationHandle, RelationId,
};
use crate::runtime::transact::SessionTx;
use crate::storage::temp::TempStorage;
use crate::storage::Storage;
use crate::runtime::relation::decode_tuple_from_kv;
use crate::compile::symb::{Symbol};
use crate::fixed_rule::FixedRule;

pub(crate) struct RunningQueryHandle {
    pub(crate) started_at: f64,
    pub(crate) poison: Poison,
}

// // // pub(crate) struct RunningQueryCleanup {
// // //     pub(crate) id: u64,
// // //     pub(crate) running_queries: Arc<Mutex<BTreeMap<u64, RunningQueryHandle>>>,
// // // }

// // // impl Drop for RunningQueryCleanup {
// // //     fn drop(&mut self) {
// // //         let mut map = self.running_queries.lock().unwrap();
// // //         if let Some(handle) = map.remove(&self.id) {
// // //             handle.poison.0.store(true, Ordering::Relaxed);
// // //         }
// // //     }
// // // }

#[derive(serde_derive::Serialize, serde_derive::Deserialize)]
pub struct DbManifest {
    pub storage_version: u64,
}

// // /// Whether a script is mutable or immutable.
// // #[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
// // pub enum ScriptMutability {
// //     /// The script is mutable.
// //     Mutable,
// //     /// The script is immutable.
// //     Immutable,
// // }

/// The database object of Cozo.
#[derive(Clone)]
pub struct Db<S> {
    pub(crate) db: S,
    temp_db: TempStorage,
    relation_store_id: Arc<AtomicU64>,
    pub(crate) queries_count: Arc<AtomicU64>,
    pub(crate) running_queries: Arc<Mutex<BTreeMap<u64, RunningQueryHandle>>>,
    pub(crate) fixed_rules: Arc<ShardedLock<BTreeMap<String, Arc<Box<dyn FixedRule>>>>>,
    // // pub(crate) tokenizers: Arc<TokenizerCache>,
    #[cfg(not(target_arch = "wasm32"))]
    callback_count: Arc<AtomicU32>,
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) event_callbacks: Arc<ShardedLock<EventCallbackRegistry>>,
    relation_locks: Arc<ShardedLock<BTreeMap<SmartString<LazyCompact>, Arc<ShardedLock<()>>>>>,
}

impl<S> Debug for Db<S> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Db")
    }
}

#[derive(Debug, Diagnostic, Error)]
#[error("Initialization of database failed")]
#[diagnostic(code(db::init))]
pub(crate) struct BadDbInit(#[help] pub(crate) String);

// // #[derive(Debug, Error, Diagnostic)]
// // #[error("Cannot import data into relation {0} as it is an index")]
// // #[diagnostic(code(tx::import_into_index))]
// // pub(crate) struct ImportIntoIndex(pub(crate) String);

#[derive(serde_derive::Serialize, serde_derive::Deserialize, Debug, Clone, Default)]
/// Rows in a relation, together with headers for the fields.
pub struct NamedRows {
    /// The headers
    pub headers: Vec<String>,
    /// The rows
    pub rows: Vec<Tuple>,
    /// Contains the next named rows, if exists
    pub next: Option<Box<NamedRows>>,
}

impl IntoIterator for NamedRows {
    type Item = Tuple;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.rows.into_iter()
    }
}

impl NamedRows {
    /// create a named rows with the given headers and rows
    pub fn new(headers: Vec<String>, rows: Vec<Tuple>) -> Self {
        Self {
            headers,
            rows,
            next: None,
        }
    }

    // // /// If there are more named rows after the current one
    // // pub fn has_more(&self) -> bool {
    // //     self.next.is_some()
    // // }

    // // /// convert a chain of named rows to individual named rows
    // // pub fn flatten(self) -> Vec<Self> {
    // //     let mut collected = vec![];
    // //     let mut current = self;
    // //     loop {
    // //         let nxt = current.next.take();
    // //         collected.push(current);
    // //         if let Some(n) = nxt {
    // //             current = *n;
    // //         } else {
    // //             break;
    // //         }
    // //     }
    // //     collected
    // // }

    // // /// Convert to a JSON object
    // // pub fn into_json(self) -> JsonValue {
    // //     let nxt = match self.next {
    // //         None => json!(null),
    // //         Some(more) => more.into_json(),
    // //     };
    // //     let rows = self
    // //         .rows
    // //         .into_iter()
    // //         .map(|row| row.into_iter().map(JsonValue::from).collect::<JsonValue>())
    // //         .collect::<JsonValue>();
    // //     json!({
    // //         "headers": self.headers,
    // //         "rows": rows,
    // //         "next": nxt,
    // //     })
    // // }
    // // /// Make named rows from JSON
    // // pub fn from_json(value: &JsonValue) -> Result<Self> {
    // //     let headers = value
    // //         .get("headers")
    // //         .ok_or_else(|| miette!("NamedRows requires 'headers' field"))?;
    // //     let headers = headers
    // //         .as_array()
    // //         .ok_or_else(|| miette!("'headers' field must be an array"))?;
    // //     let headers = headers
    // //         .iter()
    // //         .map(|h| -> Result<String> {
    // //             let h = h
    // //                 .as_str()
    // //                 .ok_or_else(|| miette!("'headers' field must be an array of strings"))?;
    // //             Ok(h.to_string())
    // //         })
    // //         .try_collect()?;
    // //     let rows = value
    // //         .get("rows")
    // //         .ok_or_else(|| miette!("NamedRows requires 'rows' field"))?;
    // //     let rows = rows
    // //         .as_array()
    // //         .ok_or_else(|| miette!("'rows' field must be an array"))?;
    // //     let rows = rows
    // //         .iter()
    // //         .map(|row| -> Result<Vec<DataValue>> {
    // //             let row = row
    // //                 .as_array()
    // //                 .ok_or_else(|| miette!("'rows' field must be an array of arrays"))?;
    // //             Ok(row.iter().map(DataValue::from).collect_vec())
    // //         })
    // //         .try_collect()?;
    // //     Ok(Self {
    // //         headers,
    // //         rows,
    // //         next: None,
    // //     })
    // // }

    // // /// Create a query and parameters to apply an operation (insert, put, delete, rm) to a stored
    // // /// relation with the named rows.
    // // pub fn into_payload(self, relation: &str, op: &str) -> Payload {
    // //     let cols_str = self.headers.join(", ");
    // //     let query = format!("?[{cols_str}] <- $data :{op} {relation} {{ {cols_str} }}");
    // //     let data = DataValue::List(self.rows.into_iter().map(|r| DataValue::List(r)).collect());
    // //     (query, [("data".to_string(), data)].into())
    // // }
}

const STATUS_STR: &str = "status";
const OK_STR: &str = "OK";

/// The query and parameters.
pub type Payload = (String, BTreeMap<String, DataValue>);


impl<'s, S: Storage<'s>> Db<S> {

    //     let lower = vec![DataValue::from("")].encode_as_key(RelationId::SYSTEM);
    //     let upper =
    //         vec![DataValue::from(String::from(LARGEST_UTF_CHAR))].encode_as_key(RelationId::SYSTEM);
    //     let mut rows: Vec<Vec<JsonValue>> = vec![];
    //     for kv_res in tx.store_tx.range_scan(&lower, &upper) {
    //         let (k_slice, v_slice) = kv_res?;
    //         if upper <= k_slice {
    //             break;
    //         }
    //         let meta = RelationHandle::decode(&v_slice)?;
    //         let n_keys = meta.metadata.keys.len();
    //         let n_dependents = meta.metadata.non_keys.len();
    //         let arity = n_keys + n_dependents;
    //         let name = meta.name;
    //         let access_level = if name.contains(':') {
    //             "index".to_string()
    //         } else {
    //             meta.access_level.to_string()
    //         };
    //         rows.push(vec![
    //             json!(name),
    //             json!(arity),
    //             json!(access_level),
    //             json!(n_keys),
    //             json!(n_dependents),
    //             json!(meta.put_triggers.len()),
    //             json!(meta.rm_triggers.len()),
    //             json!(meta.replace_triggers.len()),
    //             json!(meta.description),
    //         ]);
    //     }
    //     let rows = rows
    //         .into_iter()
    //         .map(|row| row.into_iter().map(DataValue::from).collect_vec())
    //         .collect_vec();
    //     Ok(NamedRows::new(
    //         vec![
    //             "name".to_string(),
    //             "arity".to_string(),
    //             "access_level".to_string(),
    //             "n_keys".to_string(),
    //             "n_non_keys".to_string(),
    //             "n_put_triggers".to_string(),
    //             "n_rm_triggers".to_string(),
    //             "n_replace_triggers".to_string(),
    //             "description".to_string(),
    //         ],
    //         rows,
    //     ))
    // }
}


/// Used for user-initiated termination of running queries
#[derive(Clone, Default)]
pub struct Poison(pub(crate) Arc<AtomicBool>);

impl Poison {
    /// Will return `Err` if user has initiated termination.
    #[inline(always)]
    pub fn check(&self) -> Result<()> {
        #[derive(Debug, Error, Diagnostic)]
        #[error("Running query is killed before completion")]
        #[diagnostic(code(eval::killed))]
        #[diagnostic(help("A query may be killed by timeout, or explicit command"))]
        struct ProcessKilled;

        if self.0.load(Ordering::Relaxed) {
            bail!(ProcessKilled)
        }
        Ok(())
    }
    #[cfg(target_arch = "wasm32")]
    pub(crate) fn set_timeout(&self, _secs: f64) -> Result<()> {
        bail!("Cannot set timeout when threading is disallowed");
    }
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) fn set_timeout(&self, secs: f64) -> Result<()> {
        let pill = self.clone();
        thread::spawn(move || {
            thread::sleep(Duration::from_micros((secs * 1000000.) as u64));
            pill.0.store(true, Ordering::Relaxed);
        });
        Ok(())
    }
}