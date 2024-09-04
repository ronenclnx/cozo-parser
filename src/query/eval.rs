/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

// use std::collections::btree_map::Entry;
// use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

// use itertools::Itertools;
// use log::{debug, trace};
// // use miette::Result;
// // #[cfg(not(target_arch = "wasm32"))]
// // use rayon::prelude::*;

// // use crate::data::aggr::Aggregation;
// // use crate::data::program::{MagicSymbol, NoEntryError};
// // use crate::data::symb::{Symbol, PROG_ENTRY};
// // use crate::data::tuple::Tuple;
// // use crate::data::value::DataValue;
// // use crate::fixed_rule::FixedRulePayload;
// // use crate::parse::SourceSpan;
// // use crate::query::compile::{
// //     AggrKind, CompiledProgram, CompiledRule, CompiledRuleSet, ContainedRuleMultiplicity,
// // };
// // use crate::runtime::db::Poison;
// // use crate::runtime::temp_store::{EpochStore, MeetAggrStore, RegularTempStore};
use crate::runtime::transact::SessionTx;

pub(crate) struct QueryLimiter {
    total: Option<usize>,
    skip: Option<usize>,
    counter: AtomicUsize,
}

impl QueryLimiter {
}

impl<'a> SessionTx<'a> {
}
