// /*
//  * Copyright 2022, The Cozo Project Authors.
//  *
//  * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
//  * If a copy of the MPL was not distributed with this file,
//  * You can obtain one at https://mozilla.org/MPL/2.0/.
//  */

// use std::collections::BTreeMap;
// #[allow(unused_imports)]
// use miette::{bail, miette, Diagnostic, IntoDiagnostic, Result, WrapErr};
// use smartstring::{LazyCompact, SmartString};

// use crate::data::expr::Expr;
// use crate::data::symb::Symbol;
// use crate::data::value::DataValue;
// use crate::fixed_rule::{CannotDetermineArity, FixedRule, FixedRulePayload};
// use crate::parse::SourceSpan;

// #[derive(Debug)]
// pub(crate) struct JsonReader;

// impl FixedRule for JsonReader {

//     fn arity(
//         &self,
//         opts: &BTreeMap<SmartString<LazyCompact>, Expr>,
//         _rule_head: &[Symbol],
//         span: SourceSpan,
//     ) -> Result<usize> {
//         let with_row_num = match opts.get("prepend_index") {
//             None => 0,
//             Some(Expr::Const {
//                 val: DataValue::Bool(true),
//                 ..
//             }) => 1,
//             Some(Expr::Const {
//                 val: DataValue::Bool(false),
//                 ..
//             }) => 0,
//             _ => bail!(CannotDetermineArity(
//                 "JsonReader".to_string(),
//                 "invalid option 'prepend_index' given, expect a boolean".to_string(),
//                 span
//             )),
//         };
//         let fields = opts.get("fields").ok_or_else(|| {
//             CannotDetermineArity(
//                 "JsonReader".to_string(),
//                 "option 'fields' not provided".to_string(),
//                 span,
//             )
//         })?;
//         Ok(match fields.clone().eval_to_const()? {
//             DataValue::List(l) => l.len() + with_row_num,
//             _ => bail!(CannotDetermineArity(
//                 "JsonReader".to_string(),
//                 "invalid option 'fields' given, expect a list".to_string(),
//                 span
//             )),
//         })
//     }
    
//     fn init_options(
//         &self,
//         _options: &mut BTreeMap<SmartString<LazyCompact>, Expr>,
//         _span: SourceSpan,
//     ) -> Result<()> {
//         Ok(())
//     }
// }

