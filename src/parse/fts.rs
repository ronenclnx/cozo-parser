/*
 * Copyright 2023, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

// use crate::fts::ast::{FtsExpr, FtsLiteral, FtsNear};
use crate::parse::expr::parse_string;
use crate::parse::{CozoScriptParser, Pair, Rule};
use itertools::Itertools;
use lazy_static::lazy_static;
use miette::{IntoDiagnostic, Result};
use pest::pratt_parser::{Op, PrattParser};
use pest::Parser;
use smartstring::SmartString;


//     let mut inner = pair.into_inner();
//     let kernel = inner.next().unwrap();
//     let core_text = match kernel.as_rule() {
//         Rule::fts_phrase_group => SmartString::from(kernel.as_str().trim()),
//         Rule::quoted_string | Rule::s_quoted_string | Rule::raw_string => parse_string(kernel)?,
//         _ => unreachable!("unexpected rule: {:?}", kernel.as_rule()),
//     };
//     let mut is_quoted = false;
//     let mut booster = 1.0;
//     for pair in inner {
//         match pair.as_rule() {
//             Rule::fts_prefix_marker => is_quoted = true,
//             Rule::fts_booster => {
//                 let boosted = pair.into_inner().next().unwrap();
//                 match boosted.as_rule() {
//                     Rule::dot_float => {
//                         let f = boosted
//                             .as_str()
//                             .replace('_', "")
//                             .parse::<f64>()
//                             .into_diagnostic()?;
//                         booster = f;
//                     }
//                     Rule::int => {
//                         let i = boosted
//                             .as_str()
//                             .replace('_', "")
//                             .parse::<i64>()
//                             .into_diagnostic()?;
//                         booster = i as f64;
//                     }
//                     _ => unreachable!("unexpected rule: {:?}", boosted.as_rule()),
//                 }
//             }
//             _ => unreachable!("unexpected rule: {:?}", pair.as_rule()),
//         }
//     }
//     Ok(FtsLiteral {
//         value: core_text,
//         is_prefix: is_quoted,
//         booster: booster.into(),
//     })
// }

lazy_static! {
    static ref PRATT_PARSER: PrattParser<Rule> = {
        use pest::pratt_parser::Assoc::*;

        PrattParser::new()
            .op(Op::infix(Rule::fts_not, Left))
            .op(Op::infix(Rule::fts_and, Left))
            .op(Op::infix(Rule::fts_or, Left))
    };
}

