// /*
//  *  Copyright 2022, The Cozo Project Authors.
//  *
//  *  This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
//  *  If a copy of the MPL was not distributed with this file,
//  *  You can obtain one at https://mozilla.org/MPL/2.0/.
//  *
//  */

// use std::collections::BTreeMap;
// use std::time::Duration;

// use itertools::Itertools;
// use log::debug;
// use serde_json::json;
// use smartstring::{LazyCompact, SmartString};

// use crate::data::expr::Expr;
// use crate::data::symb::Symbol;
// use crate::data::value::DataValue;
// use crate::fixed_rule::FixedRulePayload;
// // use crate::fts::{TokenizerCache, TokenizerConfig};
// use crate::parse::SourceSpan;
// use crate::runtime::callback::CallbackOp;
// use crate::runtime::db::Poison;
// // use crate::{DbInstance, FixedRule, RegularTempStore};
// use crate::runtime::db::ScriptMutability;

// #[test]
// fn imperative_script() {
//     // let db = DbInstance::default();
//     // let res = db
//     //     .run_default(
//     //         r#"
//     //     {:create _test {a}}
//     //
//     //     %loop
//     //         %if { len[count(x)] := *_test[x]; ?[x] := len[z], x = z >= 10 }
//     //             %then %return _test
//     //         %end
//     //         { ?[a] := a = rand_uuid_v1(); :put _test {a} }
//     //         %debug _test
//     //     %end
//     // "#,
//     //         Default::default(),
//     //     )
//     //     .unwrap();
//     // assert_eq!(res.rows.len(), 10);
//     //
//     // let res = db
//     //     .run_default(
//     //         r#"
//     //     {?[a] <- [[1], [2], [3]]
//     //      :replace _test {a}}
//     //
//     //     %loop
//     //         { ?[a] := *_test[a]; :limit 1; :rm _test {a} }
//     //         %debug _test
//     //
//     //         %if_not _test
//     //         %then %break
//     //         %end
//     //     %end
//     //
//     //     %return _test
//     // "#,
//     //         Default::default(),
//     //     )
//     //     .unwrap();
//     // assert_eq!(res.rows.len(), 0);
//     //
//     // let res = db.run_default(
//     //     r#"
//     //     {:create _test {a}}
//     //
//     //     %loop
//     //         { ?[a] := a = rand_uuid_v1(); :put _test {a} }
//     //
//     //         %if { len[count(x)] := *_test[x]; ?[x] := len[z], x = z < 10 }
//     //             %continue
//     //         %end
//     //
//     //         %return _test
//     //         %debug _test
//     //     %end
//     // "#,
//     //     Default::default(),
//     // );
//     // if let Err(err) = &res {
//     //     eprintln!("{err:?}");
//     // }
//     // assert_eq!(res.unwrap().rows.len(), 10);
//     //
//     // let res = db
//     //     .run_default(
//     //         r#"
//     //     {?[a] <- [[1], [2], [3]]
//     //      :replace _test {a}}
//     //     {?[a] <- []
//     //      :replace _test2 {a}}
//     //     %swap _test _test2
//     //     %return _test
//     // "#,
//     //         Default::default(),
//     //     )
//     //     .unwrap();
//     // assert_eq!(res.rows.len(), 0);
// }


