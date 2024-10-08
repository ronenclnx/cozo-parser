/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

//! This crate provides the core functionalities of [CozoDB](https://cozodb.org).
//! It may be used to embed CozoDB in your application.
//!
//! This doc describes the Rust API. To learn how to use CozoDB to query (CozoScript), see:
//!
//! * [The CozoDB documentation](https://docs.cozodb.org)
//!
//! Rust API usage:
//! ```
//! use cozo::*;
//!
//! let db = DbInstance::new("mem", "", Default::default()).unwrap();
//! let script = "?[a] := a in [1, 2, 3]";
//! let result = db.run_script(script, Default::default(), ScriptMutability::Immutable).unwrap();
//! println!("{:?}", result);
//! ```
//! We created an in-memory database above. There are other persistent options:
//! see [DbInstance::new]. It is perfectly fine to run multiple storage engines in the same process.
//!
// #![doc = document_features::document_features!()]
#![warn(rust_2018_idioms, future_incompatible)]
#![warn(missing_docs)]
#![allow(clippy::type_complexity)]
#![allow(clippy::too_many_arguments)]

use std::collections::BTreeMap;
use std::sync::Arc;
#[allow(unused_imports)]
use std::time::Instant;

use fixed_rule::FixedRule;
use lazy_static::lazy_static;
pub use miette::Error;
use miette::Report;
#[allow(unused_imports)]
use miette::{
    bail, miette, GraphicalReportHandler, GraphicalTheme, IntoDiagnostic, JSONReportHandler,
    Result, ThemeCharacters, ThemeStyles,
};
use parse::SourceSpan;
use crate::compile::{Compiler};
use serde_json::json;

use crate::compile::symb::Symbol;

mod data;
mod fixed_rule;
mod parse;
mod query;
mod runtime;
mod storage;
mod utils;
mod translate;
mod compile;
mod diagnostics;



lazy_static! {
    static ref TEXT_ERR_HANDLER: GraphicalReportHandler = miette::GraphicalReportHandler::new()
        .with_theme(GraphicalTheme {
            characters: ThemeCharacters::unicode(),
            styles: ThemeStyles::ansi()
        });
    static ref JSON_ERR_HANDLER: JSONReportHandler = miette::JSONReportHandler::new();
}

// above starts from old lib.rs


use crate::parse::parse_script;
/// no documentation
pub fn main() {
    println!("hello cozo parser experiment");

    // let script = r##"
    //     fibo[n, x] := n=0, x=1
    //     fibo[n, x] := n=1, x=1
    //     fibo[n, x] := fibo[n1, a], fibo[n2, b], n=n1+1, n=n2+2, x=a+b, n<10

    //     nodes[n] := is_node(n.id)

    //     ?[n, x] := fibo[n, x]
    //     "##;

    let script = r##"
        mutations[m] := *mutations[m]
        has_added[m, n] := *has_added[m, n]
        has_target[m, n] := *has_target[m, n]
        is_parent[p,c] := mutations[m], has_added[m, c], has_target[m, p]
        ?[x, y] := is_parent[x, y]
        "##;

    // let script = r##"
    //     mutations[m] := *mutations[m]
    //     ?[x] := mutations[x]
    //     "##;

    // let params: BTreeMap<String, DataValue> = BTreeMap::new();
    let fixed_rules:BTreeMap<String, Arc<Box<dyn FixedRule>>> = BTreeMap::new();
    // let res = parse_script(script, &params, &fixed_rules, cur_vld).unwrap().get_single_program().unwrap();
    
    // println!("res = {res:?}");


    let mut compiler = Compiler::new();
    compiler.compile_script(":create has_added{ m: Uuid, n: Uuid => }").unwrap();
    compiler.compile_script(":create has_target{ m: Uuid, n: Uuid => }").unwrap();
    compiler.compile_script(":create mutations{ m: Uuid => }").unwrap();

    let res = compiler.compile_script(script);
    println!("\n\nxxx151 res = {res:?}");

    let temp = res.unwrap();
    println!("\n\nxxx160\n keys = {:?}", temp[0].keys());


    let s = Symbol{name: "?".into(), span: SourceSpan(0,0) };
    let s = compile::program::MagicSymbol::Muggle { inner: s };
    let t = match &temp[0][&s] {
        compile::CompiledRuleSet::Rules(rs) => &rs[0],
        compile::CompiledRuleSet::Fixed(_) => todo!(),
    } ;
    {
        // data::program::InputInlineRulesOrFixed::Rules { rules } => &rules[0].body[0],
        // data::program::InputInlineRulesOrFixed::Fixed { fixed } => todo!(),
    };
    println!("\n\nxxx161\n t = {t:?}");

    let s = Symbol{name: "mutations".into(), span: SourceSpan(0,0) };
    let s = compile::program::MagicSymbol::Magic { inner: s, adornment: vec![false].into() };
    let t = match &temp[0][&s] {
        compile::CompiledRuleSet::Rules(rs) => &rs[0],
        compile::CompiledRuleSet::Fixed(_) => todo!(),
    } ;
    {
        // data::program::InputInlineRulesOrFixed::Rules { rules } => &rules[0].body[0],
        // data::program::InputInlineRulesOrFixed::Fixed { fixed } => todo!(),
    };
    println!("\n\nxxx161\n t = {t:?}");


    let explain =  diagnostics::explain_compiled(&temp).unwrap();
    println!("\n\nxxx177\n {explain:?}");

    let translated = translate::translate_program(&temp[0]);
    println!("\n\nxxx181\n {translated:?}");


}