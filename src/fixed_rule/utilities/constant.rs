/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::BTreeMap;

use miette::{bail, ensure, Diagnostic, Result};
// use smartstring::{LazyCompact, SmartString};
use thiserror::Error;

use crate::compile::expr::Expr;
use crate::compile::program::WrongFixedRuleOptionError;
use crate::compile::symb::Symbol;
use crate::data::value::DataValue;
use crate::fixed_rule::{FixedRule};
use crate::parse::SourceSpan;
use crate::runtime::temp_store::RegularTempStore;

#[derive(Debug)]
pub(crate) struct Constant;

impl FixedRule for Constant {

    fn arity(
        &self,
        options: &BTreeMap<String, Expr>,
        rule_head: &[Symbol],
        span: SourceSpan,
    ) -> Result<usize> {
        let data = options
            .get("data")
            .unwrap()
            .get_const()
            .unwrap()
            .get_slice()
            .unwrap();
        Ok(if data.is_empty() {
            match rule_head.len() {
                0 => {
                    #[derive(Error, Debug, Diagnostic)]
                    #[error("Constant rule does not have data")]
                    #[diagnostic(code(parser::empty_const_rule))]
                    #[diagnostic(help(
                        "If you insist on using this empty rule, explicitly give its head"
                    ))]
                    struct EmptyConstRuleError(#[label] SourceSpan);
                    bail!(EmptyConstRuleError(span))
                }
                i => i,
            }
        } else {
            data.first().unwrap().get_slice().unwrap().len()
        })
    }

    fn init_options(
        &self,
        options: &mut BTreeMap<String, Expr>,
        span: SourceSpan,
    ) -> Result<()> {
        let data = options
            .get("data")
            .ok_or_else(|| WrongFixedRuleOptionError {
                name: "data".to_string(),
                span: Default::default(),
                rule_name: "Constant".to_string(),
                help: "a list of lists is required".to_string(),
            })?;
        let data = match data.clone().eval_to_const()? {
            DataValue::List(l) => l,
            _ => bail!(WrongFixedRuleOptionError {
                name: "data".to_string(),
                span: Default::default(),
                rule_name: "Constant".to_string(),
                help: "a list of lists is required".to_string(),
            }),
        };

        let mut tuples = vec![];
        let mut last_len = None;
        for row in data {
            match row {
                DataValue::List(tuple) => {
                    if let Some(l) = &last_len {
                        #[derive(Error, Debug, Diagnostic)]
                        #[error("Constant head must have the same arity as the data given")]
                        #[diagnostic(code(parser::const_data_arity_mismatch))]
                        #[diagnostic(help("First row length: {0}; the mismatch: {1:?}"))]
                        struct ConstRuleRowArityMismatch(
                            usize,
                            Vec<DataValue>,
                            #[label] SourceSpan,
                        );

                        ensure!(
                            *l == tuple.len(),
                            ConstRuleRowArityMismatch(*l, tuple, span)
                        );
                    };
                    last_len = Some(tuple.len());
                    tuples.push(DataValue::List(tuple));
                }
                row => {
                    #[derive(Error, Debug, Diagnostic)]
                    #[error("Bad row for constant rule: {0:?}")]
                    #[diagnostic(code(parser::bad_row_for_const))]
                    #[diagnostic(help(
                        "The body of a constant rule should evaluate to a list of lists"
                    ))]
                    struct ConstRuleRowNotList(DataValue);

                    bail!(ConstRuleRowNotList(row))
                }
            }
        }

        options.insert(
            String::from("data"),
            Expr::Const {
                val: DataValue::List(tuples),
                span: Default::default(),
            },
        );

        Ok(())
    }
}
