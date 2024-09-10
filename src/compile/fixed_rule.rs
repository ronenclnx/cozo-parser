/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

 use std::collections::BTreeMap;
 use std::fmt::Debug;
 use std::sync::Arc;
 
//  use crossbeam::channel::{bounded, Receiver, Sender};
 #[allow(unused_imports)]
 use either::{Left, Right};
 #[cfg(feature = "graph-algo")]
//  use graph::prelude::{CsrLayout, DirectedCsrGraph, GraphBuilder};
 use itertools::Itertools;
 use lazy_static::lazy_static;
 use miette::IntoDiagnostic;
 #[allow(unused_imports)]
 use miette::{bail, ensure, Diagnostic, Report, Result};
//  use smartstring::{LazyCompact, SmartString};
 use thiserror::Error;
 
 use crate::compile::expr::Expr;
 use super::program::{
     FixedRuleOptionNotFoundError, MagicFixedRuleApply, MagicFixedRuleRuleArg, MagicSymbol, WrongFixedRuleOptionError
 };
use super::Compiler;
 use crate::compile::symb::Symbol;
 use crate::data::tuple::TupleIter;
 use crate::data::value::DataValue;
//  use crate::fixed_rule::utilities::*;
 use crate::parse::SourceSpan;
 use crate::runtime::temp_store::{EpochStore, RegularTempStore};
 use crate::runtime::transact::SessionTx;
 use crate::runtime::db::NamedRows;
 use miette::{NamedSource};

 
 
 /// Represents an input relation during the execution of a fixed rule
 #[derive(Copy, Clone)]
 pub struct FixedRuleInputRelation<'a, 'b> {
     arg_manifest: &'a MagicFixedRuleRuleArg,
     stores: &'a BTreeMap<MagicSymbol, EpochStore>,
     tx: &'a SessionTx<'b>,
     compiler: &'a Compiler,
 }
 
 impl<'a, 'b> FixedRuleInputRelation<'a, 'b> {
     /// The arity of the input relation
     pub fn arity(&self) -> Result<usize> {
         self.arg_manifest.arity(self.tx, self.stores)
     }
     /// Get the binding map of the input relation
     pub fn get_binding_map(&self, offset: usize) -> BTreeMap<Symbol, usize> {
         self.arg_manifest.get_binding_map(offset)
     }
     /// Get the source span of the input relation. Useful for generating informative error messages.
     pub fn span(&self) -> SourceSpan {
         self.arg_manifest.span()
     }
 }
 
 
 /// Trait for an implementation of an algorithm or a utility
 pub trait FixedRule: Send + Sync + Debug {
     /// Called to initialize the options given.
     /// Will always be called once, before anything else.
     /// You can mutate the options if you need to.
     /// The default implementation does nothing.
     fn init_options(
         &self,
         _options: &mut BTreeMap<String, Expr>,
         _span: SourceSpan,
     ) -> Result<()> {
         Ok(())
     }
     /// You must return the row width of the returned relation and it must be accurate.
     /// This function may be called multiple times.
     fn arity(
         &self,
         options: &BTreeMap<String, Expr>,
         rule_head: &[Symbol],
         span: SourceSpan,
     ) -> Result<usize>;
 }
 
 /// Simple wrapper for custom fixed rule. You have less control than implementing [FixedRule] directly,
 /// but implementation is simpler.
 pub struct SimpleFixedRule {
     return_arity: usize,
     rule: Box<
         dyn Fn(Vec<NamedRows>, BTreeMap<String, DataValue>) -> Result<NamedRows>
             + Send
             + Sync
             + 'static,
     >,
 }
 
 impl Debug for SimpleFixedRule {
     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
         f.debug_struct("SimpleFixedRule").field("return_arity", &self.return_arity).field("rule", &"TODO: IMPLEMENT THIS").finish()
     }
 }
 
 impl SimpleFixedRule {
     /// Construct a SimpleFixedRule.
     ///
     /// * `return_arity`: The return arity of this rule.
     /// * `rule`:  The rule implementation as a closure.
     //    The first argument is a vector of input relations, realized into NamedRows,
     //    and the second argument is a JSON object of passed in options.
     //    The returned NamedRows is the return relation of the application of this rule.
     //    Every row of the returned relation must have length equal to `return_arity`.
     pub fn new<R>(return_arity: usize, rule: R) -> Self
     where
         R: Fn(Vec<NamedRows>, BTreeMap<String, DataValue>) -> Result<NamedRows>
             + Send
             + Sync
             + 'static,
     {
         Self {
             return_arity,
             rule: Box::new(rule),
         }
     }
    // //  /// Construct a SimpleFixedRule that uses channels for communication.
    // //  pub fn rule_with_channel(
    // //      return_arity: usize,
    // //  ) -> (
    // //      Self,
    // //      Receiver<(
    // //          Vec<NamedRows>,
    // //          BTreeMap<String, DataValue>,
    // //          Sender<Result<NamedRows>>,
    // //      )>,
    // //  ) {
    // //      let (db2app_sender, db2app_receiver) = bounded(0);
    // //      (
    // //          Self {
    // //              return_arity,
    // //              rule: Box::new(move |inputs, options| -> Result<NamedRows> {
    // //                  let (app2db_sender, app2db_receiver) = bounded(0);
    // //                  db2app_sender
    // //                      .send((inputs, options, app2db_sender))
    // //                      .into_diagnostic()?;
    // //                  app2db_receiver.recv().into_diagnostic()?
    // //              }),
    // //          },
    // //          db2app_receiver,
    // //      )
    // //  }
 }
 
 impl FixedRule for SimpleFixedRule {
     fn arity(
         &self,
         _options: &BTreeMap<String, Expr>,
         _rule_head: &[Symbol],
         _span: SourceSpan,
     ) -> Result<usize> {
         Ok(self.return_arity)
     }
 
     
     fn init_options(
         &self,
         _options: &mut BTreeMap<String, Expr>,
         _span: SourceSpan,
     ) -> Result<()> {
         Ok(())
     }
 }
 
 #[derive(Debug, Error, Diagnostic)]
 #[error("Cannot determine arity for algo {0} since {1}")]
 #[diagnostic(code(parser::no_algo_arity))]
 pub(crate) struct CannotDetermineArity(
     pub(crate) String,
     pub(crate) String,
     #[label] pub(crate) SourceSpan,
 );
 
 #[derive(Clone, Debug)]
 pub(crate) struct FixedRuleHandle {
     pub(crate) name: Symbol,
 }
 
 
 impl FixedRuleHandle {
     pub(crate) fn new(name: &str, span: SourceSpan) -> Self {
         FixedRuleHandle {
             name: Symbol::new(name, span),
         }
     }
 }
 
 #[derive(Error, Diagnostic, Debug)]
 #[error("The relation cannot be interpreted as an edge")]
 #[diagnostic(code(algo::not_an_edge))]
 #[diagnostic(help("Edge relation requires tuples of length at least two"))]
 struct NotAnEdgeError(#[label] SourceSpan);
 
 // // #[derive(Error, Diagnostic, Debug)]
 // // #[error(
 // //     "The value {0:?} at the third position in the relation cannot be interpreted as edge weights"
 // // )]
 // // #[diagnostic(code(algo::invalid_edge_weight))]
 // // #[diagnostic(help(
 // //     "Edge weights must be finite numbers. Some algorithm also requires positivity."
 // // ))]
 // // struct BadEdgeWeightError(DataValue, #[label] SourceSpan);
 
 
  
 #[derive(Error, Diagnostic, Debug)]
 #[error("Invalid reverse scanning of triples")]
 #[diagnostic(code(algo::invalid_reverse_triple_scan))]
 #[diagnostic(help(
     "Inverse scanning of triples requires the type to be 'ref', or the value be indexed"
 ))]
 struct InvalidInverseTripleUse(String, #[label] SourceSpan);
 
 #[derive(Error, Diagnostic, Debug)]
 #[error("Required node with key {missing:?} not found")]
 #[diagnostic(code(algo::node_with_key_not_found))]
 #[diagnostic(help(
     "The relation is interpreted as a relation of nodes, but the required key is missing"
 ))]
 pub(crate) struct NodeNotFoundError {
     pub(crate) missing: DataValue,
     #[label]
     pub(crate) span: SourceSpan,
 }
 
 #[derive(Error, Diagnostic, Debug)]
 #[error("Unacceptable value {0:?} encountered")]
 #[diagnostic(code(algo::unacceptable_value))]
 pub(crate) struct BadExprValueError(
     pub(crate) DataValue,
     #[label] pub(crate) SourceSpan,
     #[help] pub(crate) String,
 );
 
 #[derive(Error, Diagnostic, Debug)]
 #[error("The requested fixed rule '{0}' is not found")]
 #[diagnostic(code(parser::fixed_rule_not_found))]
 pub(crate) struct FixedRuleNotFoundError(pub(crate) String, #[label] pub(crate) SourceSpan);
 
 
impl MagicFixedRuleRuleArg {
    pub(crate) fn arity(
        &self,
        tx: &SessionTx<'_>,
        stores: &BTreeMap<MagicSymbol, EpochStore>,
    ) -> Result<usize> {
        Ok(match self {
            MagicFixedRuleRuleArg::InMem { name, .. } => {
                let store = stores.get(name).ok_or_else(|| {
                    RuleNotFoundError(name.symbol().to_string(), name.symbol().span)
                })?;
                store.arity
            }
            MagicFixedRuleRuleArg::Stored { name, .. } => {
                let handle = tx.get_relation(name, false)?;
                handle.arity()
            }
        })
    }
}

// use crate::fixed_rule::RuleNotFoundError;
#[derive(Error, Diagnostic, Debug)]
#[error("The requested rule '{0}' cannot be found")]
#[diagnostic(code(algo::rule_not_found))]
pub struct RuleNotFoundError(String, #[label] SourceSpan);
