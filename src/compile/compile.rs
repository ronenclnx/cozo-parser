/*
 * Copyright 2022, The Cozo Project Authors.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public License, v. 2.0.
 * If a copy of the MPL was not distributed with this file,
 * You can obtain one at https://mozilla.org/MPL/2.0/.
 */

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::hash::Hash;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use itertools::Itertools;
use miette::{bail, ensure, Context, Diagnostic, Error, IntoDiagnostic, Result};
use thiserror::Error;

use crate::data::aggr::Aggregation;
use crate::compile::expr::Expr;
use crate::data::functions::current_validity;
use super::program::{
    FixedRuleArg, InputProgram, MagicAtom, MagicFixedRuleApply, MagicInlineRule, MagicRulesOrFixed, MagicSymbol, RelationOp, StratifiedMagicProgram
};
use crate::compile::symb::Symbol;
use crate::data::value::DataValue;
use crate::fixed_rule::{FixedRule, FixedRuleHandle};
use crate::parse::{parse_script, CozoScript, SourceSpan};
use crate::runtime::callback::CallbackCollector;
use miette::Report;

pub type CompiledProgram = BTreeMap<MagicSymbol, CompiledRuleSet>;
use crate::data::tuple::TupleT;
//  use crate::data::{NamedRows, ValidityTs};
use crate::data::value::ValidityTs;
use crate::runtime::db::NamedRows;
use serde_json::{json, Value};
use crate::data::json::JsonValue;
// use crate::query::ra::{InnerJoin, InlineFixedRA};
 use smartstring::{LazyCompact, SmartString};
 
 #[derive(Debug)]
 pub enum CompiledRuleSet {
     Rules(Vec<CompiledRule>),
     Fixed(MagicFixedRuleApply),
 }
 
 #[derive(Debug, Copy, Clone, Eq, PartialEq)]
 pub(crate) enum AggrKind {
     None,
     Normal,
     Meet,
 }
 
 impl CompiledRuleSet {
     pub(crate) fn arity(&self) -> usize {
         match self {
             CompiledRuleSet::Rules(rs) => rs[0].aggr.len(),
             CompiledRuleSet::Fixed(fixed) => fixed.arity,
         }
     }
     pub(crate) fn aggr_kind(&self) -> AggrKind {
         match self {
             CompiledRuleSet::Rules(rules) => {
                 let mut has_non_meet = false;
                 let mut has_aggr = false;
                 for maybe_aggr in rules[0].aggr.iter() {
                     match maybe_aggr {
                         None => {
                             // meet aggregations must all be at the last positions
                             if has_aggr {
                                 has_non_meet = true
                             }
                         }
                         Some((aggr, _)) => {
                             has_aggr = true;
                             has_non_meet = has_non_meet || !aggr.is_meet
                         }
                     }
                 }
                 match (has_aggr, has_non_meet) {
                     (false, _) => AggrKind::None,
                     (true, true) => AggrKind::Normal,
                     (true, false) => AggrKind::Meet,
                 }
             }
             CompiledRuleSet::Fixed(_) => AggrKind::None,
         }
     }
 }
 
 #[derive(Debug, Copy, Clone, Eq, PartialEq)]
 pub enum ContainedRuleMultiplicity {
     One,
     Many,
 }
 
 #[derive(Debug)]
 pub struct CompiledRule {
     pub(crate) aggr: Vec<Option<(Aggregation, Vec<DataValue>)>>,
     pub(crate) relation: RelAlgebra,
     pub(crate) contained_rules: BTreeMap<MagicSymbol, ContainedRuleMultiplicity>,
 }
 
 #[derive(Debug, Error, Diagnostic)]
 #[error("Requested rule {0} not found")]
 #[diagnostic(code(eval::rule_not_found))]
 struct RuleNotFound(String, #[label] SourceSpan);
 
 #[derive(Debug, Error, Diagnostic)]
 #[error("Arity mismatch for rule application {0}")]
 #[diagnostic(code(eval::rule_arity_mismatch))]
 #[diagnostic(help("Required arity: {1}, number of arguments given: {2}"))]
 struct ArityMismatch(String, usize, usize, #[label] SourceSpan);
 
 #[derive(Debug, Copy, Clone, Eq, PartialEq)]
 pub enum IndexPositionUse {
     Join,
     BindForLater,
     Ignored,
 }
 
 
 
 #[derive(Clone, Debug)]
 pub(crate) struct CompiledRelationHandle {
     id: u16,
     name: String,
     arity: u8,
     pub(crate) keys: Vec<ColumnDef>,
     pub(crate) non_keys: Vec<ColumnDef>,
 }
 
 impl CompiledRelationHandle {
 }
 
 pub struct Compiler {
     compiled_relations: BTreeMap<String, CompiledRelationHandle>,
     fixed_rules: Vec<u16>,// TODO: type
     relations: HashMap<String, u16>, //TODO: type
     rules: HashMap<String, u16>,
 }
 
 #[derive(Debug, Diagnostic, Error)]
 #[error("Cannot create relation {0} as one with the same name already exists")]
 #[diagnostic(code(eval::rel_name_conflict))]
 struct CompiledRelNameConflictError(String);
 
 impl Compiler {
    pub(crate) fn relation_exists(&self, name: &str) -> bool {
        self.relations.contains_key(name)
    }

    pub(crate) fn stratified_magic_compile(
        &self,
        prog: StratifiedMagicProgram,
    ) -> Result<Vec<CompiledProgram>> {
        let mut store_arities: BTreeMap<MagicSymbol, usize> = Default::default();

        for stratum in prog.0.iter() {
            for (name, ruleset) in &stratum.prog {
                store_arities.insert(name.clone(), ruleset.arity()?);
            }
        }

        let compiled: Vec<_> = prog
            .0
            .into_iter()
            .rev()
            .map(|cur_prog| -> Result<CompiledProgram> {
                cur_prog
                    .prog
                    .into_iter()
                    .map(|(k, body)| -> Result<(MagicSymbol, CompiledRuleSet)> {
                        match body {
                            MagicRulesOrFixed::Rules { rules: body } => {
                                // println!("xxx135 rules={body:?}");
                                let mut collected = Vec::with_capacity(body.len());
                                for rule in body.iter() {
                                    let header = &rule.head;
                                    let mut relation =
                                        self.compile_magic_rule_body(rule, &k, &store_arities, header)?;
                                    relation.fill_binding_indices_and_compile().with_context(|| {
                                        format!(
                                            "error encountered when filling binding indices for {relation:#?}"
                                        )
                                    })?;

                                    
                                    println!("xxx145,header={header:?} relation=\n{relation:?}");
                                    collected.push(CompiledRule {
                                        aggr: rule.aggr.clone(),
                                        relation,
                                        contained_rules: rule.contained_rules(),
                                    })
                                }
                                Ok((k, CompiledRuleSet::Rules(collected)))
                            }

                            MagicRulesOrFixed::Fixed { fixed } => {
                                Ok((k, CompiledRuleSet::Fixed(fixed)))
                            }
                        }
                    })
                    .try_collect()
            })
            .try_collect()?;
        println!("xxx164, compiled=\n{compiled:?}");
        Ok(compiled)
    }
    pub(crate) fn compile_magic_rule_body(
        &self,
        rule: &MagicInlineRule,
        rule_name: &MagicSymbol,
        store_arities: &BTreeMap<MagicSymbol, usize>,
        ret_vars: &[Symbol],
    ) -> Result<RelAlgebra> {
        let mut ret = RelAlgebra::unit(rule_name.symbol().span);
        let mut seen_variables = BTreeSet::new();
        let mut serial_id = 0;
        let mut gen_symb = |span| {
            let ret = Symbol::new(&format!("**{serial_id}") as &str, span);
            serial_id += 1;
            ret
        };
        for atom in &rule.body {
            match atom {
                MagicAtom::Rule(rule_app) => {
                    let store_arity = store_arities.get(&rule_app.name).ok_or_else(|| {
                        RuleNotFound(
                            rule_app.name.symbol().to_string(),
                            rule_app.name.symbol().span,
                        )
                    })?;

                    ensure!(
                        *store_arity == rule_app.args.len(),
                        ArityMismatch(
                            rule_app.name.symbol().to_string(),
                            *store_arity,
                            rule_app.args.len(),
                            rule_app.span
                        )
                    );
                    let mut prev_joiner_vars = vec![];
                    let mut right_joiner_vars = vec![];
                    let mut right_vars = vec![];

                    for var in &rule_app.args {
                        if seen_variables.contains(var) {
                            prev_joiner_vars.push(var.clone());
                            let rk = gen_symb(var.span);
                            right_vars.push(rk.clone());
                            right_joiner_vars.push(rk);
                        } else {
                            seen_variables.insert(var.clone());
                            right_vars.push(var.clone());
                        }
                    }

                    let right =
                        RelAlgebra::derived(right_vars, rule_app.name.clone(), rule_app.span);
                    debug_assert_eq!(prev_joiner_vars.len(), right_joiner_vars.len());
                    ret = ret.join(right, prev_joiner_vars, right_joiner_vars, rule_app.span);
                }
                MagicAtom::Relation(rel_app) => {
                    let store = self.get_relation(&rel_app.name)?;
                    ensure!(
                        store.arity as usize == rel_app.args.len(),
                        ArityMismatch(
                            rel_app.name.to_string(),
                            store.arity as usize,
                            rel_app.args.len(),
                            rel_app.span
                        )
                    );
                    // already existing vars
                    let mut prev_joiner_vars = vec![];
                    // vars introduced by right and joined
                    let mut right_joiner_vars = vec![];
                    // used to split in case we need to join again
                    let mut right_joiner_vars_pos = vec![];
                    // used to find the right joiner var with the tuple position
                    let mut right_joiner_vars_pos_rev = vec![None; rel_app.args.len()];
                    // vars introduced by right, regardless of joining
                    let mut right_vars = vec![];
                    // used for choosing indices
                    let mut join_indices = vec![];

                    for (i, var) in rel_app.args.iter().enumerate() {
                        if seen_variables.contains(var) {
                            prev_joiner_vars.push(var.clone());
                            let rk = gen_symb(var.span);
                            right_vars.push(rk.clone());
                            right_joiner_vars.push(rk);
                            right_joiner_vars_pos.push(i);
                            right_joiner_vars_pos_rev[i] = Some(right_joiner_vars.len()-1);
                            join_indices.push(IndexPositionUse::Join)
                        } else {
                            seen_variables.insert(var.clone());
                            right_vars.push(var.clone());
                            if var.is_generated_ignored_symbol() {
                                join_indices.push(IndexPositionUse::Ignored)
                            } else {
                                join_indices.push(IndexPositionUse::BindForLater)
                            }
                        }
                    }

                    let name = store.name; // TODO: ronen - not at all sure that's the right name, originally the realation() constructor accepts a store
                    // scan original relation
                    let right = RelAlgebra::relation(
                        right_vars,
                        rel_app.span,
                        name,
                    )?;
                    debug_assert_eq!(prev_joiner_vars.len(), right_joiner_vars.len());
                    ret =
                        ret.join(right, prev_joiner_vars, right_joiner_vars, rel_app.span);
                }
                MagicAtom::Predicate(p) => {
                    ret = ret.filter(p.clone())?;
                }
                MagicAtom::Unification(u) => {
                    if seen_variables.contains(&u.binding) {
                        let expr = if u.one_many_unif {
                            Expr::build_is_in(
                                vec![
                                    Expr::Binding {
                                        var: u.binding.clone(),
                                        tuple_pos: None,
                                    },
                                    u.expr.clone(),
                                ],
                                u.span,
                            )
                        } else {
                            Expr::build_equate(
                                vec![
                                    Expr::Binding {
                                        var: u.binding.clone(),
                                        tuple_pos: None,
                                    },
                                    u.expr.clone(),
                                ],
                                u.span,
                            )
                        };
                        ret = ret.filter(expr)?;
                    } else {
                        seen_variables.insert(u.binding.clone());
                        ret = ret.unify(u.binding.clone(), u.expr.clone(), u.one_many_unif, u.span);
                    }
                }
                MagicAtom::NegatedRule(_) => todo!(),
                MagicAtom::NegatedRelation(_) => todo!(),
            }
        }

        let ret_vars_set = ret_vars.iter().cloned().collect();
        ret.eliminate_temp_vars(&ret_vars_set)?;
        let cur_ret_set: BTreeSet<_> = ret.bindings_after_eliminate().into_iter().collect();
        if cur_ret_set != ret_vars_set {
            let ret_span = ret.span();
            ret = ret.cartesian_join(RelAlgebra::unit(ret_span), ret_span);
            ret.eliminate_temp_vars(&ret_vars_set)?;
        }

        let cur_ret_set: BTreeSet<_> = ret.bindings_after_eliminate().into_iter().collect();
        #[derive(Debug, Error, Diagnostic)]
        #[error("Symbol '{0}' in rule head is unbound")]
        #[diagnostic(code(eval::unbound_symb_in_head))]
        #[diagnostic(help(
            "Note that symbols occurring only in negated positions are not considered bound"
        ))]
        struct UnboundSymbolInRuleHead(String, #[label] SourceSpan);

        ensure!(cur_ret_set == ret_vars_set, {
            let unbound = ret_vars_set.difference(&cur_ret_set).next().unwrap();
            UnboundSymbolInRuleHead(unbound.to_string(), unbound.span)
        });
        let cur_ret_bindings = ret.bindings_after_eliminate();
        if ret_vars != cur_ret_bindings {
            ret = ret.reorder(ret_vars.to_vec());
        }

        Ok(ret)
    }

    pub(crate) fn create_relation(
        &mut self,
        name: String,
        arity: u8,
    ) -> Result<CompiledRelationHandle> {


        if self.compiled_relations.contains_key(&name) {
            bail!(CompiledRelNameConflictError(name))
        };

        let id = self.compiled_relations.len() as u16;

        let key = name.clone();
        let meta = CompiledRelationHandle {
            name,
            id,
            arity,
            keys: vec![],
            non_keys: vec![]
        };


        self.compiled_relations.insert(key, meta.clone());

        Ok(meta)
    }

    pub(crate) fn get_relation(&self, name: &str) -> Result<CompiledRelationHandle> {
        #[derive(Error, Diagnostic, Debug)]
        #[error("Cannot find requested stored relation '{0}'")]
        #[diagnostic(code(query::relation_not_found))]
        struct StoredRelationNotFoundError(String);

        let found = self.compiled_relations
            .get(name)
            .cloned()
            .ok_or_else(|| StoredRelationNotFoundError(name.to_string()));

        Ok(found?)
    }
 
 }
 
 
 #[derive(Debug, Clone)]
 pub enum RelAlgebra {
     Fixed(InlineFixedRA),
     TempStore(TempStoreRA),
     Stored(StoredRA),
     Join(Box<InnerJoin>),
     Reorder(ReorderRA),
     Filter(FilteredRA),
     Unification(UnificationRA),
 }
 
 impl RelAlgebra {
     pub(crate) fn span(&self) -> SourceSpan {
         match self {
             RelAlgebra::Fixed(i) => i.span,
             RelAlgebra::TempStore(i) => i.span,
             RelAlgebra::Stored(i) => i.span,
             RelAlgebra::Join(i) => i.span,
             RelAlgebra::Reorder(i) => i.relation.span(),
             RelAlgebra::Filter(i) => i.span,
             RelAlgebra::Unification(i) => i.span,
         }
     }
     pub(crate) fn is_unit(&self) -> bool {
        if let RelAlgebra::Fixed(r) = self {
            r.bindings.is_empty() && r.data.len() == 1
        } else {
            false
        }
    } 
 }
 
 #[derive(Debug, Clone)]
 pub(crate) struct ReorderRA {
     pub(crate) relation: Box<RelAlgebra>,
     pub(crate) new_order: Vec<Symbol>,
 }
 
 #[derive(Debug, Clone)]
 pub(crate) struct FilteredRA {
     pub(crate) parent: Box<RelAlgebra>,
     pub(crate) filters: Vec<Expr>,
     pub(crate) to_eliminate: BTreeSet<Symbol>,
     pub(crate) span: SourceSpan,
 }
 
 #[derive(Debug, Clone)]
 pub struct InlineFixedRA {
     pub(crate) bindings: Vec<Symbol>,
     pub(crate) data: Vec<Vec<DataValue>>,
     pub(crate) to_eliminate: BTreeSet<Symbol>,
     pub(crate) span: SourceSpan,
 }
 
 #[derive(Debug, Clone)]
 pub struct TempStoreRA {
     pub(crate) bindings: Vec<Symbol>,
     pub(crate) storage_key: MagicSymbol,
     pub(crate) filters: Vec<Expr>,
     pub(crate) span: SourceSpan,
 }
 
 #[derive(Debug, Clone)]
 pub struct StoredRA {
     pub(crate) bindings: Vec<Symbol>,
     pub(crate) filters: Vec<Expr>,
     pub(crate) span: SourceSpan,
     pub(crate) name: String,
 }
 
 #[derive(Debug, Clone)]
 pub struct InnerJoin {
     pub(crate) left: RelAlgebra,
     pub(crate) right: RelAlgebra,
     pub(crate) joiner: Joiner,
     pub(crate) to_eliminate: BTreeSet<Symbol>,
     pub(crate) span: SourceSpan,
 }
 
 #[derive(Debug, Clone)]
 pub(crate) struct Joiner {
     // invariant: these are of the same lengths
     pub(crate) left_keys: Vec<Symbol>,
     pub(crate) right_keys: Vec<Symbol>,
 }
 
 #[derive(Debug, Clone)]
 pub(crate) struct UnificationRA {
     pub(crate) parent: Box<RelAlgebra>,
     pub(crate) binding: Symbol,
     pub(crate) expr: Expr,
     pub(crate) is_multi: bool,
     pub(crate) to_eliminate: BTreeSet<Symbol>,
     pub(crate) span: SourceSpan,
 }
 
 impl RelAlgebra {
     pub(crate) fn unit(span: SourceSpan) -> Self {
         Self::Fixed(InlineFixedRA::unit(span))
     }
 
     pub(crate) fn cartesian_join(self, right: RelAlgebra, span: SourceSpan) -> Self {
         self.join(right, vec![], vec![], span)
     }
 
     pub(crate) fn join(
         self,
         right: RelAlgebra,
         left_keys: Vec<Symbol>,
         right_keys: Vec<Symbol>,
         span: SourceSpan,
     ) -> Self {
         RelAlgebra::Join(Box::new(InnerJoin {
             left: self,
             right,
             joiner: Joiner {
                 left_keys,
                 right_keys,
             },
             to_eliminate: Default::default(),
             span,
         }))
     }
 
     pub(crate) fn reorder(self, new_order: Vec<Symbol>) -> Self {
         Self::Reorder(ReorderRA {
             relation: Box::new(self),
             new_order,
         })
     }
 
     pub(crate) fn bindings_after_eliminate(&self) -> Vec<Symbol> {
         let ret = self.bindings_before_eliminate();
         if let Some(to_eliminate) = self.eliminate_set() {
             ret.into_iter()
                 .filter(|kw| !to_eliminate.contains(kw))
                 .collect()
         } else {
             ret
         }
     }
 
     fn bindings_before_eliminate(&self) -> Vec<Symbol> {
         match self {
             RelAlgebra::Fixed(f) => f.bindings.clone(),
             RelAlgebra::TempStore(d) => d.bindings.clone(),
             RelAlgebra::Stored(v) => v.bindings.clone(),
             RelAlgebra::Join(j) => j.bindings(),
             RelAlgebra::Reorder(r) => r.bindings(),
             RelAlgebra::Filter(r) => r.parent.bindings_after_eliminate(),
             RelAlgebra::Unification(u) => {
                 let mut bindings = u.parent.bindings_after_eliminate();
                 bindings.push(u.binding.clone());
                 bindings
             }
         }
     }
 
     fn eliminate_set(&self) -> Option<&BTreeSet<Symbol>> {
         match self {
             RelAlgebra::Fixed(r) => Some(&r.to_eliminate),
             RelAlgebra::TempStore(_) => None,
             RelAlgebra::Stored(_) => None,
             RelAlgebra::Join(r) => Some(&r.to_eliminate),
             RelAlgebra::Reorder(_) => None,
             RelAlgebra::Filter(r) => Some(&r.to_eliminate),
             RelAlgebra::Unification(u) => Some(&u.to_eliminate),
         }
     }
 
     pub(crate) fn eliminate_temp_vars(&mut self, used: &BTreeSet<Symbol>) -> Result<()> {
         match self {
             RelAlgebra::Fixed(r) => r.do_eliminate_temp_vars(used),
             RelAlgebra::TempStore(_r) => Ok(()),
             RelAlgebra::Stored(_v) => Ok(()),
             RelAlgebra::Join(r) => r.do_eliminate_temp_vars(used),
             RelAlgebra::Reorder(r) => r.relation.eliminate_temp_vars(used),
             RelAlgebra::Filter(r) => r.do_eliminate_temp_vars(used),
             RelAlgebra::Unification(r) => r.do_eliminate_temp_vars(used),
         }
     }
 
     pub(crate) fn filter(self, filter: Expr) -> Result<Self> {
         Ok(match self {
             s @ (RelAlgebra::Fixed(_)
             | RelAlgebra::Reorder(_)
             | RelAlgebra::Unification(_)) => {
                 let span = filter.span();
                 RelAlgebra::Filter(FilteredRA {
                     parent: Box::new(s),
                     filters: vec![filter],
                     to_eliminate: Default::default(),
                     span,
                 })
             }
             RelAlgebra::Filter(FilteredRA {
                 parent,
                 filters: mut pred,
                 to_eliminate,
                 span,
             }) => {
                 pred.push(filter);
                 RelAlgebra::Filter(FilteredRA {
                     parent,
                     filters: pred,
                     to_eliminate,
                     span,
                 })
             }
             RelAlgebra::TempStore(TempStoreRA {
                 bindings,
                 storage_key,
                 mut filters,
                 span,
             }) => {
                 filters.push(filter);
                 RelAlgebra::TempStore(TempStoreRA {
                     bindings,
                     storage_key,
                     filters,
                     span,
                 })
             }
             RelAlgebra::Stored(StoredRA {
                 bindings,
                 mut filters,
                 span,
                 name,
             }) => {
                 filters.push(filter);
                 RelAlgebra::Stored(StoredRA {
                     bindings,
                     filters,
                     span,
                     name,
                 })
             }
             RelAlgebra::Join(inner) => {
                 let filters = filter.to_conjunction();
                 let left_bindings: BTreeSet<Symbol> =
                     inner.left.bindings_before_eliminate().into_iter().collect();
                 let right_bindings: BTreeSet<Symbol> = inner
                     .right
                     .bindings_before_eliminate()
                     .into_iter()
                     .collect();
                 let mut remaining = vec![];
                 let InnerJoin {
                     mut left,
                     mut right,
                     joiner,
                     to_eliminate,
                     span,
                     ..
                 } = *inner;
                 for filter in filters {
                     let f_bindings = filter.bindings()?;
                     if f_bindings.is_subset(&left_bindings) {
                         left = left.filter(filter)?;
                     } else if f_bindings.is_subset(&right_bindings) {
                         right = right.filter(filter)?;
                     } else {
                         remaining.push(filter);
                     }
                 }
                 let mut joined = RelAlgebra::Join(Box::new(InnerJoin {
                     left,
                     right,
                     joiner,
                     to_eliminate,
                     span,
                 }));
                 if !remaining.is_empty() {
                     joined = RelAlgebra::Filter(FilteredRA {
                         parent: Box::new(joined),
                         filters: remaining,
                         to_eliminate: Default::default(),
                         span,
                     });
                 }
                 joined
             }
         })
     }
     pub(crate) fn unify(
         self,
         binding: Symbol,
         expr: Expr,
         is_multi: bool,
         span: SourceSpan,
     ) -> Self {
         RelAlgebra::Unification(UnificationRA {
             parent: Box::new(self),
             binding,
             expr,
             is_multi,
             to_eliminate: Default::default(),
             span,
         })
     }
 
     pub(crate) fn relation(
         bindings: Vec<Symbol>,
         span: SourceSpan,
         name: String,
     ) -> Result<Self> {
         Ok(Self::Stored(StoredRA {
             bindings,
             filters: vec![],
             span,
             name,
         }))
     }
 
     pub(crate) fn derived(
         bindings: Vec<Symbol>,
         storage_key: MagicSymbol,
         span: SourceSpan,
     ) -> Self {
         Self::TempStore(TempStoreRA {
             bindings,
             storage_key,
             filters: vec![],
             span,
         })
     }
 
     pub(crate) fn fill_binding_indices_and_compile(&mut self) -> Result<()> {
         match self {
             RelAlgebra::Fixed(_) => {}
             RelAlgebra::TempStore(d) => {
                 d.fill_binding_indices_and_compile()?;
             }
             RelAlgebra::Stored(v) => {
                 v.fill_binding_indices_and_compile()?;
             }
             RelAlgebra::Reorder(r) => {
                 r.relation.fill_binding_indices_and_compile()?;
             }
             RelAlgebra::Filter(f) => {
                 f.parent.fill_binding_indices_and_compile()?;
                 f.fill_binding_indices_and_compile()?
             }
             RelAlgebra::Unification(u) => {
                 u.parent.fill_binding_indices_and_compile()?;
                 u.fill_binding_indices_and_compile()?
             }
             RelAlgebra::Join(r) => {
                 r.left.fill_binding_indices_and_compile()?;
                 r.right.fill_binding_indices_and_compile()?;
             }
         }
         Ok(())
     }
 
 }
 
 impl InlineFixedRA {
     pub(crate) fn unit(span: SourceSpan) -> Self {
         Self {
             bindings: vec![],
             data: vec![vec![]],
             to_eliminate: Default::default(),
             span,
         }
     }
 
     pub(crate) fn do_eliminate_temp_vars(&mut self, used: &BTreeSet<Symbol>) -> Result<()> {
         for binding in &self.bindings {
             if !used.contains(binding) {
                 self.to_eliminate.insert(binding.clone());
             }
         }
         Ok(())
     }
 }
 
 impl InnerJoin {
     pub(crate) fn bindings(&self) -> Vec<Symbol> {
         let mut ret = self.left.bindings_after_eliminate();
         ret.extend(self.right.bindings_after_eliminate());
         debug_assert_eq!(ret.len(), ret.iter().collect::<BTreeSet<_>>().len());
         ret
     }
 
     pub(crate) fn do_eliminate_temp_vars(&mut self, used: &BTreeSet<Symbol>) -> Result<()> {
         for binding in self.bindings() {
             if !used.contains(&binding) {
                 self.to_eliminate.insert(binding.clone());
             }
         }
         let mut left = used.clone();
         left.extend(self.joiner.left_keys.clone());
         if let Some(filters) = match &self.right {
             RelAlgebra::TempStore(r) => Some(&r.filters),
             _ => None,
         } {
             for filter in filters {
                 left.extend(filter.bindings()?);
             }
         }
         self.left.eliminate_temp_vars(&left)?;
         let mut right = used.clone();
         right.extend(self.joiner.right_keys.clone());
         self.right.eliminate_temp_vars(&right)?;
         Ok(())
     }
 }
 
 impl ReorderRA {
     fn bindings(&self) -> Vec<Symbol> {
         self.new_order.clone()
     }
 }
 
 impl FilteredRA {
     pub(crate) fn do_eliminate_temp_vars(&mut self, used: &BTreeSet<Symbol>) -> Result<()> {
         for binding in self.parent.bindings_before_eliminate() {
             if !used.contains(&binding) {
                 self.to_eliminate.insert(binding.clone());
             }
         }
         let mut nxt = used.clone();
         for e in self.filters.iter() {
             nxt.extend(e.bindings()?);
         }
         self.parent.eliminate_temp_vars(&nxt)?;
         Ok(())
     }
 
 
     fn fill_binding_indices_and_compile(&mut self) -> Result<()> {
         let parent_bindings: BTreeMap<_, _> = self
             .parent
             .bindings_after_eliminate()
             .into_iter()
             .enumerate()
             .map(|(a, b)| (b, a))
             .collect();
         for e in self.filters.iter_mut() {
             e.fill_binding_indices(&parent_bindings)?;
         }
         Ok(())
     }
 }
 
 impl UnificationRA {
     fn fill_binding_indices_and_compile(&mut self) -> Result<()> {
         let parent_bindings: BTreeMap<_, _> = self
             .parent
             .bindings_after_eliminate()
             .into_iter()
             .enumerate()
             .map(|(a, b)| (b, a))
             .collect();
         self.expr.fill_binding_indices(&parent_bindings)?;
         Ok(())
     }
 
     pub(crate) fn do_eliminate_temp_vars(&mut self, used: &BTreeSet<Symbol>) -> Result<()> {
         for binding in self.parent.bindings_before_eliminate() {
             if !used.contains(&binding) {
                 self.to_eliminate.insert(binding.clone());
             }
         }
         let mut nxt = used.clone();
         nxt.extend(self.expr.bindings()?);
         self.parent.eliminate_temp_vars(&nxt)?;
         Ok(())
     }
 }
 
 #[derive(Debug, Clone, Eq, PartialEq, serde_derive::Deserialize, serde_derive::Serialize)]
 pub(crate) struct ColumnDef {
     pub(crate) name: SmartString<LazyCompact>,
     pub(crate) typing: NullableColType,
     pub(crate) default_gen: Option<Expr>,
 }
 
 #[derive(Debug, Clone, Eq, PartialEq, serde_derive::Deserialize, serde_derive::Serialize)]
 pub enum ColType {
     Any,
     Bool,
     Int,
     Float,
     String,
     Bytes,
     Uuid,
     List {
         eltype: Box<NullableColType>,
         len: Option<usize>,
     },
     Tuple(Vec<NullableColType>),
     Validity,
     Json,
 }
 
 #[derive(Debug, Clone, Eq, PartialEq, serde_derive::Deserialize, serde_derive::Serialize)]
 pub struct NullableColType {
     pub coltype: ColType,
     pub nullable: bool,
 }
 
 impl StoredRA {
     fn fill_binding_indices_and_compile(&mut self) -> Result<()> {
         let bindings: BTreeMap<_, _> = self
             .bindings
             .iter()
             .cloned()
             .enumerate()
             .map(|(a, b)| (b, a))
             .collect();
         for e in self.filters.iter_mut() {
             e.fill_binding_indices(&bindings)?;
         }
         Ok(())
     }
 }
 
 impl TempStoreRA {
     fn fill_binding_indices_and_compile(&mut self) -> Result<()> {
         let bindings: BTreeMap<_, _> = self
             .bindings
             .iter()
             .cloned()
             .enumerate()
             .map(|(a, b)| (b, a))
             .collect();
         for e in self.filters.iter_mut() {
             e.fill_binding_indices(&bindings)?;
         }
         Ok(())
     }
 }
 
 impl Compiler {
    pub fn new() -> Self {
        Compiler {
            compiled_relations: BTreeMap::new(),
            fixed_rules: Vec::new(),
            relations: HashMap::new(),
            rules: HashMap::new(),
        }
    }

    fn do_compile_script(
        &mut self,
        payload: &str,
        param_pool: &BTreeMap<String, DataValue>,
        cur_vld: ValidityTs,
    ) -> Result<Vec<BTreeMap<MagicSymbol, CompiledRuleSet>>> {
        match parse_script(
            payload,
            param_pool,
            &BTreeMap::new(),
            cur_vld,
        )? {
            CozoScript::Single(p) => self.compile_single(cur_vld, p),
            _ => todo!("it's a bug")
        }
    }

    fn compile_single(
        &mut self,
        cur_vld: ValidityTs,
        p: InputProgram,
    ) -> Result<Vec<BTreeMap<MagicSymbol, CompiledRuleSet>>, Report> {
        let mut callback_collector = BTreeMap::new();
        let callback_targets = Default::default();
        let res;
        {

            res = self.compile_single_program(
                p,
                cur_vld,
                &callback_targets,
                &mut callback_collector,
            )?;


        }

        Ok(res)
    }

    pub(crate) fn compile_single_program(
        &mut self,
        p: InputProgram,
        cur_vld: ValidityTs,
        callback_targets: &BTreeSet<SmartString<LazyCompact>>,
        callback_collector: &mut CallbackCollector,
    ) -> Result<Vec<BTreeMap<MagicSymbol, CompiledRuleSet>>> {
        let compiled =
            self.compile_query(p, cur_vld, callback_targets, callback_collector, true)?;
        Ok(compiled)
    }

    /// This is the entry to query evaluation
    pub(crate) fn compile_query(
        &mut self,
        input_program: InputProgram,
        cur_vld: ValidityTs,
        callback_targets: &BTreeSet<SmartString<LazyCompact>>,
        callback_collector: &mut CallbackCollector,
        top_level: bool,
    ) -> Result<Vec<BTreeMap<MagicSymbol, CompiledRuleSet>>> {
        // cleanups contain stored relations that should be deleted at the end of query

        // Some checks in case the query specifies mutation
        if let Some((meta, op, _)) = &input_program.out_opts.store_relation {
            if *op == RelationOp::Create {
                #[derive(Debug, Error, Diagnostic)]
                #[error("Stored relation {0} conflicts with an existing one")]
                #[diagnostic(code(eval::stored_relation_conflict))]
                struct StoreRelationConflict(String);

                ensure!(
                    !self.relation_exists(&meta.name),
                    StoreRelationConflict(meta.name.to_string())
                );

                let arity = meta.metadata.keys.len() as u8; // TODO: ronen - not sure this is the arity of the relation, check latedr
                self.create_relation(meta.name.name.to_string(), arity)?;
            }
        };

        // query compilation
        let entry_head_or_default = input_program.get_entry_out_head_or_default()?;
        let (normalized_program, out_opts) = input_program.into_normalized_program(self)?;
        let (stratified_program, store_lifetimes) = normalized_program.into_stratified_program()?;
        let program = stratified_program.magic_sets_rewrite(self)?;
        let compiled = self.stratified_magic_compile(program)?;

        Ok(compiled)

    }
 
     /// Compile the CozoScript passed in. The `params` argument is a map of parameters.
     pub fn compile_script(
         &mut self,
         payload: &str,
     ) -> Result<Vec<BTreeMap<MagicSymbol, CompiledRuleSet>>> {
        let params: BTreeMap<String, DataValue> = BTreeMap::new();
        println!("xxx404");
         let cur_vld = current_validity();
         self.do_compile_script(
             payload,
             &params,
             cur_vld,
         )
     }

 }

 #[derive(Debug)]
pub(crate) struct StratifiedNormalFormProgram(pub(crate) Vec<NormalFormProgram>);

#[derive(Debug, Default)]
pub(crate) struct NormalFormProgram {
    pub(crate) prog: BTreeMap<Symbol, NormalFormRulesOrFixed>,
    pub(crate) disable_magic_rewrite: bool,
}

#[derive(Debug)]
pub(crate) enum NormalFormRulesOrFixed {
    Rules { rules: Vec<NormalFormInlineRule> },
    Fixed { fixed: FixedRuleApply },
}

#[derive(Debug)]
pub(crate) struct NormalFormInlineRule {
    pub(crate) head: Vec<Symbol>,
    pub(crate) aggr: Vec<Option<(Aggregation, Vec<DataValue>)>>,
    pub(crate) body: Vec<NormalFormAtom>,
}

#[derive(Clone, Debug)]
pub(crate) struct FixedRuleApply {
    pub(crate) fixed_handle: FixedRuleHandle,
    pub(crate) rule_args: Vec<FixedRuleArg>,
    pub(crate) options: Arc<BTreeMap<SmartString<LazyCompact>, Expr>>,
    pub(crate) head: Vec<Symbol>,
    pub(crate) arity: usize,
    pub(crate) span: SourceSpan,
    pub(crate) fixed_impl: Arc<Box<dyn FixedRule>>,
}

#[derive(Debug, Clone)]
pub(crate) enum NormalFormAtom {
    Rule(NormalFormRuleApplyAtom),
    Relation(NormalFormRelationApplyAtom),
    NegatedRule(NormalFormRuleApplyAtom),
    NegatedRelation(NormalFormRelationApplyAtom),
    Predicate(Expr),
    Unification(Unification),
}

#[derive(Clone, Debug)]
pub(crate) struct Unification {
    pub(crate) binding: Symbol,
    pub(crate) expr: Expr,
    pub(crate) one_many_unif: bool,
    pub(crate) span: SourceSpan,
}

impl Unification {
    pub(crate) fn is_const(&self) -> bool {
        matches!(self.expr, Expr::Const { .. })
    }
    pub(crate) fn bindings_in_expr(&self) -> Result<BTreeSet<Symbol>> {
        self.expr.bindings()
    }
}

#[derive(Clone, Debug)]
pub(crate) struct NormalFormRelationApplyAtom {
    pub(crate) name: Symbol,
    pub(crate) args: Vec<Symbol>,
    pub(crate) valid_at: Option<ValidityTs>,
    pub(crate) span: SourceSpan,
}

#[derive(Clone, Debug)]
pub(crate) struct NormalFormRuleApplyAtom {
    pub(crate) name: Symbol,
    pub(crate) args: Vec<Symbol>,
    pub(crate) span: SourceSpan,
}



pub fn explain_compiled(strata: &[CompiledProgram]) -> Result<NamedRows> {
    let mut ret: Vec<JsonValue> = vec![];
    const STRATUM: &str = "stratum";
    const ATOM_IDX: &str = "atom_idx";
    const OP: &str = "op";
    const RULE_IDX: &str = "rule_idx";
    const RULE_NAME: &str = "rule";
    const REF_NAME: &str = "ref";
    const OUT_BINDINGS: &str = "out_relation";
    const JOINS_ON: &str = "joins_on";
    const FILTERS: &str = "filters/expr";

    let headers = vec![
        STRATUM.to_string(),
        RULE_IDX.to_string(),
        RULE_NAME.to_string(),
        ATOM_IDX.to_string(),
        OP.to_string(),
        REF_NAME.to_string(),
        JOINS_ON.to_string(),
        FILTERS.to_string(),
        OUT_BINDINGS.to_string(),
    ];

    for (stratum, p) in strata.iter().enumerate() {
        let mut clause_idx = -1;
        for (rule_name, v) in p {
            match v {
                CompiledRuleSet::Rules(rules) => {
                    for CompiledRule { aggr, relation, .. } in rules.iter() {
                        clause_idx += 1;
                        let mut ret_for_relation = vec![];
                        let mut rel_stack = vec![relation];
                        let mut idx = 0;
                        let mut atom_type = "out";
                        for (a, _) in aggr.iter().flatten() {
                            if a.is_meet {
                                if atom_type == "out" {
                                    atom_type = "meet_aggr_out";
                                }
                            } else {
                                atom_type = "aggr_out";
                            }
                        }

                        ret_for_relation.push(json!({
                            STRATUM: stratum,
                            ATOM_IDX: idx,
                            OP: atom_type,
                            RULE_IDX: clause_idx,
                            RULE_NAME: rule_name.to_string(),
                            OUT_BINDINGS: relation.bindings_after_eliminate().into_iter().map(|v| v.to_string()).collect_vec()
                        }));
                        idx += 1;

                        while let Some(rel) = rel_stack.pop() {
                            let (atom_type, ref_name, joins_on, filters) = match rel {
                                r @ RelAlgebra::Fixed(..) => {
                                    if r.is_unit() {
                                        continue;
                                    }
                                    ("fixed", json!(null), json!(null), json!(null))
                                }
                                RelAlgebra::TempStore(TempStoreRA {
                                    storage_key,
                                    filters,
                                    ..
                                }) => (
                                    "load_mem",
                                    json!(storage_key.to_string()),
                                    json!(null),
                                    json!(filters.iter().map(|f| f.to_string()).collect_vec()),
                                ),
                                RelAlgebra::Stored(StoredRA {
                                    name, filters, ..
                                }) => (
                                    "load_stored",
                                    json!(format!(":{}", name)),
                                    json!(null),
                                    json!(filters.iter().map(|f| f.to_string()).collect_vec()),
                                ),
                                RelAlgebra::Join(inner) => {
                                    if inner.left.is_unit() {
                                        rel_stack.push(&inner.right);
                                        continue;
                                    }
                                    let t = inner.join_type();
                                    let InnerJoin {
                                        left,
                                        right,
                                        joiner,
                                        ..
                                    } = inner.as_ref();
                                    rel_stack.push(left);
                                    rel_stack.push(right);
                                    (t, json!(null), json!(joiner.as_map()), json!(null))
                                }
                                RelAlgebra::Reorder(ReorderRA { relation, .. }) => {
                                    rel_stack.push(relation);
                                    ("reorder", json!(null), json!(null), json!(null))
                                }
                                RelAlgebra::Filter(FilteredRA {
                                    parent,
                                    filters: pred,
                                    ..
                                }) => {
                                    rel_stack.push(parent);
                                    (
                                        "filter",
                                        json!(null),
                                        json!(null),
                                        json!(pred.iter().map(|f| f.to_string()).collect_vec()),
                                    )
                                }
                                RelAlgebra::Unification(UnificationRA {
                                    parent,
                                    binding,
                                    expr,
                                    is_multi,
                                    ..
                                }) => {
                                    rel_stack.push(parent);
                                    (
                                        if *is_multi { "multi-unify" } else { "unify" },
                                        json!(binding.name),
                                        json!(null),
                                        json!(expr.to_string()),
                                    )
                                }
                            };
                            ret_for_relation.push(json!({
                                STRATUM: stratum,
                                ATOM_IDX: idx,
                                OP: atom_type,
                                RULE_IDX: clause_idx,
                                RULE_NAME: rule_name.to_string(),
                                REF_NAME: ref_name,
                                OUT_BINDINGS: rel.bindings_after_eliminate().into_iter().map(|v| v.to_string()).collect_vec(),
                                JOINS_ON: joins_on,
                                FILTERS: filters,
                            }));
                            idx += 1;
                        }
                        ret_for_relation.reverse();
                        ret.extend(ret_for_relation)
                    }
                }
                CompiledRuleSet::Fixed(_) => ret.push(json!({
                    STRATUM: stratum,
                    ATOM_IDX: 0,
                    OP: "algo",
                    RULE_IDX: 0,
                    RULE_NAME: rule_name.to_string(),
                })),
            }
        }
    }

    let rows = ret
        .into_iter()
        .map(|m| {
            headers
                .iter()
                .map(|i| DataValue::from(m.get(i).unwrap_or(&JsonValue::Null)))
                .collect_vec()
        })
        .collect_vec();

    Ok(NamedRows::new(headers, rows))
}


impl Joiner {
    pub(crate) fn as_map(&self) -> BTreeMap<&str, &str> {
        self.left_keys
            .iter()
            .zip(self.right_keys.iter())
            .map(|(l, r)| (&l.name as &str, &r.name as &str))
            .collect()
    }

    pub(crate) fn join_indices(
        &self,
        left_bindings: &[Symbol],
        right_bindings: &[Symbol],
    ) -> Result<(Vec<usize>, Vec<usize>)> {
        let left_binding_map = left_bindings
            .iter()
            .enumerate()
            .map(|(k, v)| (v, k))
            .collect::<BTreeMap<_, _>>();
        let right_binding_map = right_bindings
            .iter()
            .enumerate()
            .map(|(k, v)| (v, k))
            .collect::<BTreeMap<_, _>>();
        let mut ret_l = Vec::with_capacity(self.left_keys.len());
        let mut ret_r = Vec::with_capacity(self.left_keys.len());
        for (l, r) in self.left_keys.iter().zip(self.right_keys.iter()) {
            let l_pos = left_binding_map.get(l).unwrap();
            let r_pos = right_binding_map.get(r).unwrap();
            ret_l.push(*l_pos);
            ret_r.push(*r_pos)
        }
        Ok((ret_l, ret_r))
    }

}

impl InlineFixedRA {
    pub(crate) fn join_type(&self) -> &str {
        if self.data.is_empty() {
            "null_join"
        } else if self.data.len() == 1 {
            "singleton_join"
        } else {
            "fixed_join"
        }
    }
}

impl InnerJoin {
    pub(crate) fn join_type(&self) -> &str {
        match &self.right {
            RelAlgebra::Fixed(f) => f.join_type(),
            RelAlgebra::TempStore(_) => {
                let join_indices = self
                    .joiner
                    .join_indices(
                        &self.left.bindings_after_eliminate(),
                        &self.right.bindings_after_eliminate(),
                    )
                    .unwrap();
                if join_is_prefix(&join_indices.1) {
                    "mem_prefix_join"
                } else {
                    "mem_mat_join"
                }
            }
            RelAlgebra::Stored(_) => {
                let join_indices = self
                    .joiner
                    .join_indices(
                        &self.left.bindings_after_eliminate(),
                        &self.right.bindings_after_eliminate(),
                    )
                    .unwrap();
                if join_is_prefix(&join_indices.1) {
                    "stored_prefix_join"
                } else {
                    "stored_mat_join"
                }
            }
            RelAlgebra::Join(_) | RelAlgebra::Filter(_) | RelAlgebra::Unification(_) => {
                "generic_mat_join"
            }
            RelAlgebra::Reorder(_) => {
                panic!("joining on reordered")
            }
        }
    }
}

fn join_is_prefix(right_join_indices: &[usize]) -> bool {
    // We do not consider partial index match to be "prefix", e.g. [a, u => c]
    // with a, c bound and u unbound is not "prefix", as it is not clear that
    // using prefix scanning in this case will really save us computation.
    let mut indices = right_join_indices.to_vec();
    indices.sort();
    let l = indices.len();
    indices.into_iter().eq(0..l)
}
