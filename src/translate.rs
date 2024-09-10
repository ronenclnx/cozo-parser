use crate::compile::{CompiledProgram, CompiledRuleSet, InlineFixedRA, InnerJoin, RelAlgebra, StoredRA, TempStoreRA};



#[derive(Clone, Debug)]
pub enum DiffdafRelation {
    Join,
    Predicate(String),
}

#[derive(Clone, Debug)]
pub struct DiffdafRule {
    name: String,
    relation: DiffdafRelation
}

#[derive(Clone, Debug)]
pub struct DiffDaffProgram(Vec<DiffdafRule>);


pub fn translate_relation(relation: &RelAlgebra) -> DiffdafRelation {
    let translated = match relation {
        crate::compile::RelAlgebra::Fixed(_) => todo!(),
        crate::compile::RelAlgebra::TempStore(_) => todo!(),
        crate::compile::RelAlgebra::Stored(_) => todo!(),
        crate::compile::RelAlgebra::Join(  b) => {
            let InnerJoin{ left, right, joiner, to_eliminate, span } = (**b).clone();

            if let RelAlgebra::Fixed(InlineFixedRA{ bindings, data, to_eliminate, span }) = left{
                if data == vec![vec![]] {
                    // this is Fixed Unit rule join??? workaround we need to understand

                    if let RelAlgebra::Stored(StoredRA{ bindings, filters, span, name }) = right {
                        DiffdafRelation::Predicate(name)
                    } else if let RelAlgebra::TempStore(TempStoreRA{ bindings, storage_key, filters, span }) = right {
                        DiffdafRelation::Predicate(storage_key.to_string())
                    } else {
                        todo!()
                    }
                } else {
                    todo!()
                }
            } else {
                todo!()
            }
        },
        crate::compile::RelAlgebra::Reorder(_) => todo!(),
        crate::compile::RelAlgebra::Filter(_) => todo!(),
        crate::compile::RelAlgebra::Unification(_) => todo!(),
    };
    
    translated
}

pub fn translate_program(program: &CompiledProgram) -> DiffDaffProgram {
    let rules = 
    program.into_iter().map(|(k,v)| {
        DiffdafRule {
            name: k.to_string(),
            relation: {
                match v {
                    // TODO: this assumes only one rule per ruleset, as this is all ive seen till now, unlikely to be right, find when
                    CompiledRuleSet::Rules(rules) => translate_relation(&rules[0].relation),
                    _ => todo!()
                }
            }
        }
    }).collect();

    DiffDaffProgram(rules)
}