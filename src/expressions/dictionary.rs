use std::any::{Any, TypeId};
use std::collections::HashMap;

pub type Operation = fn(&mut dyn Any, Vec<&dyn Any>);

pub type Dictionary = HashMap<Signature, Operation>;

#[derive(Clone, PartialEq, Hash, Eq, Debug)]
pub enum Binding {
    RefColumn,
    OwnedColumn,
    ConstValue,
    Expr(Box<Expression>),
}

#[derive(Clone, PartialEq, Hash, Eq, Debug)]
pub struct Expression {
    op: Signature,
    output: Binding,
    input: Vec<Binding>,
}

#[derive(Clone, PartialEq, Hash, Eq, Debug)]
pub struct Signature {
    op_name: String,
    output: TypeId,
    input: Vec<TypeId>,
}

impl Signature {
    pub fn new(op: &str, output: TypeId, input: Vec<TypeId>) -> Self {
        Self {
            op_name: op.to_string(),
            output,
            input,
        }
    }
    pub fn input_len(&self) -> usize {
        self.input.len()
    }

    pub fn op_name(&self) -> &String {
        &self.op_name
    }
}

#[macro_export]
macro_rules! sig {
    ($elem:expr; $output:ty) => (
        Signature::new(
            $elem,
            TypeId::of::<$output>(),
            vec! []
        )
        );
    ($elem:expr; $output:ty; $($x:ty),+ $(,)?) => (
    Signature::new(
        $elem,
        TypeId::of::<$output>(),
        vec! [$(TypeId::of::<$x>()),+]
    )
    );
}

impl Expression {
    pub fn new(sig: Signature, output: Binding, input: Vec<Binding>) -> Self {
        Self {
            op: sig,
            output,
            input,
        }
    }
    pub fn eval_recursive(
        &self,
        owned_columns: &mut Vec<&mut dyn Any>,
        ref_columns: &mut Vec<&dyn Any>,
        const_values: &mut Vec<&dyn Any>,
        dict: &Dictionary,
    ) {
        let output: &mut dyn Any = match &self.output {
            Binding::OwnedColumn => owned_columns.pop().unwrap(),
            Binding::Expr(expr) => {
                (*expr).eval_recursive(owned_columns, ref_columns, const_values, dict);
                owned_columns.pop().unwrap()
            }
            Binding::RefColumn | Binding::ConstValue => panic!("Incorrect expression output type"),
        };

        let input: Vec<&dyn Any> = self
            .input
            .iter()
            .map(|b| match b {
                Binding::OwnedColumn => &(*owned_columns.pop().unwrap()),
                Binding::RefColumn => ref_columns.pop().unwrap(),
                Binding::ConstValue => const_values.pop().unwrap(),
                Binding::Expr(expr) => {
                    (*expr).eval_recursive(owned_columns, ref_columns, const_values, dict);
                    owned_columns.pop().unwrap()
                }
            })
            .collect();

        let op = dict.get(&self.op).unwrap();
        op(output, input);
        owned_columns.push(output);
    }

    pub fn eval(
        &self,
        owned_columns: &mut Vec<&mut dyn Any>,
        ref_columns: &mut Vec<&dyn Any>,
        const_values: &mut Vec<&dyn Any>,
        dict: &Dictionary,
    ) {
        self.eval_recursive(owned_columns, ref_columns, const_values, dict);
        assert_eq!(owned_columns.len(), 1);
    }
}
