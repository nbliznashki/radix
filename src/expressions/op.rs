use crate::{ColumnWrapper, Dictionary};
use std::any::TypeId;
use std::collections::HashMap;

pub enum InputTypes<'a> {
    Ref(&'a ColumnWrapper<'a>),
    Owned(ColumnWrapper<'static>),
}
#[derive(Clone)]
pub struct Operation {
    pub f: fn(&mut ColumnWrapper, Vec<InputTypes>),
    pub output_type: std::any::TypeId,
    pub output_typename: String,
}

//pub type Operation = fn(&mut ColumnWrapper, Vec<InputTypes>);
pub type OpDictionary = HashMap<Signature, Operation>;

#[derive(Clone, PartialEq, Hash, Eq, Debug)]
pub enum Binding {
    RefColumn(usize),
    OwnedColumn,
    ConstValue(usize),
    Expr(Box<Expression>),
}

#[derive(Clone, Hash, Debug)]
pub struct Signature {
    op_name: String,
    input: Vec<TypeId>,
    input_typenames: Vec<String>,
}

impl PartialEq for Signature {
    fn eq(&self, other: &Self) -> bool {
        self.input == other.input && self.op_name == other.op_name
    }
}

impl Eq for Signature {}

impl Signature {
    pub fn new(op: &str, input: Vec<TypeId>, input_typenames: Vec<String>) -> Self {
        Self {
            op_name: op.to_string(),
            input,
            input_typenames,
        }
    }
    pub fn new_op(op: &str) -> Self {
        Self {
            op_name: op.to_string(),
            input: vec![],
            input_typenames: vec![],
        }
    }

    pub fn add_input<T: 'static + ?Sized>(&mut self) {
        self.input.push(TypeId::of::<T>());
        self.input_typenames
            .push(std::any::type_name::<T>().to_string());
    }

    pub fn input_len(&self) -> usize {
        self.input.len()
    }

    pub fn op_name(&self) -> &String {
        &self.op_name
    }

    pub fn as_output_sig(&self, dict: &Dictionary) -> Self {
        let op = dict.op.get(&self).unwrap();
        Self {
            op_name: "new".to_string(),
            input: vec![op.output_type],
            input_typenames: vec![op.output_typename.clone()],
        }
    }
}

#[macro_export]
macro_rules! sig {
    ($elem:expr) => (
        Signature::new_op(
            $elem,
        )
        );
    ($elem:expr; $($x:ty),+ $(,)?) => (
    {
        let mut s=Signature::new_op(
            $elem,
        );
        $(s.add_input::<$x>();)+
        s
    }
    );
}
#[derive(Clone, PartialEq, Hash, Eq, Debug)]
pub struct Expression {
    op: Signature,
    output: Binding,
    input: Vec<Binding>,
}
impl Expression {
    pub fn new(sig: Signature, output: Binding, input: Vec<Binding>) -> Self {
        Self {
            op: sig,
            output,
            input,
        }
    }

    fn eval_recursive(
        &self,
        owned_columns: &mut Vec<&mut ColumnWrapper>,
        ref_columns: &Vec<&ColumnWrapper>,
        const_values: &Vec<&ColumnWrapper>,
        dict: &Dictionary,
    ) {
        let mut output: &mut ColumnWrapper = match &self.output {
            Binding::OwnedColumn => owned_columns.pop().unwrap(),
            Binding::Expr(expr) => {
                (*expr).eval_recursive(owned_columns, ref_columns, const_values, dict);
                owned_columns.pop().unwrap()
            }
            Binding::RefColumn(_) | Binding::ConstValue(_) => {
                panic!("Incorrect expression output type")
            }
        };

        let input: Vec<InputTypes> = self
            .input
            .iter()
            .map(|b| match b {
                Binding::OwnedColumn => InputTypes::Ref(owned_columns.pop().unwrap()),
                Binding::RefColumn(i) => InputTypes::Ref(ref_columns[*i]),
                Binding::ConstValue(i) => InputTypes::Ref(const_values[*i]),
                Binding::Expr(expr) => {
                    (*expr).eval_recursive(owned_columns, ref_columns, const_values, dict);
                    InputTypes::Ref(owned_columns.pop().unwrap())
                }
            })
            .collect();

        let op = dict.op.get(&self.op).unwrap();
        (op.f)(&mut output, input);
        owned_columns.push(output);
    }

    pub fn eval(
        &self,
        owned_columns: &mut Vec<&mut ColumnWrapper>,
        ref_columns: &Vec<&ColumnWrapper>,
        const_values: &Vec<&ColumnWrapper>,
        dict: &Dictionary,
    ) {
        self.eval_recursive(owned_columns, ref_columns, const_values, dict);
        assert_eq!(owned_columns.len(), 1);
    }

    fn compile_recursive(
        &self,
        dict: &Dictionary,
        ops: &mut Vec<Operation>,
        owned_columns: &mut Vec<ColumnWrapper>,
    ) {
        self.input.iter().rev().for_each(|inp| match inp {
            Binding::OwnedColumn | Binding::RefColumn(_) | Binding::ConstValue(_) => {}
            Binding::Expr(expr) => {
                (*expr).compile_recursive(dict, ops, owned_columns);
            }
        });

        match &self.output {
            Binding::OwnedColumn => {
                let output_sig = &self.op.as_output_sig(dict);
                let f = dict.init.get(output_sig).unwrap();
                owned_columns.push(f());
                //println!("{}", output_sig.op_name())
            }
            Binding::Expr(expr) => {
                (*expr).compile_recursive(dict, ops, owned_columns);
            }
            Binding::RefColumn(_) | Binding::ConstValue(_) => {
                panic!("Incorrect expression output type")
            }
        }
        let op = dict.op.get(&self.op).unwrap();

        ops.push(op.clone());
    }

    pub fn compile(&self, dict: &Dictionary) -> (Vec<Operation>, Vec<ColumnWrapper>) {
        let mut ops: Vec<Operation> = Vec::new();
        let mut owned_columns: Vec<ColumnWrapper> = Vec::new();
        self.compile_recursive(dict, &mut ops, &mut owned_columns);
        (ops, owned_columns)
    }

    pub fn output_type(&self, dict: &Dictionary) -> TypeId {
        self.op.as_output_sig(dict).input.pop().unwrap()
    }

    pub fn output_typename(&self, dict: &Dictionary) -> String {
        self.op.as_output_sig(dict).input_typenames.pop().unwrap()
    }
}
