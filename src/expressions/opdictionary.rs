use crate::ColumnWrapper;
use crate::InitDictionary;
use std::any::TypeId;
use std::collections::HashMap;

pub enum InputTypes<'a> {
    Ref(&'a ColumnWrapper),
    Owned(ColumnWrapper),
}

pub type Operation = fn(&mut ColumnWrapper, Vec<InputTypes>);
pub type OpDictionary = HashMap<Signature, Operation>;

#[derive(Clone, PartialEq, Hash, Eq, Debug)]
pub enum Binding {
    RefColumn(usize),
    OwnedColumn,
    ConstValue(usize),
    Expr(Box<Expression>),
}

#[derive(Clone, PartialEq, Hash, Eq, Debug)]
pub struct Signature {
    op_name: String,
    output: TypeId,
    input: Vec<TypeId>,
    output_typename: String,
    input_typenames: Vec<String>,
}

impl Signature {
    pub fn new(
        op: &str,
        output: TypeId,
        input: Vec<TypeId>,
        output_typename: String,
        input_typenames: Vec<String>,
    ) -> Self {
        Self {
            op_name: op.to_string(),
            output,
            input,
            output_typename,
            input_typenames,
        }
    }
    pub fn new_with_output<T: 'static>(op: &str) -> Self {
        Self {
            op_name: op.to_string(),
            output: TypeId::of::<T>(),
            input: vec![],
            output_typename: std::any::type_name::<T>().to_string(),
            input_typenames: vec![],
        }
    }

    pub fn add_input<T: 'static>(&mut self) {
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

    pub fn as_output_sig(&self) -> Self {
        Self {
            op_name: "new".to_string(),
            output: self.output,
            input: vec![],
            output_typename: self.output_typename.clone(),
            input_typenames: vec![],
        }
    }
}

#[macro_export]
macro_rules! sig {
    ($elem:expr; $output:ty) => (
        Signature::new_with_output::<$output>(
            $elem,
        )
        );
    ($elem:expr; $output:ty; $($x:ty),+ $(,)?) => (
    {
        let mut s=Signature::new_with_output::<$output>(
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

    pub fn output_type(&self) -> TypeId {
        self.op.output
    }
    pub fn output_typename(&self) -> &String {
        &self.op.output_typename
    }

    fn eval_recursive(
        &self,
        owned_columns: &mut Vec<&mut ColumnWrapper>,
        ref_columns: &Vec<&ColumnWrapper>,
        const_values: &Vec<&ColumnWrapper>,
        dict: &OpDictionary,
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

        let op = dict.get(&self.op).unwrap();
        op(&mut output, input);
        owned_columns.push(output);
    }

    pub fn eval(
        &self,
        owned_columns: &mut Vec<&mut ColumnWrapper>,
        ref_columns: &Vec<&ColumnWrapper>,
        const_values: &Vec<&ColumnWrapper>,
        dict: &OpDictionary,
    ) {
        self.eval_recursive(owned_columns, ref_columns, const_values, dict);
        assert_eq!(owned_columns.len(), 1);
    }

    fn compile_recursive(
        &self,
        dict: &OpDictionary,
        init_dict: &InitDictionary,
        ops: &mut Vec<Operation>,
        owned_columns: &mut Vec<ColumnWrapper>,
    ) {
        self.input.iter().rev().for_each(|inp| match inp {
            Binding::OwnedColumn | Binding::RefColumn(_) | Binding::ConstValue(_) => {}
            Binding::Expr(expr) => {
                (*expr).compile_recursive(dict, init_dict, ops, owned_columns);
            }
        });

        match &self.output {
            Binding::OwnedColumn => {
                let output_sig = &self.op.as_output_sig();
                let f = init_dict.get(output_sig).unwrap();
                owned_columns.push(f());
                //println!("{}", output_sig.op_name())
            }
            Binding::Expr(expr) => {
                (*expr).compile_recursive(dict, init_dict, ops, owned_columns);
            }
            Binding::RefColumn(_) | Binding::ConstValue(_) => {
                panic!("Incorrect expression output type")
            }
        }

        let op = dict.get(&self.op).unwrap();
        ops.push(*op);
    }

    pub fn compile(
        &self,
        dict: &OpDictionary,
        init_dict: &InitDictionary,
    ) -> (Vec<Operation>, Vec<ColumnWrapper>) {
        let mut ops: Vec<Operation> = Vec::new();
        let mut owned_columns: Vec<ColumnWrapper> = Vec::new();
        self.compile_recursive(dict, init_dict, &mut ops, &mut owned_columns);
        (ops, owned_columns)
    }
}
