use crate::*;
use sqlparser::ast::{BinaryOperator, Expr};

fn op_name(op: &BinaryOperator) -> Result<String, ErrorDesc> {
    let op_name = match op {
        BinaryOperator::Plus => "+=",
        BinaryOperator::Minus => "-=",
        BinaryOperator::Eq => "===",
        BinaryOperator::GtEq => ">==",
        BinaryOperator::Gt => ">==",
        _ => "unimplemented",
    };
    if op_name == "unimplemented" {
        Err(format!("Operation {} not implemented", op_name))?
    } else {
        Ok(op_name.to_string())
    }
}

fn op_name_init(op: &BinaryOperator) -> Result<String, ErrorDesc> {
    let op_name = match op {
        BinaryOperator::Plus => "+",
        BinaryOperator::Minus => "-",
        BinaryOperator::Eq => "==",
        BinaryOperator::GtEq => ">=",
        BinaryOperator::Gt => ">",
        _ => "unimplemented",
    };

    if op_name == "unimplemented" {
        Err(format!("Operation {} not implemented", op_name))?
    } else {
        Ok(op_name.to_string())
    }
}

fn column_ref<'a>(
    name: &str,
    input: &'a Vec<&ColumnWrapper>,
) -> Result<(&'a ColumnWrapper<'a>, usize), ErrorDesc> {
    let pos = input.iter().position(|c| c.name().as_deref() == Some(name));
    if let Some(pos) = pos {
        Ok((&input[pos], pos))
    } else {
        Err(format!(
            "Column {} not found in the list of input columns",
            name
        ))?
    }
}

pub fn parseexpr_rec<'a>(
    expr: &Expr,
    input: &'a Vec<&ColumnWrapper>,
    dict: &Dictionary,
) -> Result<Expression, ErrorDesc> {
    match expr {
        Expr::BinaryOp { left, op, right } => match (&(**left), &(**right)) {
            (Expr::Identifier(lhs), Expr::Identifier(rhs)) => {
                let (lhs_col, lhs_pos) = column_ref(&lhs.value, input)?;
                let (rhs_col, rhs_pos) = column_ref(&rhs.value, input)?;
                let op_name = op_name_init(op)?;
                let signature = Signature::new(
                    &op_name,
                    vec![lhs_col.typeid(), rhs_col.typeid()],
                    vec![lhs_col.typename().clone(), rhs_col.typename().clone()],
                );

                Ok(Expression::new(
                    signature,
                    Binding::OwnedColumn,
                    vec![Binding::RefColumn(lhs_pos), Binding::RefColumn(rhs_pos)],
                ))
            }
            (Expr::Identifier(lhs), e) => {
                let expr_right = parseexpr_rec(e, input, dict)?;

                let (lhs_col, lhs_pos) = column_ref(&lhs.value, input)?;

                let op_name = op_name_init(op)?;
                let signature = Signature::new(
                    &op_name,
                    vec![lhs_col.typeid(), expr_right.output_type(dict)?],
                    vec![
                        lhs_col.typename().clone(),
                        expr_right.output_typename(dict)?,
                    ],
                );

                Ok(Expression::new(
                    signature,
                    Binding::OwnedColumn,
                    vec![
                        Binding::RefColumn(lhs_pos),
                        Binding::Expr(Box::new(expr_right)),
                    ],
                ))
            }

            (e, Expr::Identifier(rhs)) => {
                let (rhs_col, rhs_pos) = column_ref(&rhs.value, input)?;
                let expr_left = parseexpr_rec(e, input, dict)?;

                let op_name = op_name(op)?;
                let signature = Signature::new(
                    &op_name,
                    vec![expr_left.output_type(dict)?, rhs_col.typeid()],
                    vec![expr_left.output_typename(dict)?, rhs_col.typename().clone()],
                );

                Ok(Expression::new(
                    signature,
                    Binding::Expr(Box::new(expr_left)),
                    vec![Binding::RefColumn(rhs_pos)],
                ))
            }

            _ => panic!(),
        },
        Expr::Nested(e) => parseexpr_rec(&(**e), input, dict),
        Expr::Function(f) => {
            if f.name.0[0].value == "hash" {
                if f.args.len() == 0 {
                    Err(format!("Function supplied has no arguments: {:?}", f))?
                }

                let e = f.args[0].clone();
                match e {
                    Expr::Identifier(col) => {
                        let (_col, _pos) = column_ref(&col.value, input)?;

                        let _op_name = "hash=";
                        Err("Hash not yet implemented")?
                    }
                    _ => Err("Only hash(col) supported")?,
                }
            } else {
                Err("Only the function hash is implemented")?
            }
        }
        _ => Err(format!("Expression not implemented: {:?}", expr))?,
    }
}

pub fn parseexpr(
    expr: &Expr,
    input: &Vec<&ColumnWrapper>,
    dict: &Dictionary,
) -> Result<Expression, ErrorDesc> {
    let expr_output = parseexpr_rec(expr, input, dict);
    expr_output
}
