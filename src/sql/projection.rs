use crate::*;
use core::iter::once;
use sqlparser::ast::{BinaryOperator, Expr};
use std::any::TypeId;

fn op_name(op: &BinaryOperator) -> String {
    let op_name = match op {
        BinaryOperator::Plus => "+=",
        BinaryOperator::Minus => "-=",
        _ => panic!(),
    };
    op_name.to_string()
}

fn op_name_init(op: &BinaryOperator) -> String {
    let op_name = match op {
        BinaryOperator::Plus => "+",
        BinaryOperator::Minus => "-",
        _ => panic!(),
    };
    op_name.to_string()
}

fn column_ref<'a>(name: &str, input: &'a Vec<ColumnWrapper>) -> (&'a ColumnWrapper, usize) {
    let pos = input
        .iter()
        .position(|c| c.name().as_deref() == Some(name))
        .unwrap();
    (&input[pos], pos)
}

pub fn parseexpr_rec<'a>(expr: &Expr, input: &'a Vec<ColumnWrapper>) -> Expression {
    match expr {
        Expr::BinaryOp { left, op, right } => match (&(**left), &(**right)) {
            (Expr::Identifier(lhs), Expr::Identifier(rhs)) => {
                let (lhs_col, lhs_pos) = column_ref(&lhs.value, input);
                let (rhs_col, rhs_pos) = column_ref(&rhs.value, input);
                let op_name = op_name_init(op);
                let signature = Signature::new(
                    &op_name,
                    lhs_col.typeid(),
                    vec![lhs_col.typeid(), rhs_col.typeid()],
                    lhs_col.typename().clone(),
                    vec![lhs_col.typename().clone(), rhs_col.typename().clone()],
                );
                Expression::new(
                    signature,
                    Binding::OwnedColumn,
                    vec![Binding::RefColumn(lhs_pos), Binding::RefColumn(rhs_pos)],
                )
            }
            (Expr::Identifier(lhs), e) => {
                let expr_right = parseexpr_rec(e, input);

                let (lhs_col, lhs_pos) = column_ref(&lhs.value, input);

                let op_name = op_name_init(op);
                let signature = Signature::new(
                    &op_name,
                    lhs_col.typeid(),
                    vec![lhs_col.typeid(), expr_right.output_type()],
                    lhs_col.typename().clone(),
                    vec![
                        lhs_col.typename().clone(),
                        expr_right.output_typename().clone(),
                    ],
                );
                Expression::new(
                    signature,
                    Binding::OwnedColumn,
                    vec![
                        Binding::RefColumn(lhs_pos),
                        Binding::Expr(Box::new(expr_right)),
                    ],
                )
            }

            (e, Expr::Identifier(rhs)) => {
                let (rhs_col, rhs_pos) = column_ref(&rhs.value, input);
                let expr_left = parseexpr_rec(e, input);

                let op_name = op_name(op);
                let signature = Signature::new(
                    &op_name,
                    expr_left.output_type(),
                    vec![rhs_col.typeid()],
                    expr_left.output_typename().clone(),
                    vec![rhs_col.typename().clone()],
                );

                Expression::new(
                    signature,
                    Binding::Expr(Box::new(expr_left)),
                    vec![Binding::RefColumn(rhs_pos)],
                )
            }

            _ => panic!(),
        },
        Expr::Nested(e) => parseexpr_rec(&(**e), input),
        _ => panic!(),
    }
}

pub fn parseexpr(expr: &Expr, input: &Vec<ColumnWrapper>) -> Expression {
    let s1 = sig!["+"; Vec<u32>;Vec<u32>, Vec<u32>];
    let expr_output = parseexpr_rec(expr, input);
    expr_output
}
