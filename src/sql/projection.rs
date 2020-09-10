use crate::*;
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

fn column_ref<'a>(name: &str, input: &'a Vec<ColumnWrapper>) -> &'a ColumnWrapper {
    let pos = input.iter().position(|c| c.name() == name).unwrap();
    &input[pos]
}

pub fn parseexpr_rec<'a>(
    expr: &Expr,
    input: &'a Vec<ColumnWrapper>,
    ref_columns: &mut Vec<&'a ColumnWrapper>,
) -> Expression {
    match expr {
        Expr::BinaryOp { left, op, right } => match (&(**left), &(**right)) {
            (Expr::Identifier(lhs), Expr::Identifier(rhs)) => {
                let lhs_col = column_ref(&lhs.value, input);
                let rhs_col = column_ref(&rhs.value, input);
                let op_name = op_name_init(op);
                let signature = Signature::new(
                    &op_name,
                    lhs_col.typeid(),
                    vec![lhs_col.typeid(), rhs_col.typeid()],
                );
                ref_columns.push(rhs_col);
                ref_columns.push(lhs_col);
                Expression::new(
                    signature,
                    Binding::OwnedColumn,
                    vec![Binding::RefColumn, Binding::RefColumn],
                )
            }
            (Expr::Identifier(lhs), e) => {
                let expr_right = parseexpr_rec(e, input, ref_columns);

                let lhs_col = column_ref(&lhs.value, input);

                let op_name = op_name_init(op);
                let signature = Signature::new(
                    &op_name,
                    lhs_col.typeid(),
                    vec![lhs_col.typeid(), expr_right.output_type()],
                );
                ref_columns.push(column_ref(&lhs.value, input));
                Expression::new(
                    signature,
                    Binding::OwnedColumn,
                    vec![Binding::RefColumn, Binding::Expr(Box::new(expr_right))],
                )
            }

            (e, Expr::Identifier(rhs)) => {
                let rhs_col = column_ref(&rhs.value, input);
                ref_columns.push(rhs_col);
                let expr_left = parseexpr_rec(e, input, ref_columns);

                let op_name = op_name(op);
                let signature =
                    Signature::new(&op_name, expr_left.output_type(), vec![rhs_col.typeid()]);

                Expression::new(
                    signature,
                    Binding::Expr(Box::new(expr_left)),
                    vec![Binding::RefColumn],
                )
            }

            _ => panic!(),
        },
        Expr::Nested(e) => parseexpr_rec(&(**e), input, ref_columns),
        _ => panic!(),
    }
}

pub fn parseexpr<'a>(
    expr: &Expr,
    input: &'a Vec<ColumnWrapper>,
) -> (Expression, Vec<&'a ColumnWrapper>) {
    let s1 = sig!["+"; Vec<u32>;Vec<u32>, Vec<u32>];
    let mut ref_columns: Vec<&ColumnWrapper> = Vec::new();
    let expr_output = parseexpr_rec(expr, input, &mut ref_columns);
    (expr_output, ref_columns)
}
