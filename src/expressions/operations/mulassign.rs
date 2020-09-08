use crate::bitmap::Bitmap;
use crate::expressions::dictionary::Dictionary;
use crate::expressions::dictionary::Signature;
use crate::{Column, ColumnMut, OwnedColumn};
use concat_idents::concat_idents;
use core::any::{Any, TypeId};
use std::sync::Arc;

use std::ops::MulAssign;

use rayon::prelude::*;

use crate::*;

#[allow(dead_code)]
const OP: &str = "*=";

macro_rules! binary_operation_load {
    ($dict:ident; $(($tl:ty, $tr:ty))+) => ($(
        concat_idents!(fn_name = mulassign, _, ownedcolumnvec,$tl,_,arcvec,$tr {
            let signature=sig![OP;OwnedColumn<Vec<$tl>>; Arc<Vec<$tr>>];
            $dict.insert(signature, fn_name);
        });
    )+)
}

macro_rules! binary_operation_impl {
    ($(($tl:ty, $tr:ty))+) => ($(
        concat_idents!(fn_name = mulassign, _, ownedcolumnvec,$tl,_,arcvec,$tr {
            #[allow(dead_code)]
            fn fn_name(left: &mut dyn Any, right: Vec<&dyn Any>) {

                type T1=$tl;
                type T2=$tr;

                let down_left = left.downcast_mut::<OwnedColumn<Vec<T1>>>().unwrap();
                let down_right = right[0].downcast_ref::<Arc<Vec<T2>>>().unwrap();

                let (data_left, index_left, bitmap_left) = down_left.all_mut();

                //The output column should have no index
                assert_eq!(index_left, &None);

                let bitmap_right = &down_right.bitmap().as_ref();
                let index_right = &down_right.index().as_ref();
                let data_right = &down_right.col();

                let left_len = data_left.len();
                let right_len = if let Some(ind) = index_right {
                    ind.len()
                } else {
                    data_right.len()
                };

                assert_eq!(left_len, right_len);

                match (&index_right, &bitmap_left, &bitmap_right) {
                    (Some(ind), None, None) => data_left
                        .par_iter_mut()
                        .zip_eq(ind.par_iter().map(|i| &data_right[*i]))
                        .for_each(|(l, r)| l.mul_assign( T1::from(*r))),
                    (Some(ind), Some(b_left), None) => data_left
                        .par_iter_mut()
                        .zip_eq(ind.par_iter().map(|i| &data_right[*i]))
                        .zip_eq(b_left.bits.par_iter())
                        .for_each(|((l, r), b_l)| {
                            l.mul_assign( if *b_l != 0 {
                                T1::from(*r)
                            } else {
                                Default::default()
                            })
                        }),
                    (Some(ind), None, Some(b_right)) => data_left
                        .par_iter_mut()
                        .zip_eq(ind.par_iter().map(|i| &data_right[*i]))
                        .zip_eq(b_right.bits.par_iter())
                        .for_each(|((l, r), b_r)| {
                            l.mul_assign(if *b_r != 0 {
                                T1::from(*r)
                            } else {
                                Default::default()
                            })
                        }),
                    (Some(ind), Some(b_left), Some(b_right)) => data_left
                        .par_iter_mut()
                        .zip_eq(ind.par_iter().map(|i| &data_right[*i]))
                        .zip_eq(b_left.bits.par_iter())
                        .zip_eq(b_right.bits.par_iter())
                        .for_each(|(((l, r), b_l), b_r)| {
                            l.mul_assign(if (*b_l != 0) & (*b_r != 0) {
                                T1::from(*r)
                            } else {
                                Default::default()
                            })
                        }),

                    (None, None, None) => data_left
                        .par_iter_mut()
                        .zip_eq(data_right.par_iter())
                        .for_each(|(l, r)| l.mul_assign(T1::from(*r))),
                    (None, Some(b_left), None) => data_left
                        .par_iter_mut()
                        .zip_eq(data_right.par_iter())
                        .zip_eq(b_left.bits.par_iter())
                        .for_each(|((l, r), b_l)| {
                            l.mul_assign(if *b_l != 0 {
                                T1::from(*r)
                            } else {
                                Default::default()
                            })
                        }),
                    (None, None, Some(b_right)) => data_left
                        .par_iter_mut()
                        .zip_eq(data_right.par_iter())
                        .zip_eq(b_right.bits.par_iter())
                        .for_each(|((l, r), b_r)| {
                            l.mul_assign(if *b_r != 0 {
                                T1::from(*r)
                            } else {
                                Default::default()
                            })
                        }),
                    (None, Some(b_left), Some(b_right)) => data_left
                        .par_iter_mut()
                        .zip_eq(data_right.par_iter())
                        .zip_eq(b_left.bits.par_iter())
                        .zip_eq(b_right.bits.par_iter())
                        .for_each(|(((l, r), b_l), b_r)| {
                            l.mul_assign(if (*b_l != 0) & (*b_r != 0) {
                                T1::from(*r)
                            } else {
                                Default::default()
                            })
                        }),
                }
                if bitmap_left.is_none() {
                    *bitmap_left = match (index_right, bitmap_right) {
                        (_, None) => None,
                        (None, Some(b_right)) => Some((*b_right).clone()),
                        (Some(ind), Some(b_right)) => Some(Bitmap {
                            bits: ind.par_iter().map(|i| b_right.bits[*i]).collect(),
                        }),
                    };
                } else {
                    let mut b_left = bitmap_left.take().unwrap();
                    match (index_right, bitmap_right) {
                        (_, None) => {}
                        (None, Some(b_right)) => b_left
                            .bits
                            .par_iter_mut()
                            .zip_eq(b_right.bits.par_iter())
                            .for_each(|(b_l, b_r)| *b_l &= b_r),
                        (Some(ind), Some(b_right)) => b_left
                            .bits
                            .par_iter_mut()
                            .zip_eq(ind.par_iter())
                            .for_each(|(b_l, i)| *b_l &= b_right.bits[*i]),
                    };
                    *bitmap_left = Some(b_left);
                }
            }

    });
    )+)
}

binary_operation_impl! {

(u64, u64) (u64, u32) (u64, u16) (u64, u8) (u64, bool)

}

pub(crate) fn init_dict(dict: &mut Dictionary) {
    binary_operation_load! {dict;
            (u64, u64) (u64, u32) (u64, u16) (u64, u8) (u64, bool)
    };
}
