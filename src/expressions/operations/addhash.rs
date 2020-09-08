use crate::bitmap::Bitmap;
use crate::expressions::dictionary::Dictionary;
use crate::expressions::dictionary::Signature;
use crate::{Column, ColumnMut, OwnedColumn};
use concat_idents::concat_idents;
use core::any::{Any, TypeId};
use std::sync::Arc;

use std::hash::{BuildHasher, Hash, Hasher};
use std::ops::AddAssign;

use rayon::prelude::*;

use crate::*;

#[allow(dead_code)]
const OP: &str = "hash+=";

macro_rules! binary_operation_load {
    ($dict:ident; $($tr:ty)+) => ($(
        concat_idents!(fn_name = hashadd, _, ownedcolumnvecu64,_,arcvec,$tr {
            let signature=sig![OP;OwnedColumn<Vec<u64>>; Arc<Vec<$tr>>];
            $dict.insert(signature, fn_name);
        });
    )+)
}

macro_rules! binary_operation_impl {
    ($($tr:ty)+) => ($(
        concat_idents!(fn_name = hashadd, _, ownedcolumnvecu64,_,arcvec,$tr {
            #[allow(dead_code)]
            fn fn_name(left: &mut dyn Any, right: Vec<&dyn Any>) {

                let rs=ahash::RandomState::with_seeds(1234,5678);

                type T2=$tr;

                let down_left = left.downcast_mut::<OwnedColumn<Vec<u64>>>().unwrap();
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

                match (&index_right, &bitmap_right) {
                    (Some(ind), None) => data_left
                        .par_iter_mut()
                        .zip_eq(ind.par_iter().map(|i| &data_right[*i]))
                        .for_each(|(l, r)| l.add_assign( {let mut h=rs.build_hasher(); r.hash(&mut h); h.finish()})),
                    (Some(ind), Some(b_right)) => data_left
                        .par_iter_mut()
                        .zip_eq(ind.par_iter().map(|i| &data_right[*i]))
                        .zip_eq(b_right.bits.par_iter())
                        .for_each(|((l, r), b_r)| {
                            l.add_assign(if *b_r != 0 {
                                {let mut h=rs.build_hasher(); r.hash(&mut h); h.finish()}
                            } else {
                                u64::MAX
                            })
                        }),

                    (None, None) => data_left
                        .par_iter_mut()
                        .zip_eq(data_right.par_iter())
                        .for_each(|(l, r)| l.add_assign({let mut h=rs.build_hasher(); r.hash(&mut h); h.finish()})),

                    (None, Some(b_right)) => data_left
                        .par_iter_mut()
                        .zip_eq(data_right.par_iter())
                        .zip_eq(b_right.bits.par_iter())
                        .for_each(|((l, r), b_r)| {
                            l.add_assign(if *b_r != 0 {
                                {let mut h=rs.build_hasher(); r.hash(&mut h); h.finish()}
                            } else {
                                u64::MAX
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

u64 u32 u16 u8 bool

}

//{ usize u8 u16 u32 u64 u128 isize i8 i16 i32 i64 i128 f32 f64 }

//binary_operation_impl! { (u64,u8) (u64,u16) (u64,u32) (u64,u64) }

pub(crate) fn init_dict(dict: &mut Dictionary) {
    //dict.insert(s, columnadd_onwedcolumnvecu64_vecu64);7
    binary_operation_load! {dict;

        u64 u32 u16 u8 bool

    };
}
