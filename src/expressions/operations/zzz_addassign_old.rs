use crate::bitmap::Bitmap;
use crate::expressions::dictionary::Dictionary;
use crate::expressions::dictionary::Signature;
use crate::{Column, ColumnMut, OwnedColumn};
use concat_idents::concat_idents;
use core::any::{Any, TypeId};
use core::ops::AddAssign;
use core::ops::Deref;
use core::ops::DerefMut;
use rayon::prelude::*;

use crate::*;

macro_rules! columnadd_assign_load {
    ($dict:ident; $(($tl:ty, $tr:ty))+) => ($(
        concat_idents!(fn_name = columnadd, _, ownedcolumnvec,$tl,_,vec,$tr {
            let signature=sig!["+=";OwnedColumn<Vec<$tl>>; Vec<$tr>];
            $dict.insert(signature, fn_name);
        });
    )+)
}

macro_rules! columnadd_assign_impl {
    ($(($tl:ty, $tr:ty))+) => ($(
        concat_idents!(fn_name = columnadd, _, ownedcolumnvec,$tl,_,vec,$tr {
        fn fn_name(left: &mut dyn Any, right: Vec<&dyn Any>) {
            let down_left = left.downcast_mut::<OwnedColumn<Vec<$tl>>>().unwrap();
            let down_right = right[0].downcast_ref::<Vec<$tr>>().unwrap();

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

            let bitmap_dummy_left = (0..left_len).into_par_iter().map(|_| &1u8);
            let bitmap_dummy_right = (0..right_len).into_par_iter().map(|_| &1u8);

            match (&index_right, &bitmap_left, &bitmap_right) {
                (Some(ind), None, None) => columnadd(
                    data_left,
                    ind.par_iter().map(|i| &data_right[*i]),
                    bitmap_dummy_left,
                    bitmap_dummy_right,
                ),
                (Some(ind), Some(b_left), None) => columnadd(
                    data_left,
                    ind.par_iter().map(|i| &data_right[*i]),
                    b_left.bits.par_iter(),
                    bitmap_dummy_right,
                ),
                (Some(ind), None, Some(b_right)) => columnadd(
                    data_left,
                    ind.par_iter().map(|i| &data_right[*i]),
                    bitmap_dummy_left,
                    ind.par_iter().map(|i| &b_right.bits[*i]),
                ),
                (Some(ind), Some(b_left), Some(b_right)) => columnadd(
                    data_left,
                    ind.par_iter().map(|i| &data_right[*i]),
                    b_left.bits.par_iter(),
                    ind.par_iter().map(|i| &b_right.bits[*i]),
                ),

                (None, None, None) => columnadd(
                    data_left,
                    data_right.par_iter(),
                    bitmap_dummy_left,
                    bitmap_dummy_right,
                ),
                (None, Some(b_left), None) => columnadd(
                    data_left,
                    data_right.par_iter(),
                    b_left.bits.par_iter(),
                    bitmap_dummy_right,
                ),
                (None, None, Some(b_right)) => columnadd(
                    data_left,
                    data_right.par_iter(),
                    bitmap_dummy_left,
                    b_right.bits.par_iter(),
                ),
                (None, Some(b_left), Some(b_right)) => columnadd(
                    data_left,
                    data_right.par_iter(),
                    b_left.bits.par_iter(),
                    b_right.bits.par_iter(),
                ),
            }

            if let Some(b_right) = bitmap_right {
                match &index_right {
                    Some(ind) => {
                        bitmap_and(bitmap_left, ind.par_iter().map(|i| &b_right.bits[*i]));
                    }
                    None => {
                        bitmap_and(bitmap_left, b_right.bits.par_iter());
                    }
                };
            }
        }

    });
    )+)
}

fn columnadd<U1, U2, U3, U4, T1, T2, T3, T4, D2>(
    left_data: &mut U1,
    right_data: U2,
    left_index: U3,
    right_index: U4,
) where
    U1: DerefMut<Target = [T1]>,
    U2: IndexedParallelIterator<Item = T2>,
    U3: IndexedParallelIterator<Item = T3>,
    U4: IndexedParallelIterator<Item = T4>,
    T1: AddAssign,
    T1: Default,
    T1: From<D2>,
    D2: Copy,
    T1: Send + Sync,
    T2: Send + Sync,
    T3: Send + Sync,
    T4: Send + Sync,
    T2: Deref<Target = D2>,
    T3: Deref<Target = u8>,
    T4: Deref<Target = u8>,
{
    left_data
        .par_iter_mut()
        .zip_eq(right_data.into_par_iter())
        .zip_eq(left_index.into_par_iter())
        .zip_eq(right_index.into_par_iter())
        .for_each(|(((l, r), lb), rb)| {
            *l += if ((*lb) != 0) && ((*rb) != 0) {
                T1::from(*r)
            } else {
                Default::default()
            }
        });
}

fn bitmap_and<U2, T3>(left: &mut Option<Bitmap>, right: U2)
where
    U2: IndexedParallelIterator<Item = T3>,
    T3: Send + Sync,
    T3: Deref<Target = u8>,
{
    if let Some(bitmap) = left {
        bitmap
            .bits
            .par_iter_mut()
            .zip_eq(right.into_par_iter())
            .for_each(|(l, r)| *l &= *r);
    } else {
        *left = Some(Bitmap {
            bits: right.into_par_iter().map(|i| *i).collect(),
        });
    }
}

columnadd_assign_impl! {
(usize, usize) (usize, u16) (usize, u8)  (usize, bool)
(u8, u8) (u8, bool)
(u16, u16) (u16, u8) (u16, bool)
(u32, u32) (u32, u16) (u32, u8) (u32, bool)
(u64, u64) (u64, u32) (u64, u16) (u64, u8) (u64, bool)
(u128, u128) (u128, u64) (u128, u32) (u128, u16) (u128, u8) (u128, bool)
(isize, isize)  (isize, i16) (isize, i8) (isize, u8) (isize, bool)
(i8, i8) (i8, bool)
(i16, i16) (i16, i8) (i16, u8) (i16, bool)
(i32, i32)  (i32, i16) (i32, u16)  (i32, i8) (i32, u8) (i32, bool)
(i64, i64) (i64, i32) (i64, u32)  (i64, i16) (i64, u16)  (i64, i8) (i64, u8) (i64, bool)
(i128, i128) (i128, i64) (i128, u64) (i128, i32) (i128, u32)  (i128, i16) (i128, u16)  (i128, i8) (i128, u8) (i128, bool)
(f32, f32) (f32, i16) (f32, u16)  (f32, i8) (f32, u8)
(f64, f64) (f64, f32) (f64, i32) (f64, u32) (f64, i16) (f64, u16) (f64, i8) (f64, u8)
}

//{ usize u8 u16 u32 u64 u128 isize i8 i16 i32 i64 i128 f32 f64 }

//columnadd_assign_impl! { (u64,u8) (u64,u16) (u64,u32) (u64,u64) }

pub(crate) fn init_dict(dict: &mut Dictionary) {
    //dict.insert(s, columnadd_onwedcolumnvecu64_vecu64);7
    columnadd_assign_load! {dict;
            (usize, usize) (usize, u16) (usize, u8)  (usize, bool)
            (u8, u8) (u8, bool)
            (u16, u16) (u16, u8) (u16, bool)
            (u32, u32) (u32, u16) (u32, u8) (u32, bool)
            (u64, u64) (u64, u32) (u64, u16) (u64, u8) (u64, bool)
            (u128, u128) (u128, u64) (u128, u32) (u128, u16) (u128, u8) (u128, bool)
            (isize, isize)  (isize, i16) (isize, i8) (isize, u8) (isize, bool)
            (i8, i8) (i8, bool)
            (i16, i16) (i16, i8) (i16, u8) (i16, bool)
            (i32, i32)  (i32, i16) (i32, u16)  (i32, i8) (i32, u8) (i32, bool)
            (i64, i64) (i64, i32) (i64, u32)  (i64, i16) (i64, u16)  (i64, i8) (i64, u8) (i64, bool)
            (i128, i128) (i128, i64) (i128, u64) (i128, i32) (i128, u32)  (i128, i16) (i128, u16)  (i128, i8) (i128, u8) (i128, bool)
            (f32, f32) (f32, i16) (f32, u16)  (f32, i8) (f32, u8)
            (f64, f64) (f64, f32) (f64, i32) (f64, u32) (f64, i16) (f64, u16) (f64, i8) (f64, u8)
    };
}
