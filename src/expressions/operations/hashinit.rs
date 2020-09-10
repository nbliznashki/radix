use crate::bitmap::Bitmap;
use crate::{Column, ColumnMut, OwnedColumn};
use concat_idents::concat_idents;
use core::any::{Any, TypeId};

use std::hash::{BuildHasher, Hash, Hasher};

use rayon::prelude::*;

use crate::*;

#[allow(dead_code)]
const OP: &str = "hash=";

macro_rules! binary_operation_load {
    ($dict:ident; $($tr:ty)+) => ($(
        concat_idents!(fn_name = hash, _, ownedcolumnvecu64,_,ownedcolumnvec,$tr {
            let signature=sig![OP;OwnedColumn<Vec<u64>>; OwnedColumn<Vec<$tr>>];
            $dict.insert(signature, fn_name);
        });
    )+)
}

macro_rules! binary_operation_impl {
    ($($tr:ty)+) => ($(
        concat_idents!(fn_name = hash, _, ownedcolumnvecu64,_,ownedcolumnvec,$tr {
            #[allow(dead_code)]
            fn fn_name(left: &mut dyn Any, right: Vec<InputTypes>) {

                let rs=ahash::RandomState::with_seeds(1234,5678);

                type T2=$tr;

                let output = left.downcast_mut::<OwnedColumn<Vec<u64>>>().unwrap();
                let down_right = match &right[0] {
                    InputTypes::Ref(a)=>a.downcast_ref::<OwnedColumn<Vec<T2>>>().unwrap(),
                    InputTypes::Owned(a)=>a.downcast_ref::<OwnedColumn<Vec<T2>>>().unwrap()
                };




                let bitmap_right = &down_right.bitmap().as_ref();
                let index_right = &down_right.index().as_ref();
                let data_right = &down_right.col();


                let len_right = if let Some(ind) = index_right {
                    ind.len()
                } else {
                    data_right.len()
                };

                //Clean up
                let (data_output, index_output, bitmap_output) = output.all_mut();
                data_output.truncate(0);
                *index_output=None;
                *bitmap_output=None;
                //Reserve enough storage for result
                data_output.reserve(len_right);


                match (&index_right, &bitmap_right) {
                    (Some(ind), None) => data_output.par_extend(
                        ind.par_iter().map(|i| &data_right[*i])
                        .map(|r|  {
                            let mut h=rs.build_hasher();
                            r.hash(&mut h); h.finish()
                        })),
                    (Some(ind), Some(b_right)) => data_output.par_extend(
                        ind.par_iter().map(|i| &data_right[*i])
                        .zip_eq(b_right.bits.par_iter())
                        .map(|(r, b_r)| {
                            if *b_r != 0 {
                                {let mut h=rs.build_hasher(); r.hash(&mut h); h.finish()}
                            } else {
                                u64::MAX
                            }
                        })),

                    (None, None) => data_output.par_extend(
                        data_right.par_iter()
                        .map(|r| {let mut h=rs.build_hasher(); r.hash(&mut h); h.finish()})),

                    (None, Some(b_right)) => data_output.par_extend(
                        data_right.par_iter()
                        .zip_eq(b_right.bits.par_iter())
                        .map(|(r, b_r)| {
                            if *b_r != 0 {
                                {let mut h=rs.build_hasher(); r.hash(&mut h); h.finish()}
                            } else {
                                u64::MAX
                            }
                        })),
                };

                if let Some(bmap)=&bitmap_right{
                    if let Some(ind)=&index_right{
                        *bitmap_output=Some(Bitmap{bits: ind.par_iter().map(|i| bmap.bits[*i]).collect()});
                    } else {
                        *bitmap_output=Some(Bitmap{bits: bmap.bits.par_iter().map(|i| *i).collect()});
                    }
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

pub(crate) fn load_op_dict(dict: &mut OpDictionary) {
    //dict.insert(s, columnadd_onwedcolumnvecu64_vecu64);7
    binary_operation_load! {dict;

        u64 u32 u16 u8 bool

    };
}
