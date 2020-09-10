use crate::bitmap::Bitmap;
use concat_idents::concat_idents;
use core::any::TypeId;

use std::hash::{BuildHasher, Hash, Hasher};

use rayon::prelude::*;

use crate::*;

#[allow(dead_code)]
const OP: &str = "hash=";

macro_rules! binary_operation_load {
    ($dict:ident; $($tr:ty)+) => ($(
        concat_idents!(fn_name = hash, _, vecu64,_,vec,$tr {
            let signature=sig![OP;Vec<u64>; Vec<$tr>];
            $dict.insert(signature, fn_name);
        });
    )+)
}

macro_rules! binary_operation_impl {
    ($($tr:ty)+) => ($(
        concat_idents!(fn_name = hash, _, vecu64,_,vec,$tr {
            #[allow(dead_code)]
            fn fn_name(output: &mut ColumnWrapper, input: Vec<InputTypes>)  {

                let rs=ahash::RandomState::with_seeds(1234,5678);

                type T1=u64;
                type T2=$tr;

                //naming convention:
                // left->output
                //right[0]-->input
                //if right[0] and right[1]-> input_lhs, input_rhs

                let (data_output, index_output, bitmap_output) = output.all_mut::<Vec<T1>>();

                let (data_input, index_input, bitmap_input) = match &input[0] {
                    InputTypes::Ref(a)=>(a.downcast_ref::<Vec<T1>>(), a.index().as_ref(), a.bitmap().as_ref()),
                    InputTypes::Owned(a)=>(a.downcast_ref::<Vec<T1>>(), a.index().as_ref(), a.bitmap().as_ref())
                };

                let len_input = if let Some(ind) = index_input {
                    ind.len()
                } else {
                    data_input.len()
                };

                //Clean up
                data_output.truncate(0);
                *index_output=None;
                *bitmap_output=None;
                //Reserve enough storage for result
                data_output.reserve(len_input);


                match (&index_input, &bitmap_input) {
                    (Some(ind), None) => data_output.par_extend(
                        ind.par_iter().map(|i| &data_input[*i])
                        .map(|r|  {
                            let mut h=rs.build_hasher();
                            r.hash(&mut h); h.finish()
                        })),
                    (Some(ind), Some(b_right)) => data_output.par_extend(
                        ind.par_iter().map(|i| &data_input[*i])
                        .zip_eq(b_right.bits.par_iter())
                        .map(|(r, b_r)| {
                            if *b_r != 0 {
                                {let mut h=rs.build_hasher(); r.hash(&mut h); h.finish()}
                            } else {
                                u64::MAX
                            }
                        })),

                    (None, None) => data_output.par_extend(
                        data_input.par_iter()
                        .map(|r| {let mut h=rs.build_hasher(); r.hash(&mut h); h.finish()})),

                    (None, Some(b_right)) => data_output.par_extend(
                        data_input.par_iter()
                        .zip_eq(b_right.bits.par_iter())
                        .map(|(r, b_r)| {
                            if *b_r != 0 {
                                {let mut h=rs.build_hasher(); r.hash(&mut h); h.finish()}
                            } else {
                                u64::MAX
                            }
                        })),
                };

                if let Some(bmap)=&bitmap_input{
                    if let Some(ind)=&index_input{
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
