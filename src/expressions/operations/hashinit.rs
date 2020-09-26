use crate::bitmap::Bitmap;
use paste::paste;

use std::hash::{BuildHasher, Hash, Hasher};

use crate::*;

#[allow(dead_code)]
const OP: &str = "hash=";

macro_rules! binary_operation_load {
    ($dict:ident; $($tr:ty)+) => ($(
            let signature=sig![OP;Vec<$tr>];
            let op=Operation{
                f: paste!{[<hash_vecu64_vec_ $tr:lower>]},
                output_type: std::any::TypeId::of::<Vec<u64>>(),
                output_typename: std::any::type_name::<Vec<u64>>().to_string()
            };
            $dict.insert(signature, op);
    )+)
}

macro_rules! binary_operation_impl {
    ($($tr:ty)+) => ($(
            #[allow(dead_code)]
            paste!{
            fn [<hash_vecu64_vec_ $tr:lower>](output: &mut ColumnWrapper, input: Vec<InputTypes>)  {

                let rs=ahash::RandomState::with_seeds(1234,5678);

                type T1=u64;
                //type T2=$tr;

                //naming convention:
                // left->output
                //right[0]-->input
                //if right[0] and right[1]-> input_lhs, input_rhs

                let (data_output, index_output, bitmap_output) = output.all_mut::<Vec<T1>>();

                let (data_input, index_input, bitmap_input) = match &input[0] {
                    InputTypes::Ref(a)=>(a.downcast_ref::<Vec<T1>>(), a.index(), a.bitmap()),
                    InputTypes::Owned(a)=>(a.downcast_ref::<Vec<T1>>(), a.index(), a.bitmap())
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
                    (Some(ind), None) => data_output.extend(
                        ind.iter().map(|i| &data_input[*i])
                        .map(|r|  {
                            let mut h=rs.build_hasher();
                            r.hash(&mut h); h.finish()
                        })),
                    (Some(ind), Some(b_right)) => data_output.extend(
                        ind.iter().map(|i| &data_input[*i])
                        .zip(b_right.iter())
                        .map(|(r, b_r)| {
                            if *b_r != 0 {
                                {let mut h=rs.build_hasher(); r.hash(&mut h); h.finish()}
                            } else {
                                u64::MAX
                            }
                        })),

                    (None, None) => data_output.extend(
                        data_input.iter()
                        .map(|r| {let mut h=rs.build_hasher(); r.hash(&mut h); h.finish()})),

                    (None, Some(b_right)) => data_output.extend(
                        data_input.iter()
                        .zip(b_right.iter())
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
                        *bitmap_output=Some(Bitmap{bits: ind.iter().map(|i| bmap[*i]).collect()});
                    } else {
                        *bitmap_output=Some(Bitmap{bits: bmap.iter().map(|i| *i).collect()});
                    }
                }
            }

    }
    )+)
}

binary_operation_impl! {

u64 u32 u16 u8 bool usize String

}

//{ usize u8 u16 u32 u64 u128 isize i8 i16 i32 i64 i128 f32 f64 }

//binary_operation_impl! { (u64,u8) (u64,u16) (u64,u32) (u64,u64) }

pub(crate) fn load_op_dict(dict: &mut OpDictionary) {
    //dict.insert(s, columnadd_onwedcolumnvecu64_vecu64);7
    binary_operation_load! {dict;

        u64 u32 u16 u8 bool usize String

    };
}
