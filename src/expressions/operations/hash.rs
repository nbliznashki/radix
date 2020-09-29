use crate::bitmap::Bitmap;
use paste::paste;

use std::hash::{BuildHasher, Hash, Hasher};
use std::ops::AddAssign;

use crate::column::hashcolumn::HashData;

use crate::*;

#[allow(dead_code)]
const OP: &str = "hash+=";

macro_rules! binary_operation_load {
    ($dict:ident; $($tr:ty)+) => ($(

            let signature=sig![OP; Vec<$tr>];
            let op=Operation{
                f: paste!{[<hashadd_vecu64_vec_ $tr:lower>]},
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
                fn [<hashadd_vecu64_vec_ $tr:lower>](output: &mut ColumnWrapper, input: Vec<InputTypes>)->Result<(),ErrorDesc>{

                let rs=ahash::RandomState::with_seeds(1234,5678);

                type T1=u64;
                type T2=$tr;

                //naming convention:
                // left->output
                //right[0]-->input
                //if right[0] and right[1]-> input_lhs, input_rhs

                let (data_output, index_output, bitmap_output) = output.all_mut::<HashData>()?;

                let (data_input, index_input, bitmap_input) = match &input[0] {
                    InputTypes::Ref(a)=>(a.downcast_ref::<Vec<T2>>()?, a.index(), a.bitmap()),
                    InputTypes::Owned(a)=>(a.downcast_ref::<Vec<T2>>()?, a.index(), a.bitmap())
                };



                //The output column should have no index
                if let Some(_) = index_output {
                    Err(format!(
                        "The output column for operation {} can't have an index",
                        OP
                    ))?
                };


                let len_output = data_output.len();
                let len_input = if let Some(ind) = index_input {
                    ind.len()
                } else {
                    data_input.len()
                };

                //The input and output columns should have the same length
                if len_output != len_input {
                    Err(format!(
                        "The input and output columns should have the same length, but they are {} and {} respectively",
                        len_input, len_output
                    ))?
                };

                match (&index_input, &bitmap_input) {
                    (Some(ind), None) => data_output
                        .iter_mut()
                        .zip(ind.iter().map(|i| &data_input[*i]))
                        .for_each(|(l, r)| l.add_assign( {let mut h=rs.build_hasher(); r.hash(&mut h); h.finish()})),
                    (Some(ind), Some(b_right)) => data_output
                        .iter_mut()
                        .zip(ind.iter().map(|i| &data_input[*i]))
                        .zip(b_right.iter())
                        .for_each(|((l, r), b_r)| {
                            l.add_assign(if *b_r != 0 {
                                {let mut h=rs.build_hasher(); r.hash(&mut h); h.finish()}
                            } else {
                                T1::MAX
                            })
                        }),

                    (None, None) => data_output
                        .iter_mut()
                        .zip(data_input.iter())
                        .for_each(|(l, r)| l.add_assign({let mut h=rs.build_hasher(); r.hash(&mut h); h.finish()})),

                    (None, Some(b_right)) => data_output
                        .iter_mut()
                        .zip(data_input.iter())
                        .zip(b_right.iter())
                        .for_each(|((l, r), b_r)| {
                            l.add_assign(if *b_r != 0 {
                                {let mut h=rs.build_hasher(); r.hash(&mut h); h.finish()}
                            } else {
                                T1::MAX
                            })
                        }),
                }
                if bitmap_output.is_none() {
                    *bitmap_output = match (index_input, bitmap_input) {
                        (_, None) => None,
                        (None, Some(b_right)) => Some(Bitmap::from(b_right.to_vec())),
                        (Some(ind), Some(b_right)) => Some(Bitmap {
                            bits: ind.iter().map(|i| b_right[*i]).collect(),
                        }),
                    };
                } else {
                    let mut b_left = bitmap_output.take().unwrap();
                    match (index_input, bitmap_input) {
                        (_, None) => {}
                        (None, Some(b_right)) => b_left
                            .bits
                            .iter_mut()
                            .zip(b_right.iter())
                            .for_each(|(b_l, b_r)| *b_l &= b_r),
                        (Some(ind), Some(b_right)) => b_left
                            .bits
                            .iter_mut()
                            .zip(ind.iter())
                            .for_each(|(b_l, i)| *b_l &= b_right[*i]),
                    };
                    *bitmap_output = Some(b_left);
                }
                Ok(())
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
