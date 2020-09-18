use crate::bitmap::Bitmap;
use paste::paste;

use std::ops::AddAssign;

use rayon::prelude::*;

use crate::*;

#[allow(dead_code)]
const OP: &str = "+=";

macro_rules! operation_load {
    ($dict:ident; $(($tl:ty, $tr:ty))+) => ($(
            let signature=sig![OP; Vec<$tl>, Vec<$tr>];
            //println!("{:?}", signature);
            let op=Operation{
                f: paste!{[<addassign_vec $tl _ vec $tr>]},
                output_type: std::any::TypeId::of::<Vec<$tl>>(),
                output_typename: std::any::type_name::<Vec<$tl>>().to_string()
            };
            $dict.insert(signature, op);
    )+)
}

macro_rules! operation_impl {
    ($(($tl:ty, $tr:ty))+) => ($(
        paste! {
            #[allow(dead_code)]
            fn [<addassign_vec $tl _ vec $tr>](output: &mut ColumnWrapper, input: Vec<InputTypes>) {

                type T1=$tl;
                type T2=$tr;

                //naming convention:
                // left->output
                //right[0]-->input
                //if right[0] and right[1]-> input_lhs, input_rhs

                let (data_output, index_output, bitmap_output) = output.all_mut::<Vec<T1>>();

                let (data_input, index_input, bitmap_input) = match &input[0] {
                    InputTypes::Ref(a)=>(a.downcast_ref::<Vec<T2>>(), a.index().as_ref(), a.bitmap().as_ref()),
                    InputTypes::Owned(a)=>(a.downcast_ref::<Vec<T2>>(), a.index().as_ref(), a.bitmap().as_ref())
                };



                //The output column should have no index
                assert_eq!(index_output, &None);


                let len_output = data_output.len();
                let len_input = if let Some(ind) = index_input {
                    ind.len()
                } else {
                    data_input.len()
                };

                assert_eq!(len_output, len_input);

                match (&index_input, &bitmap_output, &bitmap_input) {
                    (Some(ind), None, None) => data_output
                        .par_iter_mut()
                        .zip_eq(ind.par_iter().map(|i| &data_input[*i]))
                        .for_each(|(l, r)| l.add_assign( T1::from(*r))),
                    (Some(ind), Some(b_left), None) => data_output
                        .par_iter_mut()
                        .zip_eq(ind.par_iter().map(|i| &data_input[*i]))
                        .zip_eq(b_left.bits.par_iter())
                        .for_each(|((l, r), b_l)| {
                            l.add_assign( if *b_l != 0 {
                                T1::from(*r)
                            } else {
                                Default::default()
                            })
                        }),
                    (Some(ind), None, Some(b_right)) => data_output
                        .par_iter_mut()
                        .zip_eq(ind.par_iter().map(|i| &data_input[*i]))
                        .zip_eq(b_right.bits.par_iter())
                        .for_each(|((l, r), b_r)| {
                            l.add_assign(if *b_r != 0 {
                                T1::from(*r)
                            } else {
                                Default::default()
                            })
                        }),
                    (Some(ind), Some(b_left), Some(b_right)) => data_output
                        .par_iter_mut()
                        .zip_eq(ind.par_iter().map(|i| &data_input[*i]))
                        .zip_eq(b_left.bits.par_iter())
                        .zip_eq(b_right.bits.par_iter())
                        .for_each(|(((l, r), b_l), b_r)| {
                            l.add_assign(if (*b_l != 0) & (*b_r != 0) {
                                T1::from(*r)
                            } else {
                                Default::default()
                            })
                        }),

                    (None, None, None) => data_output
                        .par_iter_mut()
                        .zip_eq(data_input.par_iter())
                        .for_each(|(l, r)| l.add_assign(T1::from(*r))),
                    (None, Some(b_left), None) => data_output
                        .par_iter_mut()
                        .zip_eq(data_input.par_iter())
                        .zip_eq(b_left.bits.par_iter())
                        .for_each(|((l, r), b_l)| {
                            l.add_assign(if *b_l != 0 {
                                T1::from(*r)
                            } else {
                                Default::default()
                            })
                        }),
                    (None, None, Some(b_right)) => data_output
                        .par_iter_mut()
                        .zip_eq(data_input.par_iter())
                        .zip_eq(b_right.bits.par_iter())
                        .for_each(|((l, r), b_r)| {
                            l.add_assign(if *b_r != 0 {
                                T1::from(*r)
                            } else {
                                Default::default()
                            })
                        }),
                    (None, Some(b_left), Some(b_right)) => data_output
                        .par_iter_mut()
                        .zip_eq(data_input.par_iter())
                        .zip_eq(b_left.bits.par_iter())
                        .zip_eq(b_right.bits.par_iter())
                        .for_each(|(((l, r), b_l), b_r)| {
                            l.add_assign(if (*b_l != 0) & (*b_r != 0) {
                                T1::from(*r)
                            } else {
                                Default::default()
                            })
                        }),
                }
                if bitmap_output.is_none() {
                    *bitmap_output = match (index_input, bitmap_input) {
                        (_, None) => None,
                        (None, Some(b_right)) => Some((*b_right).clone()),
                        (Some(ind), Some(b_right)) => Some(Bitmap {
                            bits: ind.par_iter().map(|i| b_right.bits[*i]).collect(),
                        }),
                    };
                } else {
                    let mut b_left = bitmap_output.take().unwrap();
                    match (index_input, bitmap_input) {
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
                    *bitmap_output = Some(b_left);
                }
            }

    }
    )+)
}

operation_impl! {
    (u64, u64) (u64, u32) (u64, u16) (u64, u8) (u64, bool) (u32,u32)
}

pub(crate) fn load_op_dict(dict: &mut OpDictionary) {
    operation_load! {dict;
            (u64, u64) (u64, u32) (u64, u16) (u64, u8) (u64, bool) (u32,u32)
    };
}
