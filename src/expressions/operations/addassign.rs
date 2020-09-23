use crate::bitmap::Bitmap;
use crate::*;

use std::ops::AddAssign;

use paste::paste;

const OP: &str = "+=";

//Load operation for the 4 combinations of output/input equal to Vec or Slice
macro_rules! operation_load {
    ($dict:ident; $(($tl:ty, $tr:ty))+) => ($(
            {
                //Vec Vec
                type V1=Vec<$tl>;
                type V2=Vec<$tr>;
                let signature=sig![OP; V1, V2];
                let op=Operation{
                    f:  paste!{[<addassign_vec $tl:lower _ vec $tr:lower>]},
                    output_type: std::any::TypeId::of::<V1>(),
                    output_typename: std::any::type_name::<V1>().to_string()
                };
            $dict.insert(signature, op);
            }
            {
                //Slice Slice
                type V1=[$tl];
                type V2=[$tr];
                let signature=sig![OP; V1, V2];
                let op=Operation{
                    f:  paste!{[<addassign_slice $tl:lower _ slice $tr:lower>]},
                    output_type: std::any::TypeId::of::<V1>(),
                    output_typename: std::any::type_name::<V1>().to_string()
                };
                $dict.insert(signature, op);
            }
            {
                //Vec Slice
                type V1=Vec<$tl>;
                type V2=[$tr];
                let signature=sig![OP; V1, V2];
                let op=Operation{
                    f:  paste!{[<addassign_vec $tl:lower _ slice $tr:lower>]},
                    output_type: std::any::TypeId::of::<V1>(),
                    output_typename: std::any::type_name::<V1>().to_string()
                };
                $dict.insert(signature, op);
            }
            {
                //Slice Vec
                type V1=[$tl];
                type V2=Vec<$tr>;
                let signature=sig![OP; V1, V2];
                let op=Operation{
                    f:  paste!{[<addassign_slice $tl:lower _ vec $tr:lower>]},
                    output_type: std::any::TypeId::of::<V1>(),
                    output_typename: std::any::type_name::<V1>().to_string()
                };
                $dict.insert(signature, op);
            }
    )+)
}
//Define operation for the 4 combinations of output/input equal to Vec or Slice
//Downcast input/output and then call the generic function addcolumn_c
macro_rules! operation_impl {
    ($(($tl:ty, $tr:ty))+) => ($(
        paste!   {
            fn [<addassign_vec $tl:lower _ vec $tr:lower>](output: &mut ColumnWrapper, input: Vec<InputTypes>)
            {
                //Vec Vec
                type V1=Vec<$tl>;
                type V2=Vec<$tr>;
                let (data_output, index_output, bitmap_output) = output.all_mut::<V1>();
                let (data_input, index_input, bitmap_input) = match &input[0] {
                    InputTypes::Ref(a) => (
                        a.downcast_ref::<V2>(),
                        a.index(),
                        a.bitmap(),
                    ),
                    InputTypes::Owned(a) => (
                        a.downcast_ref::<V2>(),
                        a.index(),
                        a.bitmap(),
                    ),
                };
                addassign_c(data_output, index_output,bitmap_output, data_input, index_input, bitmap_input);
            }
            fn [<addassign_slice $tl:lower _ slice $tr:lower>](output: &mut ColumnWrapper, input: Vec<InputTypes>)
            {
                //Slice Slice
                type V1=[$tl];
                type V2=[$tr];
                let (data_output, index_output, bitmap_output) = output.slice_all_mut::<V1>();
                let (data_input, index_input, bitmap_input) = match &input[0] {
                    InputTypes::Ref(a) => (
                        a.downcast_slice_ref::<V2>(),
                        a.index(),
                        a.bitmap(),
                    ),
                    InputTypes::Owned(a) => (
                        a.downcast_slice_ref::<V2>(),
                        a.index(),
                        a.bitmap(),
                    ),
                };
                addassign_c(data_output, index_output,bitmap_output, data_input, index_input, bitmap_input);
            }
            fn [<addassign_vec $tl:lower _ slice $tr:lower>](output: &mut ColumnWrapper, input: Vec<InputTypes>)
            {
                //Vec Slice
                type V1=Vec<$tl>;
                type V2=[$tr];

                let (data_output, index_output, bitmap_output) = output.all_mut::<V1>();

                let (data_input, index_input, bitmap_input) = match &input[0] {
                    InputTypes::Ref(a) => (
                        a.downcast_slice_ref::<V2>(),
                        a.index(),
                        a.bitmap(),
                    ),
                    InputTypes::Owned(a) => (
                        a.downcast_slice_ref::<V2>(),
                        a.index(),
                        a.bitmap(),
                    ),
                };
                addassign_c(data_output, index_output,bitmap_output, data_input, index_input, bitmap_input);
            }
            fn [<addassign_slice $tl:lower _ vec $tr:lower>](output: &mut ColumnWrapper, input: Vec<InputTypes>)
            {
                //Slice Vec
                type V1=[$tl];
                type V2=Vec<$tr>;
                let (data_output, index_output, bitmap_output) = output.slice_all_mut::<V1>();
                let (data_input, index_input, bitmap_input) = match &input[0] {
                    InputTypes::Ref(a) => (
                        a.downcast_ref::<V2>(),
                        a.index(),
                        a.bitmap(),
                    ),
                    InputTypes::Owned(a) => (
                        a.downcast_ref::<V2>(),
                        a.index(),
                        a.bitmap(),
                    ),
                };
                addassign_c(data_output, index_output,bitmap_output, data_input, index_input, bitmap_input);
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

fn addassign_c<T1, T2>(
    data_output: &mut [T1],
    index_output: &mut ColumnIndex,
    bitmap_output: &mut Option<Bitmap>,
    data_input: &[T2],
    index_input: &ColumnIndex,
    bitmap_input: &Option<Bitmap>,
) where
    T1: AddAssign,
    T1: From<T2>,
    T1: Default,
    T2: Copy,
{
    //naming convention:
    // left->output
    //right[0]-->input
    //if right[0] and right[1]-> input_lhs, input_rhs

    //The output column should have no index
    //Otherwise an element of the output vector can be updated twice if
    //it is indexed twice in the index vector
    assert_eq!(index_output, &None);

    let len_output = data_output.len();
    let len_input = if let Some(ind) = index_input {
        ind.len()
    } else {
        data_input.len()
    };

    //The input and output columns should have the same length
    assert_eq!(len_output, len_input);

    match (&index_input, &bitmap_output, &bitmap_input) {
        (Some(ind), None, None) => data_output
            .iter_mut()
            .zip(ind.iter().map(|i| &data_input[*i]))
            .for_each(|(l, r)| l.add_assign(T1::from(*r))),
        (Some(ind), Some(b_left), None) => data_output
            .iter_mut()
            .zip(ind.iter().map(|i| &data_input[*i]))
            .zip(b_left.bits.iter())
            .for_each(|((l, r), b_l)| {
                l.add_assign(if *b_l != 0 {
                    T1::from(*r)
                } else {
                    Default::default()
                })
            }),
        (Some(ind), None, Some(b_right)) => data_output
            .iter_mut()
            .zip(ind.iter().map(|i| &data_input[*i]))
            .zip(b_right.bits.iter())
            .for_each(|((l, r), b_r)| {
                l.add_assign(if *b_r != 0 {
                    T1::from(*r)
                } else {
                    Default::default()
                })
            }),
        (Some(ind), Some(b_left), Some(b_right)) => data_output
            .iter_mut()
            .zip(ind.iter().map(|i| &data_input[*i]))
            .zip(b_left.bits.iter())
            .zip(b_right.bits.iter())
            .for_each(|(((l, r), b_l), b_r)| {
                l.add_assign(if (*b_l != 0) & (*b_r != 0) {
                    T1::from(*r)
                } else {
                    Default::default()
                })
            }),

        (None, None, None) => data_output
            .iter_mut()
            .zip(data_input.iter())
            .for_each(|(l, r)| l.add_assign(T1::from(*r))),
        (None, Some(b_left), None) => data_output
            .iter_mut()
            .zip(data_input.iter())
            .zip(b_left.bits.iter())
            .for_each(|((l, r), b_l)| {
                l.add_assign(if *b_l != 0 {
                    T1::from(*r)
                } else {
                    Default::default()
                })
            }),
        (None, None, Some(b_right)) => data_output
            .iter_mut()
            .zip(data_input.iter())
            .zip(b_right.bits.iter())
            .for_each(|((l, r), b_r)| {
                l.add_assign(if *b_r != 0 {
                    T1::from(*r)
                } else {
                    Default::default()
                })
            }),
        (None, Some(b_left), Some(b_right)) => data_output
            .iter_mut()
            .zip(data_input.iter())
            .zip(b_left.bits.iter())
            .zip(b_right.bits.iter())
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
                bits: ind.iter().map(|i| b_right.bits[*i]).collect(),
            }),
        };
    } else {
        let mut b_left = bitmap_output.take().unwrap();
        match (index_input, bitmap_input) {
            (_, None) => {}
            (None, Some(b_right)) => b_left
                .bits
                .iter_mut()
                .zip(b_right.bits.iter())
                .for_each(|(b_l, b_r)| *b_l &= b_r),
            (Some(ind), Some(b_right)) => b_left
                .bits
                .iter_mut()
                .zip(ind.iter())
                .for_each(|(b_l, i)| *b_l &= b_right.bits[*i]),
        };
        *bitmap_output = Some(b_left);
    }
}
