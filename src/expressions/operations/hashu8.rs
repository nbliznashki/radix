use crate::bitmap::Bitmap;
use crate::*;

use std::hash::{BuildHasher, Hash, Hasher};
use std::ops::AddAssign;

#[allow(dead_code)]
const OP: &str = "hash+=";

pub(crate) fn load_op_dict(dict: &mut OpDictionary) {
    let signature = sig![OP; ColumnU8];
    let op = Operation {
        f: hashadd_vecu64_columnu8,
        output_type: std::any::TypeId::of::<Vec<u64>>(),
        output_typename: std::any::type_name::<Vec<u64>>().to_string(),
    };
    dict.insert(signature, op);
}

fn hashadd_vecu64_columnu8(output: &mut ColumnWrapper, input: Vec<InputTypes>) {
    let rs = ahash::RandomState::with_seeds(1234, 5678);

    type T1 = u64;
    type T2 = ColumnU8;

    //naming convention:
    // left->output
    //right[0]-->input
    //if right[0] and right[1]-> input_lhs, input_rhs

    let (data_output, index_output, bitmap_output) = output.all_mut::<Vec<T1>>();

    let (datau8_input, index_input, bitmap_input) = match &input[0] {
        InputTypes::Ref(a) => (a.downcast_ref::<T2>(), a.index(), a.bitmap()),
        InputTypes::Owned(a) => (a.downcast_ref::<T2>(), a.index(), a.bitmap()),
    };

    //The output column should have no index
    assert_eq!(index_output, &None);

    let len_output = data_output.len();
    let len_input = if let Some(ind) = index_input {
        ind.len()
    } else {
        datau8_input.len.len()
    };

    assert_eq!(len_output, len_input);

    let slice_len = &datau8_input.len;
    let slice_start_pos = &datau8_input.start_pos;
    let data = &datau8_input.data;

    match (&index_input, &bitmap_input) {
        (Some(ind), None) => data_output
            .iter_mut()
            .zip(
                ind.iter()
                    .map(|i| &data[slice_start_pos[*i]..slice_start_pos[*i] + slice_len[*i]]),
            )
            .for_each(|(l, sliceu8)| {
                l.add_assign({
                    let mut h = rs.build_hasher();
                    sliceu8.hash(&mut h);
                    h.finish()
                })
            }),
        (Some(ind), Some(b_right)) => data_output
            .iter_mut()
            .zip(
                ind.iter()
                    .map(|i| &data[slice_start_pos[*i]..slice_start_pos[*i] + slice_len[*i]]),
            )
            .zip(b_right.iter())
            .for_each(|((l, sliceu8), b_r)| {
                l.add_assign(if *b_r != 0 {
                    {
                        let mut h = rs.build_hasher();
                        sliceu8.hash(&mut h);
                        h.finish()
                    }
                } else {
                    u64::MAX
                })
            }),

        (None, None) => data_output
            .iter_mut()
            .zip(slice_start_pos.iter())
            .zip(slice_len.iter())
            .for_each(|((l, slice_start), len)| {
                l.add_assign({
                    let mut h = rs.build_hasher();
                    let slice = &data[*slice_start..*slice_start + *len];
                    slice.hash(&mut h);
                    h.finish()
                })
            }),

        (None, Some(b_right)) => data_output
            .iter_mut()
            .zip(slice_start_pos.iter())
            .zip(slice_len.iter())
            .zip(b_right.iter())
            .for_each(|(((l, slice_start), len), b_r)| {
                l.add_assign(if *b_r != 0 {
                    {
                        let mut h = rs.build_hasher();
                        let slice = &data[*slice_start..*slice_start + *len];
                        slice.hash(&mut h);
                        h.finish()
                    }
                } else {
                    u64::MAX
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
}
