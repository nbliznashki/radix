use crate::bitmap::Bitmap;
use crate::*;
use crate::{Column, ColumnMut, OwnedColumn};

use core::any::{Any, TypeId};

use std::hash::{BuildHasher, Hash, Hasher};
use std::ops::AddAssign;

use rayon::prelude::*;

#[allow(dead_code)]
const OP: &str = "hash+=";

pub(crate) fn load_op_dict(dict: &mut OpDictionary) {
    let signature = sig![OP;OwnedColumn<Vec<u64>>; OwnedColumn<ColumnU8>];
    dict.insert(signature, hashadd_ownedcolumnvecu64_ownedcolumncolumnu8);
}

fn hashadd_ownedcolumnvecu64_ownedcolumncolumnu8(left: &mut dyn Any, right: Vec<InputTypes>) {
    let rs = ahash::RandomState::with_seeds(1234, 5678);

    let down_left = left.downcast_mut::<OwnedColumn<Vec<u64>>>().unwrap();
    let down_right = match &right[0] {
        InputTypes::Ref(a) => a.downcast_ref::<OwnedColumn<ColumnU8>>().unwrap(),
        InputTypes::Owned(a) => a.downcast_ref::<OwnedColumn<ColumnU8>>().unwrap(),
    };

    let (data_left, index_left, bitmap_left) = down_left.all_mut();

    //The output column should have no index
    assert_eq!(index_left, &None);

    let bitmap_right = &down_right.bitmap().as_ref();
    let index_right = &down_right.index().as_ref();
    let datau8_right = &down_right.col();

    let slice_len = &datau8_right.len;
    let slice_start_pos = &datau8_right.start_pos;
    let data = &datau8_right.data;

    let left_len = data_left.len();
    let right_len = if let Some(ind) = index_right {
        ind.len()
    } else {
        datau8_right.len.len()
    };

    assert_eq!(left_len, right_len);

    match (&index_right, &bitmap_right) {
        (Some(ind), None) => data_left
            .par_iter_mut()
            .zip_eq(
                ind.par_iter()
                    .map(|i| &data[slice_start_pos[*i]..slice_start_pos[*i] + slice_len[*i]]),
            )
            .for_each(|(l, sliceu8)| {
                l.add_assign({
                    let mut h = rs.build_hasher();
                    sliceu8.hash(&mut h);
                    h.finish()
                })
            }),
        (Some(ind), Some(b_right)) => data_left
            .par_iter_mut()
            .zip_eq(
                ind.par_iter()
                    .map(|i| &data[slice_start_pos[*i]..slice_start_pos[*i] + slice_len[*i]]),
            )
            .zip_eq(b_right.bits.par_iter())
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

        (None, None) => data_left
            .par_iter_mut()
            .zip_eq(slice_start_pos.par_iter())
            .zip_eq(slice_len.par_iter())
            .for_each(|((l, slice_start), len)| {
                l.add_assign({
                    let mut h = rs.build_hasher();
                    let slice = &data[*slice_start..*slice_start + *len];
                    slice.hash(&mut h);
                    h.finish()
                })
            }),

        (None, Some(b_right)) => data_left
            .par_iter_mut()
            .zip_eq(slice_start_pos.par_iter())
            .zip_eq(slice_len.par_iter())
            .zip_eq(b_right.bits.par_iter())
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
