use crate::{hashcolumn::*, ColumnIndex, InitDictionary, OpDictionary};
use crate::{ColumnWrapper, Expression};

use std::cmp::max;

//Only inner join for now
pub fn build_ind(
    left: &HashColumn,
    right: &HashColumn,
    outer_buckets_count: usize,
) -> (Vec<usize>, Vec<usize>) {
    let len_left = left.len();

    let len_right = right.len();
    if len_left == 0 || len_right == 0 {
        (vec![], vec![])
    } else {
        let buckets_num = len_left.next_power_of_two();
        let num_bits_to_shift = (outer_buckets_count - 1).trailing_ones();
        let mut vhash: Vec<usize> = vec![0; buckets_num];
        let mut vlink: Vec<usize> = vec![0; len_left + 1];

        let predicted_len = max(len_right, len_left);

        match left.bitmap() {
            Some(bmap) => {
                left.iter()
                    .zip(bmap.bits.iter())
                    .enumerate()
                    .filter(|(_, (_value, b))| **b != 0)
                    .map(|(i, (value, _b))| (i + 1, value >> (num_bits_to_shift)))
                    .for_each(|(i, value)| {
                        let bucket_id = (value as usize) & (buckets_num - 1);
                        vlink[i] = vhash[bucket_id];
                        vhash[bucket_id] = i;
                    });
            }
            None => {
                left.iter()
                    .enumerate()
                    .map(|(i, value)| (i + 1, value >> (num_bits_to_shift)))
                    .for_each(|(i, value)| {
                        let bucket_id = (value as usize) & (buckets_num - 1);
                        vlink[i] = vhash[bucket_id];
                        vhash[bucket_id] = i;
                    });
            }
        }

        let mut res_left: Vec<usize> = Vec::with_capacity(predicted_len);
        let mut res_right: Vec<usize> = Vec::with_capacity(predicted_len);
        let mut vhash_sum: usize = 1;

        while vhash_sum > 0 {
            match right.bitmap() {
                Some(bmap) => {
                    right
                        .iter()
                        .zip(bmap.bits.iter())
                        .enumerate()
                        .filter(|(_, (_value, b))| **b != 0)
                        .map(|(i, (value, _b))| (i + 1, value, value >> (num_bits_to_shift)))
                        .for_each(|(i, value_orig, value)| {
                            let bucket_id = (value as usize) & (buckets_num - 1);
                            let reference_index = vhash[bucket_id];
                            if reference_index > 0 && left[reference_index - 1] == *value_orig {
                                res_left.push(reference_index - 1);
                                res_right.push(i - 1);
                            }
                        });
                }
                None => {
                    right
                        .iter()
                        .enumerate()
                        .map(|(i, value)| (i + 1, value, value >> (num_bits_to_shift)))
                        .for_each(|(i, value_orig, value)| {
                            let bucket_id = (value as usize) & (buckets_num - 1);
                            let reference_index = vhash[bucket_id];
                            if reference_index > 0 && left[reference_index - 1] == *value_orig {
                                res_left.push(reference_index - 1);
                                res_right.push(i - 1);
                            }
                        });
                }
            }

            vhash.iter_mut().for_each(|i| {
                *i = vlink[*i];
            });
            vhash_sum = vhash.iter().map(|i| (*i != 0) as usize).sum();
        }
        println!("{:?} {:?} ", res_left, res_right);
        (res_left, res_right)
    }
}

pub fn applyoneif<'a>(
    left_cols: &'a mut Vec<&'a mut ColumnWrapper>,
    right_cols: &'a mut Vec<&'a mut ColumnWrapper>,
    left_ind: &mut Vec<usize>,
    right_ind: &mut Vec<usize>,
    expr: &Expression,
    dict: &OpDictionary,
    init_dict: &InitDictionary,
) {
    let left_ind_backup: Vec<_> = left_cols.iter_mut().map(|c| c.re_index(left_ind)).collect();
    let right_ind_backup: Vec<_> = right_cols
        .iter_mut()
        .map(|c| c.re_index(right_ind))
        .collect();

    let mut ref_columns: Vec<&ColumnWrapper> = left_cols.iter().map(|c| &(**c)).collect();
    ref_columns.extend(right_cols.iter().map(|c| &(**c)));

    let mut owned_columns = expr.compile(&dict, &init_dict).1;
    expr.eval(
        &mut owned_columns.iter_mut().collect(),
        &ref_columns,
        &vec![],
        &dict,
    );

    assert!(!owned_columns.is_empty());
    let result = owned_columns.pop().unwrap();

    let (b, bitmap, _) = result.all_unwrap::<Vec<bool>>();

    assert_eq!(b.len(), left_ind.len());
    assert_eq!(b.len(), right_ind.len());

    match bitmap {
        Some(bmap) => {
            *left_ind = left_ind
                .iter()
                .zip(b.iter())
                .zip(bmap.iter())
                .filter(|((_, b), bit)| **b && **bit != 0)
                .map(|((i, _b), _bit)| *i)
                .collect();
            *right_ind = right_ind
                .iter()
                .zip(b.iter())
                .zip(bmap.iter())
                .filter(|((_, b), bit)| **b && **bit != 0)
                .map(|((i, _b), _bit)| *i)
                .collect();
        }
        None => {
            *left_ind = left_ind
                .iter()
                .zip(b.iter())
                .filter(|(_, b)| **b)
                .map(|(i, _b)| *i)
                .collect();
            *right_ind = right_ind
                .iter()
                .zip(b.iter())
                .filter(|(_, b)| **b)
                .map(|(i, _b)| *i)
                .collect();
        }
    }

    left_cols
        .iter_mut()
        .zip(left_ind_backup.into_iter())
        .for_each(|(c, ind)| *c.index_mut() = ind);

    right_cols
        .iter_mut()
        .zip(right_ind_backup.into_iter())
        .for_each(|(c, ind)| *c.index_mut() = ind);
}
