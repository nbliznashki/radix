use crate::bucketcolumn::*;
use crate::columnu8::*;
use crate::hashcolumn::*;

use std::cmp::max;

//Only inner join for now
//No Index support
pub fn hash_join(
    left: &HashColumn,
    right: &HashColumn,
    outer_buckets_count: usize,
) -> (Vec<usize>, Vec<usize>) {
    let len_left = match &left.index {
        None => left.data.len(),
        Some(index) => index.len(),
    };

    let len_right = match &right.index {
        None => left.data.len(),
        Some(index) => index.len(),
    };
    if len_left == 0 || len_right == 0 {
        return (vec![], vec![]);
    } else {
        let buckets_num = len_left.next_power_of_two();
        let num_bits_to_shift = (outer_buckets_count - 1).trailing_ones();
        let mut vhash: Vec<usize> = vec![0; buckets_num];
        let mut vlink: Vec<usize> = vec![0; len_left + 1];

        //TO-DO: Optimize to avoid reservation of memory
        let mut vhash_value: Vec<u64> = vec![0; buckets_num];
        let mut vlink_value: Vec<u64> = vec![0; len_left + 1];

        let predicted_len = max(len_right, len_left);

        match &left.index {
            None => {
                left.data
                    .iter()
                    .enumerate()
                    .filter(|(_, value)| *value & 1 == 0)
                    .map(|(i, value)| (i + 1, value >> (num_bits_to_shift + 1)))
                    .for_each(|(i, value)| {
                        let bucket_id = (value as usize) & (buckets_num - 1);
                        vlink[i] = vhash[bucket_id];
                        vhash[bucket_id] = i;
                    });
            }
            Some(index) => {
                index
                    .iter()
                    .enumerate()
                    .filter(|(_i, index)| index.is_some())
                    .map(|(i, index)| (i, index.unwrap()))
                    .filter(|(_, index)| left.data[*index] & 1 == 0)
                    .map(|(i, index)| {
                        (
                            i + 1,
                            left.data[index] >> (num_bits_to_shift + 1),
                            left.data[index],
                        )
                    })
                    .for_each(|(i, value, hash_value)| {
                        let bucket_id = (value as usize) & (buckets_num - 1);
                        vlink[i] = vhash[bucket_id];
                        vlink_value[i] = vhash_value[bucket_id];
                        vhash[bucket_id] = i;
                        vhash_value[bucket_id] = hash_value;
                    });
            }
        };

        let mut res_left: Vec<usize> = Vec::with_capacity(predicted_len);
        let mut res_right: Vec<usize> = Vec::with_capacity(predicted_len);
        let mut vhash_sum = 1;

        while vhash_sum > 0 {
            match (&right.index, &left.index) {
                (None, None) => {
                    right
                        .data
                        .iter()
                        .enumerate()
                        .filter(|(_, value)| *value & 1 == 0)
                        .map(|(i, value)| (i + 1, value, value >> (num_bits_to_shift + 1)))
                        .for_each(|(i, value_orig, value)| {
                            let bucket_id = (value as usize) & (buckets_num - 1);
                            let reference_index = vhash[bucket_id];
                            if reference_index > 0 && left.data[reference_index - 1] == *value_orig
                            {
                                res_left.push(reference_index - 1);
                                res_right.push(i - 1);
                            }
                        });
                }
                (None, Some(left_index)) => {
                    right
                        .data
                        .iter()
                        .enumerate()
                        .filter(|(_, value)| *value & 1 == 0)
                        .map(|(i, value)| (i + 1, value, value >> (num_bits_to_shift + 1)))
                        .for_each(|(i, value_orig, value)| {
                            let bucket_id = (value as usize) & (buckets_num - 1);
                            let reference_index = vhash[bucket_id];
                            if reference_index > 0 && vhash_value[bucket_id] == *value_orig {
                                res_left.push(reference_index - 1);
                                res_right.push(i - 1);
                            }
                        });
                }
                (Some(index_right), None) => {
                    index_right
                        .iter()
                        .enumerate()
                        .filter(|(_i, index)| index.is_some())
                        .map(|(i, index)| (i, index.unwrap()))
                        .filter(|(_, index)| right.data[*index] & 1 == 0)
                        .map(|(i, index)| {
                            (
                                i + 1,
                                right.data[index],
                                right.data[index] >> (num_bits_to_shift + 1),
                            )
                        })
                        .for_each(|(i, value_orig, value)| {
                            let bucket_id = (value as usize) & (buckets_num - 1);
                            let reference_index = vhash[bucket_id];
                            if reference_index > 0 && left.data[reference_index - 1] == value_orig {
                                res_left.push(reference_index - 1);
                                res_right.push(i - 1);
                            }
                        });
                }
                (Some(index_right), Some(index_left)) => {
                    index_right
                        .iter()
                        .enumerate()
                        .filter(|(_i, index)| index.is_some())
                        .map(|(i, index)| (i, index.unwrap()))
                        .filter(|(_, index)| right.data[*index] & 1 == 0)
                        .map(|(i, index)| {
                            (
                                i + 1,
                                right.data[index],
                                right.data[index] >> (num_bits_to_shift + 1),
                            )
                        })
                        .for_each(|(i, value_orig, value)| {
                            let bucket_id = (value as usize) & (buckets_num - 1);
                            let reference_index = vhash[bucket_id];
                            if reference_index > 0 && vhash_value[bucket_id] == value_orig {
                                res_left.push(reference_index - 1);
                                res_right.push(i - 1);
                            }
                        });
                }
            };

            if let Some(index_left) = &left.index {
                vhash.iter().enumerate().for_each(|(i, index)| {
                    vhash_value[i] = vlink_value[*index];
                });
            };

            vhash.iter_mut().for_each(|i| {
                *i = vlink[*i];
            });
            vhash_sum = vhash.iter().map(|i| (*i != 0) as usize).sum();
        }
        println!("{:?} {:?} ", res_left, res_right);
        (res_left, res_right)
    }
}
