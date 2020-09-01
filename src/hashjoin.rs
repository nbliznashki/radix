use crate::hashcolumn::*;

use std::cmp::max;

//Only inner join for now
pub fn hash_join(
    left: &HashColumn,
    right: &HashColumn,
    outer_buckets_count: usize,
) -> (Vec<usize>, Vec<usize>) {
    let len_left = left.data.len();

    let len_right = right.data.len();
    if len_left == 0 || len_right == 0 {
        (vec![], vec![])
    } else {
        let buckets_num = len_left.next_power_of_two();
        let num_bits_to_shift = (outer_buckets_count - 1).trailing_ones();
        let mut vhash: Vec<usize> = vec![0; buckets_num];
        let mut vlink: Vec<usize> = vec![0; len_left + 1];

        let predicted_len = max(len_right, len_left);

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

        let mut res_left: Vec<usize> = Vec::with_capacity(predicted_len);
        let mut res_right: Vec<usize> = Vec::with_capacity(predicted_len);
        let mut vhash_sum = 1;

        while vhash_sum > 0 {
            right
                .data
                .iter()
                .enumerate()
                .filter(|(_, value)| *value & 1 == 0)
                .map(|(i, value)| (i + 1, value, value >> (num_bits_to_shift + 1)))
                .for_each(|(i, value_orig, value)| {
                    let bucket_id = (value as usize) & (buckets_num - 1);
                    let reference_index = vhash[bucket_id];
                    if reference_index > 0 && left.data[reference_index - 1] == *value_orig {
                        res_left.push(reference_index - 1);
                        res_right.push(i - 1);
                    }
                });

            vhash.iter_mut().for_each(|i| {
                *i = vlink[*i];
            });
            vhash_sum = vhash.iter().map(|i| (*i != 0) as usize).sum();
        }
        println!("{:?} {:?} ", res_left, res_right);
        (res_left, res_right)
    }
}
