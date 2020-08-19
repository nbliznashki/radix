use rayon::prelude::*;
use std::ops::Deref;

use crate::hashcolumn::*;

pub struct BucketColumn {
    //TO-DO: Implement Drop in parallel
    //TO-DO: Make data Vec<Vec<usize>> and move worker count
    pub(crate) data: Vec<usize>,
    pub(crate) buckets_count: usize,
}

impl Deref for BucketColumn {
    type Target = Vec<usize>;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl BucketColumn {
    pub fn from_hash(hash: &HashColumn, bucket_bits: u32) -> BucketColumn {
        let buckets_count = 2usize.pow(bucket_bits);
        assert!((buckets_count as u64) < (u64::MAX) / 2, "Too many buckets");
        BucketColumn {
            data: hash
                .data
                .par_iter()
                .map(|h| (h % (2 * buckets_count as u64)) as usize)
                .collect(),
            buckets_count,
        }
    }
}

pub struct BucketColumnPartitioned {
    //TO-DO: Implement Drop in parallel
    //TO-DO: Make data Vec<Vec<usize>> and move worker count
    pub(crate) data: Vec<Vec<usize>>, //buckets_count
    pub(crate) buckets_count: usize,
}

impl Deref for BucketColumnPartitioned {
    type Target = Vec<Vec<usize>>;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl BucketColumnPartitioned {
    pub fn from_hash(hash: &HashColumnPartitioned, bucket_bits: u32) -> BucketColumnPartitioned {
        let buckets_count = 2usize.pow(bucket_bits);
        assert!((buckets_count as u64) < (u64::MAX) / 2, "Too many buckets");
        BucketColumnPartitioned {
            data: hash
                .par_iter()
                .map(|hash_chunk| {
                    hash_chunk
                        .iter()
                        .map(|h| (h % (2 * buckets_count as u64)) as usize)
                        .collect()
                })
                .collect(),
            buckets_count,
        }
    }
}

pub struct BucketsSizeMap {
    //TO-DO: Implement Drop in parallel
    pub(crate) data: Vec<Vec<usize>>, //workers_count x bucket_count
    pub(crate) workers_count: usize,
    pub(crate) buckets_count: usize,
    pub(crate) chunk_len: usize,
    pub(crate) bucket_column: Vec<usize>,
    pub(crate) offsets: Vec<Vec<usize>>,
    pub(crate) bucket_sizes: Vec<usize>,
}

impl Deref for BucketsSizeMap {
    type Target = Vec<Vec<usize>>;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl BucketsSizeMap {
    pub fn from_bucket_column(bc: BucketColumn, workers_count: usize) -> Self {
        let chunk_len = (bc.len() + workers_count - 1) / workers_count;
        let bmap: Vec<Vec<usize>> = bc
            .par_chunks(chunk_len)
            .map(|chunk| {
                let mut local_map: Vec<usize> = vec![0; bc.buckets_count];
                chunk
                    .iter()
                    .filter(|bucket_id| *bucket_id & 1 == 0)
                    .map(|bucket_id| bucket_id >> 1)
                    .for_each(|bucket_id| local_map[bucket_id] += 1);
                local_map
            })
            .collect();

        let mut offsets: Vec<Vec<usize>> = Vec::with_capacity(workers_count + 1);
        let mut v_current = vec![0; bc.buckets_count];
        offsets.push(v_current.par_iter().map(|i| *i).collect());
        bmap.iter().for_each(|bucket_counts| {
            let mut v: Vec<usize> = Vec::with_capacity(bc.buckets_count);
            v_current
                .par_iter_mut()
                .zip(bucket_counts.par_iter())
                .for_each(|(bucket_offset, current_val)| {
                    *bucket_offset += current_val;
                });
            v.par_extend(v_current.par_iter());
            offsets.push(v);
        });
        let bucket_sizes = offsets.pop().unwrap();
        BucketsSizeMap {
            data: bmap,
            workers_count,
            buckets_count: bc.buckets_count,
            chunk_len,
            bucket_column: bc.data,
            offsets,
            bucket_sizes,
        }
    }
}

//New code

pub struct BucketsSizeMapPartitioned {
    //TO-DO: Implement Drop in parallel
    pub(crate) data: Vec<Vec<usize>>, //workers_count x bucket_count
    pub(crate) bucket_column: Vec<Vec<usize>>,
    pub(crate) buckets_count: usize,
    pub(crate) offsets: Vec<Vec<usize>>,
    pub(crate) bucket_sizes: Vec<usize>,
}

impl Deref for BucketsSizeMapPartitioned {
    type Target = Vec<Vec<usize>>;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl BucketsSizeMapPartitioned {
    pub fn from_bucket_column(bc: BucketColumnPartitioned) -> Self {
        //let chunk_len = (bc.len() + workers_count - 1) / workers_count;
        let workers_count = bc.len();
        let bmap: Vec<Vec<usize>> = bc
            .par_iter()
            .map(|chunk| {
                let mut local_map: Vec<usize> = vec![0; bc.buckets_count];
                chunk
                    .iter()
                    .filter(|bucket_id| *bucket_id & 1 == 0)
                    .map(|bucket_id| bucket_id >> 1)
                    .for_each(|bucket_id| local_map[bucket_id] += 1);
                local_map
            })
            .collect();

        let mut offsets: Vec<Vec<usize>> = Vec::with_capacity(workers_count + 1);
        let mut v_current = vec![0; bc.buckets_count];
        offsets.push(v_current.par_iter().map(|i| *i).collect());
        bmap.iter().for_each(|bucket_counts| {
            let mut v: Vec<usize> = Vec::with_capacity(bc.buckets_count);
            v_current
                .par_iter_mut()
                .zip(bucket_counts.par_iter())
                .for_each(|(bucket_offset, current_val)| {
                    *bucket_offset += current_val;
                });
            v.par_extend(v_current.par_iter());
            offsets.push(v);
        });
        let bucket_sizes = offsets.pop().unwrap();
        Self {
            data: bmap,
            bucket_column: bc.data,
            buckets_count: bc.buckets_count,
            offsets,
            bucket_sizes,
        }
    }
}
