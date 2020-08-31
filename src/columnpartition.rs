use rayon::prelude::*;
use std::cell::UnsafeCell;
use std::hash::{BuildHasher, Hash, Hasher};
use std::mem::{self, MaybeUninit};
use std::ops::Deref;

use crate::bucketcolumn::*;
use crate::columnu8::*;
use crate::hashcolumn::*;

pub trait ColumnPartition<V, T> {
    fn get_col(&self) -> &V;
    fn hash_column<S>(&self, s: &S, index: ColumnIndex) -> HashColumn
    where
        S: BuildHasher + Sync,
        V: Deref<Target = [T]>,
        V: Sync,
        T: Send + Sync,
        T: Hash,
    {
        let data = if let Some(index) = &index {
            let data = &self.get_col();
            index
                .par_iter()
                .map(|i| match i {
                    Some(i) => {
                        let mut h = s.build_hasher();
                        data[*i].hash(&mut h);
                        h.finish() << 1
                    }
                    None => 1,
                })
                .collect()
        } else {
            (&self.get_col())
                .par_iter()
                .map(|t| {
                    let mut h = s.build_hasher();
                    t.hash(&mut h);
                    h.finish() << 1
                })
                .collect()
        };
        HashColumn { data, index }
    }
    fn hash_column_append<S>(&self, s: &S, h: &mut HashColumn)
    where
        S: BuildHasher + Sync,
        V: Deref<Target = [T]>,
        V: Sync,
        T: Send + Sync,
        T: Hash,
    {
        if let Some(index) = &h.index {
            let data = &self.get_col();
            h.data
                .par_iter_mut()
                .zip_eq(index.par_iter())
                .filter(|(current_hash, _)| **current_hash & 1 == 0)
                .for_each(|(current_hash, i)| match i {
                    None => *current_hash = 1,
                    Some(i) => {
                        let mut h = s.build_hasher();
                        data[*i].hash(&mut h);
                        *current_hash = current_hash.wrapping_add(h.finish() << 1);
                    }
                });
        } else {
            h.data
                .par_iter_mut()
                .zip_eq((&self.get_col()).par_iter())
                .filter(|(current_hash, _)| **current_hash & 1 == 0)
                .for_each(|(current_hash, t)| {
                    let mut h = s.build_hasher();
                    t.hash(&mut h);
                    *current_hash = current_hash.wrapping_add(h.finish() << 1);
                });
        }
    }

    fn partition_column(&self, bmap: &BucketsSizeMap) -> PartitionedColumn<T>
    where
        V: Deref<Target = [T]>,
        V: Sync,
        T: Send + Sync,
        T: Clone,
    {
        let column_data = self.get_col();
        let index = &bmap.hash.index;

        let column_len = match index {
            Some(v) => v.len(),
            None => column_data.len(),
        };
        //check that the Bucket Size Map and the Column are compatible (needed to allow unsafe code below)
        assert_eq!(column_len, bmap.bucket_column.len());

        let mut output: Vec<Vec<MaybeUninit<T>>> = bmap
            .bucket_sizes
            .par_iter()
            .map(|i| Vec::with_capacity(*i))
            .collect();

        //SAFETY: OK to do, because enough capacity has been reserved, and the content of the vector is assumed to be uninitialized thanks to
        //definition using MaybeUninit
        output
            .par_iter_mut()
            .zip_eq(bmap.bucket_sizes.par_iter())
            .for_each(|(v, len)| unsafe { v.set_len(*len) });

        struct UnsafeOutput<T> {
            data: UnsafeCell<Vec<Vec<MaybeUninit<T>>>>,
        }
        //SAFETY: check below
        unsafe impl<T: Sync> Sync for UnsafeOutput<T> {}

        let unsafe_output = UnsafeOutput {
            data: UnsafeCell::new(output),
        };

        let chunk_len = bmap.chunk_len;

        let data = &self.get_col();

        //PUSH the data which is hopefullz local for the current CPU (depends on index)

        if let Some(index) = index {
            index
                .par_chunks(chunk_len)
                .zip_eq(bmap.bucket_column.par_chunks(chunk_len))
                .zip_eq(bmap.offsets.par_iter())
                .for_each(|((index_chunk, bucket_chunk), initial_offset)| {
                    let mut offset = initial_offset.clone();

                    let unsafe_output = unsafe_output.data.get();
                    bucket_chunk
                        .iter()
                        .zip(index_chunk.iter())
                        .filter(|(bucket_id, _)| *bucket_id & 1 == 0)
                        .map(|(bucket_id, index)| (bucket_id >> 1, index))
                        .for_each(|(bucket_id, index)| {
                            //SAFETY - ok to do, due to the way the offset is calculated - no two threads should try to write at the same
                            //offset in the vector, and all fields of the vector should be populated
                            unsafe {
                                (*unsafe_output)[bucket_id][offset[bucket_id]]
                                    .as_mut_ptr()
                                    .write(data[(*index).unwrap()].clone());
                            };
                            offset[bucket_id] += 1;
                        })
                });
        } else {
            data.par_chunks(chunk_len)
                .zip_eq(bmap.bucket_column.par_chunks(chunk_len))
                .zip_eq(bmap.offsets.par_iter())
                .for_each(|((data_chunk, bucket_chunk), initial_offset)| {
                    let mut offset = initial_offset.clone();

                    let unsafe_output = unsafe_output.data.get();
                    bucket_chunk
                        .iter()
                        .zip(data_chunk.iter())
                        .filter(|(bucket_id, _)| *bucket_id & 1 == 0)
                        .map(|(bucket_id, data)| (bucket_id >> 1, data))
                        .for_each(|(bucket_id, data)| {
                            //SAFETY - ok to do, due to the way the offset is calculated - no two threads should try to write at the same
                            //offset in the vector, and all fields of the vector should be populated
                            unsafe {
                                (*unsafe_output)[bucket_id][offset[bucket_id]]
                                    .as_mut_ptr()
                                    .write(data.clone());
                            };
                            offset[bucket_id] += 1;
                        })
                })
        }

        let output = unsafe_output.data.into_inner();
        //SAFETY - ok to do as all fields of the vector should be populated
        let output: Vec<Vec<T>> = unsafe { mem::transmute::<_, Vec<Vec<T>>>(output) };
        PartitionedColumn::FixedLenType(output)
    }
}

impl<T: Copy> ColumnPartition<Vec<T>, T> for Vec<T>
where
    T: Send + Sync,
    T: Hash,
{
    fn get_col(&self) -> &Vec<T> {
        &self
    }
}

pub struct StringVec {
    pub strvec: Vec<String>,
}

impl ColumnPartition<Vec<String>, String> for StringVec {
    fn get_col(&self) -> &Vec<String> {
        &self.strvec
    }
    fn partition_column(&self, bmap: &BucketsSizeMap) -> PartitionedColumn<String> {
        let column_data = self.get_col();
        let index = &bmap.hash.index;

        let column_len = match &index {
            Some(v) => v.len(),
            None => column_data.len(),
        };
        //check that the Bucket Size Map and the Column are compatible (needed to allow unsafe code below)
        assert_eq!(column_len, bmap.bucket_column.len());

        //Calculate data len per worker and bucket

        let chunk_len = bmap.chunk_len;

        let data_bmap: Vec<Vec<usize>> = if let Some(index) = index {
            index
                .par_chunks(chunk_len)
                .zip_eq(bmap.bucket_column.par_chunks(chunk_len))
                .map(|(index_chunk, bucket_chunk)| {
                    let mut local_map: Vec<usize> = vec![0; bmap.buckets_count];
                    index_chunk
                        .iter()
                        .zip(bucket_chunk.iter())
                        .filter(|(_index_id, bucket_id)| *bucket_id & 1 == 0)
                        .map(|(index_id, bucket_id)| (index_id, bucket_id >> 1))
                        .for_each(|(index_id, bucket_id)| {
                            local_map[bucket_id] += column_data[index_id.unwrap()].as_bytes().len()
                        });
                    local_map
                })
                .collect()
        } else {
            column_data
                .par_chunks(chunk_len)
                .zip_eq(bmap.bucket_column.par_chunks(chunk_len))
                .map(|(data_chunk, bucket_chunk)| {
                    let mut local_map: Vec<usize> = vec![0; bmap.buckets_count];
                    data_chunk
                        .iter()
                        .zip(bucket_chunk.iter())
                        .filter(|(_index_id, bucket_id)| *bucket_id & 1 == 0)
                        .map(|(data, bucket_id)| (data, bucket_id >> 1))
                        .for_each(|(data, bucket_id)| {
                            local_map[bucket_id] += data.as_bytes().len()
                        });
                    local_map
                })
                .collect()
        };

        //Calculate data offsets
        let workers_count = bmap.workers_count;
        let buckets_count = bmap.buckets_count;

        let mut data_offsets: Vec<Vec<usize>> = Vec::with_capacity(workers_count + 1);
        let mut v_current = vec![0; buckets_count];
        data_offsets.push(v_current.par_iter().map(|i| *i).collect());
        data_bmap.iter().for_each(|data_bucket_counts| {
            let mut v: Vec<usize> = Vec::with_capacity(buckets_count);
            v_current
                .par_iter_mut()
                .zip(data_bucket_counts.par_iter())
                .for_each(|(bucket_offset, current_val)| {
                    *bucket_offset += current_val;
                });
            v.par_extend(v_current.par_iter());
            data_offsets.push(v);
        });
        let data_bucket_sizes = data_offsets.pop().unwrap();

        let mut output: Vec<MaybeColumnU8> = data_bucket_sizes
            .par_iter()
            .zip_eq(bmap.bucket_sizes.par_iter())
            .map(|(i, bsize)| MaybeColumnU8 {
                data: Vec::with_capacity(*i),
                start_pos: Vec::with_capacity(*bsize),
                len: Vec::with_capacity(*bsize),
            })
            .collect();

        //SAFETY: OK to do, because enough capacity has been reserved, and the content of the vector is assumed to be uninitialized thanks to
        //definition using MaybeUninit
        output
            .par_iter_mut()
            .zip_eq(data_bucket_sizes.par_iter())
            .for_each(|(v, len)| unsafe { v.data.set_len(*len) });

        output
            .par_iter_mut()
            .zip_eq(bmap.bucket_sizes.par_iter())
            .for_each(|(v, len)| unsafe { v.start_pos.set_len(*len) });

        output
            .par_iter_mut()
            .zip_eq(bmap.bucket_sizes.par_iter())
            .for_each(|(v, len)| unsafe { v.len.set_len(*len) });

        struct UnsafeOutput {
            data: UnsafeCell<Vec<MaybeColumnU8>>,
        }
        //SAFETY: check below
        unsafe impl Sync for UnsafeOutput {}

        let unsafe_output = UnsafeOutput {
            data: UnsafeCell::new(output),
        };

        /////--Continue from here

        if let Some(index) = &index {
            index
                .par_chunks(chunk_len)
                .zip_eq(bmap.bucket_column.par_chunks(chunk_len))
                .zip_eq(data_offsets.par_iter())
                .zip_eq(bmap.offsets.par_iter())
                .for_each(
                    |(((index_chunk, bucket_chunk), initial_data_offset), initial_offset)| {
                        let mut data_offset = initial_data_offset.clone();
                        let mut offset = initial_offset.clone();
                        let unsafe_output = unsafe_output.data.get();
                        bucket_chunk
                            .iter()
                            .zip(index_chunk.iter())
                            .filter(|(bucket_id, _)| *bucket_id & 1 == 0)
                            .map(|(bucket_id, index)| (bucket_id >> 1, index))
                            .for_each(|(bucket_id, index)| {
                                //SAFETY - ok to do, due to the way the offset is calculated - no two threads should try to write at the same
                                //offset in the vector, and all fields of the vector should be populated
                                let slice_to_write = column_data[(*index).unwrap()].as_bytes();
                                unsafe {
                                    slice_to_write.iter().enumerate().for_each(|(i, c)| {
                                        (*unsafe_output)[bucket_id].data
                                            [data_offset[bucket_id] + i]
                                            .as_mut_ptr()
                                            .write(*c);
                                    });
                                };

                                unsafe {
                                    (*unsafe_output)[bucket_id].start_pos[offset[bucket_id]]
                                        .as_mut_ptr()
                                        .write(data_offset[bucket_id]);
                                };

                                unsafe {
                                    (*unsafe_output)[bucket_id].len[offset[bucket_id]]
                                        .as_mut_ptr()
                                        .write(slice_to_write.len());
                                };

                                data_offset[bucket_id] += slice_to_write.len();
                                offset[bucket_id] += 1;
                            })
                    },
                );
        } else {
            column_data
                .par_chunks(chunk_len)
                .zip_eq(bmap.bucket_column.par_chunks(chunk_len))
                .zip_eq(data_offsets.par_iter())
                .zip_eq(bmap.offsets.par_iter())
                .for_each(
                    |(((data_chunk, bucket_chunk), initial_data_offset), initial_offset)| {
                        let mut data_offset = initial_data_offset.clone();
                        let mut offset = initial_offset.clone();

                        let unsafe_output = unsafe_output.data.get();
                        bucket_chunk
                            .iter()
                            .zip(data_chunk.iter())
                            .filter(|(bucket_id, _)| *bucket_id & 1 == 0)
                            .map(|(bucket_id, data)| (bucket_id >> 1, data))
                            .for_each(|(bucket_id, data)| {
                                //SAFETY - ok to do, due to the way the offset is calculated - no two threads should try to write at the same
                                //offset in the vector, and all fields of the vector should be populated
                                let slice_to_write = data.as_bytes();
                                unsafe {
                                    slice_to_write.iter().enumerate().for_each(|(i, c)| {
                                        (*unsafe_output)[bucket_id].data
                                            [data_offset[bucket_id] + i]
                                            .as_mut_ptr()
                                            .write(*c);
                                    });
                                };

                                unsafe {
                                    (*unsafe_output)[bucket_id].start_pos[offset[bucket_id]]
                                        .as_mut_ptr()
                                        .write(data_offset[bucket_id]);
                                };

                                unsafe {
                                    (*unsafe_output)[bucket_id].len[offset[bucket_id]]
                                        .as_mut_ptr()
                                        .write(slice_to_write.len());
                                };
                                data_offset[bucket_id] += slice_to_write.len();
                                offset[bucket_id] += 1;
                            });
                    },
                )
        }

        let output = unsafe_output.data.into_inner();
        //SAFETY - ok to do asall fields of the vector should be populated
        let output: Vec<ColumnU8> = unsafe { mem::transmute::<_, Vec<ColumnU8>>(output) };

        PartitionedColumn::VariableLenType(output)
    }
}
