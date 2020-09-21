use crate::bitmap::*;
use crate::bucketcolumn::*;
use crate::column::hashcolumn::*;
use crate::columnu8::*;
use rayon::prelude::*;
use std::cell::UnsafeCell;
use std::hash::{BuildHasher, Hash, Hasher};
use std::mem::{self, MaybeUninit};

//TO-DO:
//Implement bitmap re-partition

pub trait ColumnRePartition<T> {
    fn hash_column<S>(&self, s: &S) -> HashColumnPartitioned
    where
        S: BuildHasher + Sync,
        T: Send + Sync,
        T: Hash;
    fn hash_column_append<S>(&self, s: &S, h: &mut HashColumnPartitioned)
    where
        S: BuildHasher + Sync,
        T: Send + Sync,
        T: Hash;
    fn partition_column(&self, bmap: &BucketsSizeMapPartitioned) -> PartitionedColumn<T>
    where
        T: Send + Sync,
        T: Clone;
}

impl<T> ColumnRePartition<T> for PartitionedColumn<T> {
    fn hash_column<S>(&self, s: &S) -> HashColumnPartitioned
    where
        S: BuildHasher + Sync,
        T: Send + Sync,
        T: Hash,
    {
        let (index, bitmap) = match &self {
            PartitionedColumn::FixedLenType(_, index, bitmap) => (index, bitmap),
            PartitionedColumn::VariableLenType(_, index, bitmap) => (index, bitmap),
        };

        let bitmap_hash: Vec<Option<Bitmap>> = index
            .par_iter()
            .zip_eq(bitmap.par_iter())
            .map(|(index, bitmap)| {
                if let Some(bitmap) = &bitmap {
                    if let Some(index) = &index {
                        let bits = index.par_iter().map(|i| bitmap.bits[*i]).collect();
                        Some(Bitmap { bits })
                    } else {
                        Some((*bitmap).clone())
                    }
                } else {
                    None
                }
            })
            .collect();

        match &self {
            PartitionedColumn::FixedLenType(column_data, index, _bitmap) => HashColumnPartitioned {
                data: column_data
                    .par_iter()
                    .zip_eq(index.par_iter())
                    .zip_eq(bitmap_hash.par_iter())
                    .map(|((column_data_part, index_part), bitmap_part)| {
                        if let Some(bitmap) = bitmap_part {
                            if let Some(index) = &index_part {
                                index
                                    .iter()
                                    .zip(bitmap.bits.iter())
                                    .map(|(i, nullbit)| {
                                        if *nullbit != 0 {
                                            let mut h = s.build_hasher();
                                            column_data_part[*i].hash(&mut h);
                                            h.finish()
                                        } else {
                                            u64::MAX
                                        }
                                    })
                                    .collect()
                            } else {
                                column_data
                                    .iter()
                                    .zip(bitmap.bits.iter())
                                    .map(|(t, nullbit)| {
                                        if *nullbit != 0 {
                                            let mut h = s.build_hasher();
                                            t.hash(&mut h);
                                            h.finish()
                                        } else {
                                            u64::MAX
                                        }
                                    })
                                    .collect()
                            }
                        } else if let Some(index) = &index_part {
                            index
                                .iter()
                                .map(|i| {
                                    let mut h = s.build_hasher();
                                    column_data_part[*i].hash(&mut h);
                                    h.finish()
                                })
                                .collect()
                        } else {
                            column_data
                                .iter()
                                .map(|t| {
                                    let mut h = s.build_hasher();
                                    t.hash(&mut h);
                                    h.finish()
                                })
                                .collect()
                        }
                    })
                    .collect(),
                index: column_data.par_iter().map(|_| None).collect(),
                bitmap: bitmap_hash,
            },
            PartitionedColumn::VariableLenType(columnu8_data, index, _bitmap) => {
                HashColumnPartitioned {
                    data: columnu8_data
                        .par_iter()
                        .zip_eq(index.par_iter())
                        .zip_eq(bitmap_hash.par_iter())
                        .map(|((column_data_part, index_part), bitmap_part)| {
                            if let Some(bitmap) = bitmap_part {
                                if let Some(index) = &index_part {
                                    let columnu8 = &column_data_part;
                                    index
                                        .iter()
                                        .zip(bitmap.bits.iter())
                                        .map(|(i, nullbit)| {
                                            if *nullbit != 0 {
                                                let mut h = s.build_hasher();
                                                let slice_u8 = columnu8.data.as_slice();
                                                let start_pos = columnu8.start_pos[*i];
                                                let end_pos = start_pos + columnu8.len[*i];
                                                slice_u8[start_pos..end_pos].hash(&mut h);
                                                h.finish()
                                            } else {
                                                u64::MAX
                                            }
                                        })
                                        .collect()
                                } else {
                                    let columnu8 = &column_data_part;
                                    columnu8
                                        .start_pos
                                        .iter()
                                        .zip(columnu8.len.iter())
                                        .zip(bitmap.bits.iter())
                                        .map(|((start_pos, len), nullbit)| {
                                            if *nullbit != 0 {
                                                let mut h = s.build_hasher();
                                                let slice_u8 = columnu8.data.as_slice();
                                                let end_pos = start_pos + len;
                                                //TO-DO: Handle NULLS
                                                slice_u8[*start_pos..end_pos].hash(&mut h);
                                                h.finish()
                                            } else {
                                                u64::MAX
                                            }
                                        })
                                        .collect()
                                }
                            } else if let Some(index) = &index_part {
                                let columnu8 = &column_data_part;
                                index
                                    .iter()
                                    .map(|i| {
                                        let mut h = s.build_hasher();
                                        let slice_u8 = columnu8.data.as_slice();
                                        let start_pos = columnu8.start_pos[*i];
                                        let end_pos = start_pos + columnu8.len[*i];
                                        slice_u8[start_pos..end_pos].hash(&mut h);
                                        h.finish()
                                    })
                                    .collect()
                            } else {
                                let columnu8 = &column_data_part;
                                columnu8
                                    .start_pos
                                    .iter()
                                    .zip(columnu8.len.iter())
                                    .map(|(start_pos, len)| {
                                        let mut h = s.build_hasher();
                                        let slice_u8 = columnu8.data.as_slice();
                                        let end_pos = start_pos + len;
                                        //TO-DO: Handle NULLS
                                        slice_u8[*start_pos..end_pos].hash(&mut h);
                                        h.finish() << 1
                                    })
                                    .collect()
                            }
                        })
                        .collect(),
                    index: columnu8_data.par_iter().map(|_| None).collect(),
                    bitmap: bitmap_hash,
                }
            }
        }
    }

    fn hash_column_append<S>(&self, s: &S, h: &mut HashColumnPartitioned)
    where
        S: BuildHasher + Sync,
        T: Send + Sync,
        T: Hash,
    {
        let (index, bitmap) = match &self {
            PartitionedColumn::FixedLenType(_, index, bitmap) => (index, bitmap),
            PartitionedColumn::VariableLenType(_, index, bitmap) => (index, bitmap),
        };

        assert_eq!(index.len(), bitmap.len());
        assert_eq!(h.bitmap.len(), bitmap.len());
        assert_eq!(h.bitmap.len(), h.data.len());

        let bitmap_column_expanded: Vec<Option<Bitmap>> = index
            .par_iter()
            .zip_eq(bitmap.par_iter())
            .map(|(index, bitmap)| {
                if let Some(bitmap) = bitmap {
                    if let Some(index) = &index {
                        let bits = index.par_iter().map(|i| bitmap.bits[*i]).collect();
                        Some(Bitmap { bits })
                    } else {
                        Some((*bitmap).clone())
                    }
                } else {
                    None
                }
            })
            .collect();

        match &self {
            PartitionedColumn::FixedLenType(column_data, index, _bitmap) => {
                column_data
                    .par_iter()
                    .zip_eq(index.par_iter())
                    .zip_eq(bitmap_column_expanded.par_iter())
                    .zip_eq(h.data.par_iter_mut())
                    .for_each(|(((column_data_part, index_part), bitmap_part), h)| {
                        if let Some(bitmap) = bitmap_part {
                            if let Some(index) = &index_part {
                                h.iter_mut()
                                    .zip(index.iter())
                                    .zip(bitmap.bits.iter())
                                    .for_each(|((current_hash, i), nullbit)| {
                                        if *nullbit != 0 {
                                            let mut h = s.build_hasher();
                                            column_data_part[*i].hash(&mut h);
                                            *current_hash = current_hash.wrapping_add(h.finish());
                                        } else {
                                            *current_hash = current_hash.wrapping_add(u64::MAX);
                                        }
                                    });
                            } else {
                                h.iter_mut()
                                    .zip(column_data_part.iter())
                                    .zip(bitmap.bits.iter())
                                    .for_each(|((current_hash, t), nullbit)| {
                                        if *nullbit != 0 {
                                            let mut h = s.build_hasher();
                                            t.hash(&mut h);
                                            *current_hash = current_hash.wrapping_add(h.finish());
                                        } else {
                                            *current_hash = current_hash.wrapping_add(u64::MAX);
                                        }
                                    });
                            }
                        } else if let Some(index) = &index_part {
                            h.par_iter_mut().zip_eq(index.par_iter()).for_each(
                                |(current_hash, i)| {
                                    let mut h = s.build_hasher();
                                    column_data_part[*i].hash(&mut h);
                                    *current_hash = current_hash.wrapping_add(h.finish());
                                },
                            );
                        } else {
                            h.par_iter_mut()
                                .zip_eq(column_data_part.par_iter())
                                .for_each(|(current_hash, t)| {
                                    let mut h = s.build_hasher();
                                    t.hash(&mut h);
                                    *current_hash = current_hash.wrapping_add(h.finish());
                                });
                        };
                    });
            }
            PartitionedColumn::VariableLenType(columnu8_data, index, _bitmap) => {
                columnu8_data
                    .par_iter()
                    .zip_eq(index.par_iter())
                    .zip_eq(bitmap_column_expanded.par_iter())
                    .zip_eq(h.data.par_iter_mut())
                    .for_each(|(((column_data_part, index_part), bitmap_part), h)| {
                        if let Some(bitmap) = bitmap_part {
                            if let Some(index) = &index_part {
                                let columnu8 = &column_data_part;
                                assert_eq!(h.len(), index.len());
                                h.iter_mut()
                                    .zip(index.iter())
                                    .zip(bitmap.bits.iter())
                                    .for_each(|((current_hash, i), nullbit)| {
                                        if *nullbit != 0 {
                                            let mut h = s.build_hasher();
                                            let slice_u8 = columnu8.data.as_slice();
                                            let start_pos = columnu8.start_pos[*i];
                                            let end_pos = start_pos + columnu8.len[*i];
                                            //TO-DO: Handle NULLS
                                            //TO-DO: Check for nested parallelism
                                            slice_u8[start_pos..end_pos].hash(&mut h);
                                            *current_hash = current_hash.wrapping_add(h.finish());
                                        } else {
                                            *current_hash = current_hash.wrapping_add(u64::MAX);
                                        }
                                    });
                            } else {
                                let columnu8 = &column_data_part;
                                h.iter_mut()
                                    .zip(columnu8.start_pos.iter())
                                    .zip(columnu8.len.iter())
                                    .zip(bitmap.bits.iter())
                                    .for_each(|(((current_hash, start_pos), len), nullbit)| {
                                        if *nullbit != 0 {
                                            let mut h = s.build_hasher();
                                            let slice_u8 = columnu8.data.as_slice();
                                            let end_pos = start_pos + len;
                                            //TO-DO: Handle NULLS
                                            slice_u8[*start_pos..end_pos].hash(&mut h);
                                            *current_hash = current_hash.wrapping_add(h.finish());
                                        } else {
                                            *current_hash = current_hash.wrapping_add(u64::MAX);
                                        }
                                    });
                            }
                        } else if let Some(index) = &index_part {
                            let columnu8 = &column_data_part;
                            assert_eq!(h.len(), index.len());
                            h.iter_mut()
                                .zip(index.iter())
                                .for_each(|(current_hash, i)| {
                                    let mut h = s.build_hasher();
                                    let slice_u8 = columnu8.data.as_slice();
                                    let start_pos = columnu8.start_pos[*i];
                                    let end_pos = start_pos + columnu8.len[*i];
                                    //TO-DO: Handle NULLS
                                    //TO-DO: Check for nested parallelism
                                    slice_u8[start_pos..end_pos].hash(&mut h);
                                    *current_hash = current_hash.wrapping_add(h.finish());
                                });
                        } else {
                            let columnu8 = &column_data_part;
                            h.iter_mut()
                                .zip(columnu8.start_pos.iter())
                                .zip(columnu8.len.iter())
                                .for_each(|((current_hash, start_pos), len)| {
                                    let mut h = s.build_hasher();
                                    let slice_u8 = columnu8.data.as_slice();
                                    let end_pos = start_pos + len;
                                    //TO-DO: Handle NULLS
                                    slice_u8[*start_pos..end_pos].hash(&mut h);
                                    *current_hash = current_hash.wrapping_add(h.finish());
                                });
                        };
                    });
            }
        };

        h.bitmap
            .par_iter_mut()
            .zip_eq(bitmap_column_expanded.into_par_iter())
            .for_each(|(bitmap, bitmap_column)| {
                if let Some(mut bitmap_column) = bitmap_column {
                    if let Some(bitmap_current) = bitmap {
                        bitmap_column &= &bitmap_current;
                    };

                    bitmap.replace(bitmap_column);
                };
            });
    }

    fn partition_column(&self, bmap: &BucketsSizeMapPartitioned) -> PartitionedColumn<T>
    where
        T: Send + Sync,
        T: Clone,
    {
        let (index, bitmap) = match &self {
            PartitionedColumn::FixedLenType(_, index, bitmap) => (index, bitmap),
            PartitionedColumn::VariableLenType(_, index, bitmap) => (index, bitmap),
        };

        let bitmap_repartitioned = re_partition(bitmap, index, bmap);

        match &self {
            PartitionedColumn::FixedLenType(column_data, index, _bitmap) => {
                column_data
                    .par_iter()
                    .zip_eq(index.par_iter())
                    .zip_eq(bmap.bucket_column.par_iter())
                    .for_each(|((data, index), bc)| {
                        let column_len = match index {
                            Some(v) => v.len(),
                            None => data.len(),
                        };
                        //check that the Bucket Size Map and the Column are compatible (needed to allow unsafe code below)
                        assert_eq!(column_len, bc.len());
                    });
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

                //PUSH the data (as it is local) to remote for the CPU storage

                column_data
                    .par_iter()
                    .zip_eq(index.par_iter())
                    .zip_eq(bmap.bucket_column.par_iter())
                    .zip_eq(bmap.offsets.par_iter())
                    .for_each(
                        |(((data_chunk, index_chunk), bucket_chunk), initial_offset)| {
                            let mut offset = initial_offset.clone();
                            let unsafe_output = unsafe_output.data.get();

                            if let Some(index_chunk) = index_chunk {
                                bucket_chunk
                                    .iter()
                                    .zip(index_chunk.iter())
                                    .map(|(bucket_id, index)| (*bucket_id, index))
                                    .for_each(|(bucket_id, index)| {
                                        unsafe {
                                            (*unsafe_output)[bucket_id][offset[bucket_id]]
                                                .as_mut_ptr()
                                                .write(data_chunk[*index].clone());
                                        };
                                        offset[bucket_id] += 1;
                                    });
                            } else {
                                bucket_chunk
                                    .iter()
                                    .zip(data_chunk.iter())
                                    .map(|(bucket_id, data)| (*bucket_id, data))
                                    .for_each(|(bucket_id, data)| {
                                        unsafe {
                                            (*unsafe_output)[bucket_id][offset[bucket_id]]
                                                .as_mut_ptr()
                                                .write(data.clone());
                                        };
                                        offset[bucket_id] += 1;
                                    });
                            }
                        },
                    );

                let output = unsafe_output.data.into_inner();
                //SAFETY - ok to do asall fields of the vector should be populated
                let output: Vec<Vec<T>> = unsafe { mem::transmute::<_, Vec<Vec<T>>>(output) };
                let index = output.par_iter().map(|_| None).collect();

                PartitionedColumn::FixedLenType(output, index, bitmap_repartitioned)
            }
            PartitionedColumn::VariableLenType(columnu8_data, index, _bitmap) => {
                columnu8_data
                    .par_iter()
                    .zip_eq(index.par_iter())
                    .zip_eq(bmap.bucket_column.par_iter())
                    .for_each(|((data, index), bc)| {
                        let column_len = match index {
                            Some(v) => v.len(),
                            None => data.start_pos.len(),
                        };
                        //check that the Bucket Size Map and the Column are compatible (needed to allow unsafe code below)
                        assert_eq!(column_len, bc.len());
                    });

                let buckets_count = bmap.buckets_count;
                let data_bmap: Vec<Vec<usize>> = columnu8_data
                    .par_iter()
                    .zip_eq(index.par_iter())
                    .zip_eq(bmap.bucket_column.par_iter())
                    .map(|((columnu8_data_part, index_part), bucket_chunk)| {
                        let mut local_map: Vec<usize> = vec![0; buckets_count];
                        if let Some(index) = &index_part {
                            index
                                .iter()
                                .zip(bucket_chunk.iter())
                                .map(|(index, bucket_id)| (index, *bucket_id))
                                .for_each(|(index, bucket_id)| {
                                    local_map[bucket_id] += columnu8_data_part.len[*index];
                                });
                        } else {
                            columnu8_data_part
                                .len
                                .iter()
                                .zip(bucket_chunk.iter())
                                .map(|(len, bucket_id)| (len, *bucket_id))
                                .for_each(|(len, bucket_id)| {
                                    local_map[bucket_id] += len;
                                });
                        }
                        local_map
                    })
                    .collect();

                //Calculate data offsets

                let mut data_offsets: Vec<Vec<usize>> =
                    Vec::with_capacity(bmap.bucket_column.len() + 1);

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

                columnu8_data
                    .par_iter()
                    .zip_eq(index.par_iter())
                    .zip_eq(bmap.bucket_column.par_iter())
                    .zip_eq(data_offsets.par_iter())
                    .zip_eq(bmap.offsets.par_iter())
                    .for_each(
                        |(
                            (((columnu8_data_part, index_part), bucket_chunk), initial_data_offset),
                            initial_offset,
                        )| {
                            let mut data_offset = initial_data_offset.clone();
                            let mut offset = initial_offset.clone();
                            let unsafe_output = unsafe_output.data.get();
                            if let Some(index) = &index_part {
                                let columnu8 = &columnu8_data_part;
                                bucket_chunk
                                    .iter()
                                    .zip(index.iter())
                                    .map(|(bucket_id, index)| (*bucket_id, *index))
                                    .for_each(|(bucket_id, i)| {
                                        //SAFETY - ok to do, due to the way the offset is calculated - no two threads should try to write at the same
                                        //offset in the vector, and all fields of the vector should be populated

                                        let slice_u8 = columnu8.data.as_slice();
                                        let start_pos = columnu8.start_pos[i];
                                        let end_pos = start_pos + columnu8.len[i];

                                        let slice_to_write = &slice_u8[start_pos..end_pos];
                                        unsafe {
                                            slice_to_write.iter().enumerate().for_each(|(i, c)| {
                                                (*unsafe_output)[bucket_id].data
                                                    [data_offset[bucket_id] + i]
                                                    .as_mut_ptr()
                                                    .write(*c);
                                            });
                                        };

                                        unsafe {
                                            (*unsafe_output)[bucket_id].start_pos
                                                [offset[bucket_id]]
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
                            } else {
                                let columnu8 = &columnu8_data_part;
                                bucket_chunk
                                    .iter()
                                    .zip(columnu8.start_pos.iter())
                                    .zip(columnu8.len.iter())
                                    .map(|((bucket_id, start_pos), len)| {
                                        ((*bucket_id, start_pos), len)
                                    })
                                    .for_each(|((bucket_id, start_pos), len)| {
                                        //SAFETY - ok to do, due to the way the offset is calculated - no two threads should try to write at the same
                                        //offset in the vector, and all fields of the vector should be populated
                                        let slice_u8 = columnu8.data.as_slice();
                                        let end_pos = start_pos + len;
                                        let slice_to_write = &slice_u8[*start_pos..end_pos];
                                        unsafe {
                                            slice_to_write.iter().enumerate().for_each(|(i, c)| {
                                                (*unsafe_output)[bucket_id].data
                                                    [data_offset[bucket_id] + i]
                                                    .as_mut_ptr()
                                                    .write(*c);
                                            });
                                        };

                                        unsafe {
                                            (*unsafe_output)[bucket_id].start_pos
                                                [offset[bucket_id]]
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
                            }
                        },
                    );

                let output = unsafe_output.data.into_inner();
                //SAFETY - ok to do asall fields of the vector should be populated
                let output: Vec<ColumnU8> = unsafe { mem::transmute::<_, Vec<ColumnU8>>(output) };

                let index = output.par_iter().map(|_| None).collect();

                PartitionedColumn::VariableLenType(output, index, bitmap_repartitioned)
            }
        }
    }
}
