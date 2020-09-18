use crate::columnu8::*;
use rayon::prelude::*;
use std::cell::UnsafeCell;
use std::mem::{self, MaybeUninit};

pub trait ColumnFlatten<T> {
    fn partitions_length(&self) -> Vec<usize>
    where
        T: Send + Sync;

    fn flatten_index(&self, index: &ColumnIndexPartitioned) -> ColumnIndexFlattenMap
    where
        T: Send + Sync;

    fn flatten_column(self, index: &ColumnIndexFlattenMap) -> FlattenedColumn<T>
    where
        T: Send + Sync,
        T: Clone;
}

//We want
//The new index
//Sorted partitioned index of unique values

impl<T> ColumnFlatten<T> for PartitionedColumn<T> {
    fn partitions_length(&self) -> Vec<usize>
    where
        T: Send + Sync,
    {
        let data_chunks_len: Vec<usize> = match &self {
            PartitionedColumn::FixedLenType(column_data, _index, _bitmap) => {
                column_data.par_iter().map(|v| (v.len())).collect()
            }
            PartitionedColumn::VariableLenType(columnu8_data, _index, _bitmap) => {
                columnu8_data.par_iter().map(|v| (v.len.len())).collect()
            }
        };
        data_chunks_len
    }
    fn flatten_index(&self, index: &ColumnIndexPartitioned) -> ColumnIndexFlattenMap
    where
        T: Send + Sync,
    {
        let data_chunks_len = self.partitions_length();

        let not_none_count: usize = index
            .par_iter()
            .map(|i| (*i).as_ref().map_or(0, |_| 1))
            .sum();

        if not_none_count == 0 {
            let mut target_write_offset: Vec<usize> = Vec::with_capacity(data_chunks_len.len());
            let mut current_write_offset = 0;

            data_chunks_len.iter().for_each(|len| {
                target_write_offset.push(current_write_offset);
                current_write_offset += len;
            });

            let target_total_len = current_write_offset;

            ColumnIndexFlattenMap {
                index_flattened: None,
                index_copy_map: data_chunks_len.par_iter().map(|_| None).collect(),
                data_target_len: data_chunks_len,
                target_write_offset,
                target_total_len,
            }
        } else {
            let index_copy_map: Vec<Option<Vec<usize>>> = index
                .par_iter()
                .map(|index_part| match index_part {
                    Some(index_part) => {
                        let mut v_clone: Vec<usize> = index_part.iter().copied().collect();
                        v_clone.sort();

                        let mut current_val = usize::MAX;
                        let mut index_part_unique: Vec<usize> = Vec::with_capacity(v_clone.len());

                        v_clone.into_iter().for_each(|i| {
                            if i != current_val {
                                index_part_unique.push(i);
                                current_val = i;
                            }
                        });
                        Some(index_part_unique)
                    }
                    None => None,
                })
                .collect();

            //Validate index copy map
            index_copy_map
                .par_iter()
                .zip_eq(data_chunks_len.par_iter())
                .filter(|(index_chunk, _data_len)| index_chunk.is_some())
                .for_each(|(index_chunk, data_len)| {
                    index_chunk
                        .as_ref()
                        .unwrap()
                        .iter()
                        .rev()
                        .take(1)
                        .filter(|i| *i >= data_len)
                        .for_each(|_| panic!("Invalid index!"))
                });

            //Used for calculating flattened index write offsets
            let adjusted_index_len: Vec<usize> = index
                .par_iter()
                .zip_eq(data_chunks_len.par_iter())
                .map(|(i, len)| i.as_ref().map_or(*len, |i| i.len()))
                .collect();

            let data_target_len: Vec<usize> = index_copy_map
                .par_iter()
                .zip_eq(data_chunks_len.par_iter())
                .map(|(index_copy_vec, data_len)| {
                    index_copy_vec
                        .as_ref()
                        .map_or(*data_len, |copy_vec| copy_vec.len())
                })
                .collect();

            let mut index_addon: Vec<usize> = Vec::with_capacity(adjusted_index_len.len());
            let mut current_index_addon = 0;

            data_target_len.iter().for_each(|len| {
                index_addon.push(current_index_addon);
                current_index_addon += len;
            });

            let mut index_write_offset: Vec<usize> = Vec::with_capacity(adjusted_index_len.len());
            let mut current_index_write_offset = 0;
            adjusted_index_len.iter().for_each(|len| {
                index_write_offset.push(current_index_write_offset);
                current_index_write_offset += len;
            });

            let index_total_len = current_index_write_offset;

            let mut output: MaybeColumnIndex = Vec::with_capacity(index_total_len);

            //SAFETY: ok to do due to usage of MaybeUnInit, and capacity reservation above
            unsafe { output.set_len(index_total_len) };
            struct UnsafeOutput {
                data: UnsafeCell<MaybeColumnIndex>,
            }
            //SAFETY: check below
            unsafe impl Sync for UnsafeOutput {}

            let unsafe_output = UnsafeOutput {
                data: UnsafeCell::new(output),
            };

            index
                .par_iter()
                .zip_eq(index_write_offset.par_iter())
                .zip_eq(index_addon.par_iter())
                .zip_eq(adjusted_index_len.par_iter())
                .for_each(|(((index_chunk, write_offset), addon), write_len)| {
                    let unsafe_output = unsafe_output.data.get();
                    //SAFETY - ok to do, each thread should get a separate piece of the target vector

                    match index_chunk {
                        Some(index) => index.iter().enumerate().for_each(|(i, index_val)| unsafe {
                            (*unsafe_output)[i + *write_offset]
                                .as_mut_ptr()
                                .write(index_val + addon)
                        }),
                        None => (0..*write_len).for_each(|i| unsafe {
                            (*unsafe_output)[i + *write_offset]
                                .as_mut_ptr()
                                .write(i + addon)
                        }),
                    };
                });

            let output = unsafe_output.data.into_inner();
            //SAFETY - ok to do asall fields of the vector should be populated
            #[allow(clippy::unsound_collection_transmute)]
            let output: ColumnIndexUnwrapped =
                unsafe { mem::transmute::<_, ColumnIndexUnwrapped>(output) };

            let mut target_write_offset: Vec<usize> = Vec::with_capacity(data_chunks_len.len());
            let mut current_write_offset = 0;

            data_target_len.iter().for_each(|len| {
                target_write_offset.push(current_write_offset);
                current_write_offset += len;
            });

            let target_total_len = current_write_offset;

            ColumnIndexFlattenMap {
                index_flattened: Some(output),
                index_copy_map,
                data_target_len,
                target_write_offset,
                target_total_len,
            }
        }
    }

    fn flatten_column(self, indexmap: &ColumnIndexFlattenMap) -> FlattenedColumn<T>
    where
        T: Send + Sync,
        T: Clone,
    {
        let data_chunks_len = self.partitions_length();
        //Validate index copy map
        indexmap
            .data_target_len
            .par_iter()
            .zip_eq(indexmap.index_copy_map.par_iter())
            .zip_eq(data_chunks_len.par_iter())
            .for_each(|((target_len, source_indexes), source_len)| {
                if target_len > source_len {
                    panic!("Invalid column index map!")
                };

                match source_indexes {
                    None => {
                        if target_len != source_len {
                            panic!("Invalid column index map!")
                        }
                    }
                    Some(source_indexes) => {
                        if source_indexes.len() != *target_len {
                            panic!("Invalid column index map!")
                        };

                        source_indexes.iter().rev().take(1).for_each(|i| {
                            if i >= source_len {
                                panic!("Invalid column index map!")
                            }
                        });
                    }
                }
            });

        //Calculate add-on offsets

        match self {
            PartitionedColumn::FixedLenType(column_data, _index, _bitmap) => {
                let mut output: Vec<MaybeUninit<T>> = Vec::with_capacity(indexmap.target_total_len);

                unsafe { output.set_len(indexmap.target_total_len) };
                struct UnsafeOutput<T> {
                    data: UnsafeCell<Vec<MaybeUninit<T>>>,
                }
                //SAFETY: check below
                unsafe impl<T: Sync> Sync for UnsafeOutput<T> {}

                let unsafe_output = UnsafeOutput {
                    data: UnsafeCell::new(output),
                };

                column_data
                    .par_iter()
                    .zip_eq(indexmap.target_write_offset.par_iter())
                    .zip_eq(indexmap.index_copy_map.par_iter())
                    .for_each(|((data_chunk, target_write_offset), copy_map)| {
                        let unsafe_output = unsafe_output.data.get();
                        if let Some(copy_map) = copy_map {
                            copy_map.iter().enumerate().for_each(|(i, copy_index)| {
                                unsafe {
                                    (*unsafe_output)[i + target_write_offset]
                                        .as_mut_ptr()
                                        .write(data_chunk[*copy_index].clone());
                                };
                            });
                        } else {
                            data_chunk.iter().enumerate().for_each(|(i, d)| {
                                unsafe {
                                    (*unsafe_output)[i + target_write_offset]
                                        .as_mut_ptr()
                                        .write(d.clone());
                                };
                            });
                        }
                    });

                let output = unsafe_output.data.into_inner();
                //SAFETY - ok to do asall fields of the vector should be populated
                let output: Vec<T> = unsafe { mem::transmute::<_, Vec<T>>(output) };
                FlattenedColumn::FixedLenType(output, None)
            }
            PartitionedColumn::VariableLenType(columnu8_data, _index, _bitmap) => {
                //
                //
                //STEP 1 - Derive the flattened len vector
                //OUTPUT: flattened_len
                //        start_pos_addon->lenght in u8 of all previous partitions summed up

                let mut flattened_len: Vec<MaybeUninit<usize>> =
                    Vec::with_capacity(indexmap.target_total_len);

                unsafe { flattened_len.set_len(indexmap.target_total_len) };
                struct UnsafeOutput<T> {
                    data: UnsafeCell<Vec<MaybeUninit<T>>>,
                }
                //SAFETY: check below
                unsafe impl<T: Sync> Sync for UnsafeOutput<T> {}

                let unsafe_flattened_len = UnsafeOutput {
                    data: UnsafeCell::new(flattened_len),
                };

                columnu8_data
                    .par_iter()
                    .zip_eq(indexmap.target_write_offset.par_iter())
                    .zip_eq(indexmap.index_copy_map.par_iter())
                    .for_each(|((data_chunk, target_write_offset), copy_map)| {
                        let unsafe_output = unsafe_flattened_len.data.get();
                        if let Some(copy_map) = copy_map {
                            copy_map.iter().enumerate().for_each(|(i, copy_index)| {
                                unsafe {
                                    (*unsafe_output)[i + target_write_offset]
                                        .as_mut_ptr()
                                        .write(data_chunk.len[*copy_index]);
                                };
                            });
                        } else {
                            data_chunk.len.iter().enumerate().for_each(|(i, d)| {
                                unsafe {
                                    (*unsafe_output)[i + target_write_offset]
                                        .as_mut_ptr()
                                        .write(*d);
                                };
                            });
                        }
                    });

                let flattened_len = unsafe_flattened_len.data.into_inner();
                //SAFETY - ok to do asall fields of the vector should be populated
                let flattened_len: Vec<usize> =
                    unsafe { mem::transmute::<_, Vec<usize>>(flattened_len) };

                let len_u8_per_partition: Vec<usize> = indexmap
                    .target_write_offset
                    .par_iter()
                    .zip_eq(indexmap.data_target_len.par_iter())
                    .map(|(offset, len)| flattened_len[*offset..*offset + *len].iter().sum())
                    .collect();

                let mut start_pos_addon: Vec<usize> =
                    Vec::with_capacity(len_u8_per_partition.len());
                let mut current_start_pos_offset = 0;

                len_u8_per_partition.iter().for_each(|len| {
                    start_pos_addon.push(current_start_pos_offset);
                    current_start_pos_offset += len;
                });

                //
                //
                //
                //STEP 2 - Derive the flattened start_pos vector
                //OUTPUT: flattened_start_pos

                let mut flattened_start_pos: Vec<MaybeUninit<usize>> =
                    Vec::with_capacity(indexmap.target_total_len);

                unsafe { flattened_start_pos.set_len(indexmap.target_total_len) };

                let unsafe_flattened_start_pos = UnsafeOutput {
                    data: UnsafeCell::new(flattened_start_pos),
                };

                columnu8_data
                    .par_iter()
                    .zip_eq(indexmap.target_write_offset.par_iter())
                    .zip_eq(indexmap.index_copy_map.par_iter())
                    .zip_eq(start_pos_addon.par_iter())
                    .for_each(
                        |(((data_chunk, target_write_offset), copy_map), start_pos_addon)| {
                            let unsafe_output = unsafe_flattened_start_pos.data.get();
                            if let Some(copy_map) = copy_map {
                                copy_map.iter().enumerate().for_each(|(i, copy_index)| {
                                    unsafe {
                                        (*unsafe_output)[i + target_write_offset]
                                            .as_mut_ptr()
                                            .write(
                                                start_pos_addon + data_chunk.start_pos[*copy_index],
                                            );
                                    };
                                });
                            } else {
                                data_chunk.start_pos.iter().enumerate().for_each(|(i, d)| {
                                    unsafe {
                                        (*unsafe_output)[i + target_write_offset]
                                            .as_mut_ptr()
                                            .write(start_pos_addon + *d);
                                    };
                                });
                            }
                        },
                    );

                let flattened_start_pos = unsafe_flattened_start_pos.data.into_inner();
                //SAFETY - ok to do as all fields of the vector should be populated
                let flattened_start_pos: Vec<usize> =
                    unsafe { mem::transmute::<_, Vec<usize>>(flattened_start_pos) };

                //
                //
                //STEP 3 - Derive the data vector
                //We already have the start_pos and the len of each element of the combined vector.

                let total_len: usize = flattened_start_pos.iter().rev().take(1).sum::<usize>()
                    + flattened_len.iter().rev().take(1).sum::<usize>();

                let mut flattened_data: Vec<MaybeUninit<u8>> = Vec::with_capacity(total_len);

                unsafe { flattened_data.set_len(total_len) };

                let unsafe_flattened_data = UnsafeOutput {
                    data: UnsafeCell::new(flattened_data),
                };

                columnu8_data
                    .par_iter()
                    .zip_eq(indexmap.target_write_offset.par_iter())
                    .zip_eq(indexmap.index_copy_map.par_iter())
                    .for_each(|((data_chunk, target_write_offset), copy_map)| {
                        let unsafe_output = unsafe_flattened_data.data.get();
                        if let Some(copy_map) = copy_map {
                            copy_map.iter().enumerate().for_each(|(i, copy_index)| {
                                let slice_to_write = &data_chunk.data[data_chunk.start_pos
                                    [*copy_index]
                                    ..data_chunk.start_pos[*copy_index]
                                        + data_chunk.len[*copy_index]];

                                let target_write_pos = flattened_start_pos[i + target_write_offset];

                                slice_to_write.iter().enumerate().for_each(|(i, d)| {
                                    unsafe {
                                        (*unsafe_output)[i + target_write_pos]
                                            .as_mut_ptr()
                                            .write(*d);
                                    };
                                });
                            });
                        } else {
                            let slice_to_write = data_chunk.data.as_slice();

                            slice_to_write.iter().enumerate().for_each(|(i, d)| {
                                unsafe {
                                    (*unsafe_output)[i + flattened_start_pos[*target_write_offset]]
                                        .as_mut_ptr()
                                        .write(*d);
                                };
                            });
                        }
                    });

                let flattened_data = unsafe_flattened_data.data.into_inner();
                //SAFETY - ok to do asall fields of the vector should be populated
                #[allow(clippy::unsound_collection_transmute)]
                let flattened_data: Vec<u8> =
                    unsafe { mem::transmute::<_, Vec<u8>>(flattened_data) };

                FlattenedColumn::VariableLenTypeU8(
                    ColumnU8 {
                        data: flattened_data,
                        start_pos: flattened_start_pos,
                        len: flattened_len,
                    },
                    None,
                )
            }
        }
    }
}
