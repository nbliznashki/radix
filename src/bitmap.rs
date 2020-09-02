//! Utils for working with bits

use crate::bucketcolumn::*;
use crate::columnu8::*;

use std::cell::UnsafeCell;
use std::mem::{self, MaybeUninit};
use std::ops::{BitAnd, BitAndAssign, BitOr};

use rayon::prelude::*;

#[derive(Debug, Clone)]
pub struct Bitmap {
    pub(crate) bits: Vec<u8>,
}

impl Bitmap {
    pub fn new(num_bits: usize) -> Self {
        let len = num_bits;
        let v: Vec<u8> = vec![1; len];
        Bitmap { bits: v }
    }

    pub fn len(&self) -> usize {
        self.bits.len()
    }

    pub fn is_empty(&self) -> bool {
        self.bits.is_empty()
    }

    pub fn is_set(&self, i: usize) -> bool {
        self.bits[i] > 0
    }

    pub fn buffer_ref(&self) -> &Vec<u8> {
        &self.bits
    }

    pub fn into_buffer(self) -> Vec<u8> {
        self.bits
    }

    pub fn partition(&self, index: &ColumnIndex, bmap: &BucketsSizeMap) -> Vec<Option<Bitmap>> {
        let column_data = self;
        let index = index;

        let column_len = match index {
            Some(v) => v.len(),
            None => column_data.len(),
        };
        //check that the Bucket Size Map and the Column are compatible (needed to allow unsafe code below)
        assert_eq!(column_len, bmap.bucket_column.len());

        let mut output: Vec<Vec<MaybeUninit<u8>>> = bmap
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

        struct UnsafeOutput {
            data: UnsafeCell<Vec<Vec<MaybeUninit<u8>>>>,
        }
        //SAFETY: check below
        unsafe impl Sync for UnsafeOutput {}

        let unsafe_output = UnsafeOutput {
            data: UnsafeCell::new(output),
        };

        let chunk_len = bmap.chunk_len;

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
                        .map(|(bucket_id, index)| (*bucket_id, index))
                        .for_each(|(bucket_id, index)| {
                            //SAFETY - ok to do, due to the way the offset is calculated - no two threads should try to write at the same
                            //offset in the vector, and all fields of the vector should be populated
                            unsafe {
                                (*unsafe_output)[bucket_id][offset[bucket_id]]
                                    .as_mut_ptr()
                                    .write(column_data.bits[(*index)]);
                            };
                            offset[bucket_id] += 1;
                        })
                });
        } else {
            column_data
                .bits
                .par_chunks(chunk_len)
                .zip_eq(bmap.bucket_column.par_chunks(chunk_len))
                .zip_eq(bmap.offsets.par_iter())
                .for_each(|((data_chunk, bucket_chunk), initial_offset)| {
                    let mut offset = initial_offset.clone();

                    let unsafe_output = unsafe_output.data.get();
                    bucket_chunk
                        .iter()
                        .zip(data_chunk.iter())
                        .map(|(bucket_id, data)| (*bucket_id, data))
                        .for_each(|(bucket_id, data)| {
                            //SAFETY - ok to do, due to the way the offset is calculated - no two threads should try to write at the same
                            //offset in the vector, and all fields of the vector should be populated
                            unsafe {
                                (*unsafe_output)[bucket_id][offset[bucket_id]]
                                    .as_mut_ptr()
                                    .write(*data);
                            };
                            offset[bucket_id] += 1;
                        })
                })
        }

        let output = unsafe_output.data.into_inner();
        let output: Vec<Vec<u8>> = unsafe { mem::transmute::<_, Vec<Vec<u8>>>(output) };
        output
            .into_par_iter()
            .map(|v| Some(Bitmap { bits: v }))
            .collect()
    }
}

impl<'a, 'b> BitAnd<&'b Bitmap> for &'a Bitmap {
    type Output = Bitmap;

    fn bitand(self, rhs: &'b Bitmap) -> Bitmap {
        assert_eq!(&self.bits.len(), &rhs.bits.len());
        Bitmap {
            bits: self
                .bits
                .iter()
                .zip(rhs.bits.iter())
                .map(|(left, right)| *left & *right)
                .collect(),
        }
    }
}

impl<'a, 'b> BitAndAssign<&'b Bitmap> for Bitmap {
    fn bitand_assign(&mut self, rhs: &'b Bitmap) {
        assert_eq!(&self.bits.len(), &rhs.bits.len());
        self.bits
            .iter_mut()
            .zip(rhs.bits.iter())
            .for_each(|(left, right)| *left &= *right)
    }
}

impl<'a, 'b> BitOr<&'b Bitmap> for &'a Bitmap {
    type Output = Bitmap;

    fn bitor(self, rhs: &'b Bitmap) -> Bitmap {
        assert_eq!(&self.bits.len(), &rhs.bits.len());
        Bitmap {
            bits: self
                .bits
                .iter()
                .zip(rhs.bits.iter())
                .map(|(left, right)| *left | *right)
                .collect(),
        }
    }
}

impl From<Vec<u8>> for Bitmap {
    fn from(buf: Vec<u8>) -> Self {
        Self { bits: buf }
    }
}

impl PartialEq for Bitmap {
    fn eq(&self, other: &Self) -> bool {
        // buffer equality considers capacity, but here we want to only compare
        // actual data contents
        let self_len = self.bits.len();
        let other_len = other.bits.len();
        if self_len != other_len {
            return false;
        }
        self.bits[..self_len] == other.bits[..self_len]
    }
}

pub fn re_partition(
    bitmap_partitioned: &[Option<Bitmap>],
    index: &ColumnIndexPartitioned,
    bmap: &BucketsSizeMapPartitioned,
) -> Vec<Option<Bitmap>> {
    let column_data = bitmap_partitioned;

    if column_data.par_iter().filter(|b| b.is_some()).count() == 0 {
        return bmap.data.par_iter().map(|_| None).collect();
    };

    let mut output: Vec<Vec<MaybeUninit<u8>>> = bmap
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

    struct UnsafeOutput {
        data: UnsafeCell<Vec<Vec<MaybeUninit<u8>>>>,
    }
    //SAFETY: check below
    unsafe impl Sync for UnsafeOutput {}

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

                if let Some(data_chunk) = data_chunk {
                    if let Some(index_chunk) = index_chunk {
                        bucket_chunk
                            .iter()
                            .zip(index_chunk.iter())
                            .map(|(bucket_id, index)| (*bucket_id, index))
                            .for_each(|(bucket_id, index)| {
                                unsafe {
                                    (*unsafe_output)[bucket_id][offset[bucket_id]]
                                        .as_mut_ptr()
                                        .write(data_chunk.bits[(*index)]);
                                };
                                offset[bucket_id] += 1;
                            });
                    } else {
                        bucket_chunk
                            .iter()
                            .zip(data_chunk.bits.iter())
                            .map(|(bucket_id, data)| (*bucket_id, data))
                            .for_each(|(bucket_id, data)| {
                                unsafe {
                                    (*unsafe_output)[bucket_id][offset[bucket_id]]
                                        .as_mut_ptr()
                                        .write(*data);
                                };
                                offset[bucket_id] += 1;
                            });
                    }
                } else if let Some(index_chunk) = index_chunk {
                    bucket_chunk
                        .iter()
                        .zip(index_chunk.iter())
                        .map(|(bucket_id, index)| (*bucket_id, index))
                        .for_each(|(bucket_id, _index)| {
                            unsafe {
                                (*unsafe_output)[bucket_id][offset[bucket_id]]
                                    .as_mut_ptr()
                                    .write(1);
                            };
                            offset[bucket_id] += 1;
                        });
                } else {
                    bucket_chunk.iter().for_each(|bucket_id| {
                        unsafe {
                            (*unsafe_output)[*bucket_id][offset[*bucket_id]]
                                .as_mut_ptr()
                                .write(1);
                        };
                        offset[*bucket_id] += 1;
                    });
                };
            },
        );

    let output = unsafe_output.data.into_inner();
    //SAFETY - ok to do asall fields of the vector should be populated
    let output: Vec<Vec<u8>> = unsafe { mem::transmute::<_, Vec<Vec<u8>>>(output) };
    let bitmap: Vec<Option<Bitmap>> = output
        .into_par_iter()
        .map(|bits| Some(Bitmap { bits }))
        .collect();

    bitmap
}
