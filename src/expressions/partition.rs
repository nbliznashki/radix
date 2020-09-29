use crate::Signature;

use std::{collections::HashMap, mem};

use paste::paste;

pub struct ChunksSizedMut<'a, T: 'a> {
    v: &'a mut [T],
}

impl<'a, T> ChunksSizedMut<'a, T> {
    fn next_exact(&mut self, chunk_len: usize) -> Option<&'a mut [T]> {
        if chunk_len <= self.v.len() {
            let tmp = mem::replace(&mut self.v, &mut []);
            let (head, tail) = tmp.split_at_mut(chunk_len);
            self.v = tail;
            Some(head)
        } else {
            None
        }
    }
    //fn is_empty(&self) -> bool {
    //    self.v.len() == 0
    //}
}

pub trait ColumnPartitionInner {
    fn part<'a: 'b, 'b>(
        &self,
        inp: &'b ColumnWrapper<'a>,
        chunk_size: usize,
    ) -> Result<Vec<ColumnWrapper<'b>>, ErrorDesc>;
    fn part_mut<'a: 'b, 'b>(
        &self,
        inp: &'b mut ColumnWrapper<'a>,
        chunk_size: usize,
    ) -> Result<Vec<ColumnWrapper<'b>>, ErrorDesc>;
    fn part_with_sizes<'a: 'b, 'b>(
        &self,
        inp: &'b ColumnWrapper<'a>,
        chunks_size: &Vec<usize>,
    ) -> Result<Vec<ColumnWrapper<'b>>, ErrorDesc>;
    fn part_with_sizes_mut<'a: 'b, 'b>(
        &self,
        inp: &'b mut ColumnWrapper<'a>,
        chunks_size: &Vec<usize>,
    ) -> Result<Vec<ColumnWrapper<'b>>, ErrorDesc>;
}

pub type PartitionDictionary = HashMap<Signature, Box<dyn ColumnPartitionInner>>;

use crate::*;

#[allow(dead_code)]
const OP: &str = "part";

macro_rules! binary_operation_load {
    ($dict:ident; $($tr:ty)+) => ($(
            let signature=sig![OP;Vec<$tr>];
            $dict.insert(
                signature, Box::new(
                    {
                        paste!{[<ColumnPartitionVec $tr>]::new()}
                    }
                )
            );
    )+)
}

macro_rules! binary_operation_impl {
    ($($tr:ty)+) => ($(
        paste!{
            struct [<ColumnPartitionVec $tr>]{}
            impl [<ColumnPartitionVec $tr>]{
                fn new()->Self{
                    Self{}
                }
            }
            impl ColumnPartitionInner for [<ColumnPartitionVec $tr>]
            {
                fn part<'a: 'b, 'b>(&self, inp: &'b ColumnWrapper<'a>, chunk_size: usize) -> Result<Vec<ColumnWrapper<'b>>,ErrorDesc>
                {
                type V=Vec<$tr>;
                let (data_input, index_input, bitmap_input) = (inp.downcast_ref::<V>()?, inp.index(), inp.bitmap());

                match index_input {
                    Some(ind) => ind
                        .chunks(chunk_size)
                        .try_fold(Vec::with_capacity((ind.len()+chunk_size-1)/std::cmp::max(chunk_size,1)), |mut acc, ind| {acc.push(inp.copy_inner_as_ref()?.with_index_slice(ind)); Ok(acc)}),
                    None => {
                        let mut output: Vec<ColumnWrapper<'b>>=data_input.chunks(chunk_size).map(|c| ColumnWrapper::new_slice(c)).collect();
                        if let Some(b) = bitmap_input {
                            output=output.into_iter()
                                .zip(b.chunks(chunk_size))
                                .map(|(c, b)| c.with_bitmap_slice(b)).collect();
                        }
                        Ok(output)
                    }
                }
            }

            fn part_mut<'a: 'b, 'b>(&self, inp: &'b mut ColumnWrapper<'a>, chunk_size: usize) -> Result<Vec<ColumnWrapper<'b>>, ErrorDesc>
                {
                type V=Vec<$tr>;
                let (data_input, index_input, bitmap_input) = inp.all_mut::<V>()?;

                if let Some(_) = index_input {
                    Err("Can't partition a column with an index into mutable parts")?
                };


                let mut output: Vec<ColumnWrapper<'b>>=data_input.chunks_mut(chunk_size).map(|c| ColumnWrapper::new_slice_mut(c)).collect();
                if let Some(b) = bitmap_input {
                    output=output.into_iter()
                        .zip(b.bits.chunks_mut(chunk_size))
                        .map(|(c, b)| c.with_bitmap_slice_mut(b)).collect();
                }
                Ok(output)
            }

            fn part_with_sizes<'a: 'b, 'b>(
                &self,
                inp: &'b ColumnWrapper<'a>,
                chunks_size: &Vec<usize>,
            ) -> Result<Vec<ColumnWrapper<'b>>, ErrorDesc>{

                type V=Vec<$tr>;
                let (data_input, index_input, bitmap_input) = (inp.downcast_ref::<V>()?, inp.index(), inp.bitmap());

                let mut output: Vec<ColumnWrapper<'b>>=Vec::with_capacity(chunks_size.len());

                let total_len=chunks_size.iter().sum::<usize>();

                match index_input {
                    Some(ind) => {
                        if ind.len()!=total_len {
                            Err(format!("Trying to partition an index with length {} into parts with total length {}", ind.len(), total_len))?
                        };
                        let mut cur_slice=ind;
                        chunks_size.iter().try_fold(Vec::with_capacity(chunks_size.len()), |mut acc, l| {
                            let (l,r)=cur_slice.split_at(*l);
                            cur_slice=r;
                            acc.push(inp.copy_inner_as_ref()?.with_index_slice(l));
                            Ok(acc)
                        })
                    }
                    None => {
                        if data_input.len()!=total_len {
                            Err(format!("Trying to partition a data object with length {} into parts with total length {}", data_input.len(), total_len))?
                        };
                        let mut cur_slice_data=data_input.as_slice();
                        if let Some(bitmap)=bitmap_input{
                            if bitmap.len()!=total_len{
                                Err(format!("Trying to partition a data object with bitmap index of length {} into parts with total length {}", bitmap.len(), total_len))?
                            };
                            let mut cur_slice_bitmap=bitmap;
                            chunks_size.iter().for_each(|l|{
                                let (l_data,r)=cur_slice_data.split_at(*l);
                                cur_slice_data=r;
                                let (l_bitmap,r)=cur_slice_bitmap.split_at(*l);
                                cur_slice_bitmap=r;
                                output.push(ColumnWrapper::new_slice(l_data).with_bitmap_slice(l_bitmap));
                            });
                        } else{
                            chunks_size.iter().for_each(|l|{
                                let (l_data,r)=cur_slice_data.split_at(*l);
                                cur_slice_data=r;
                                output.push(ColumnWrapper::new_slice(l_data));
                            });
                        }
                        Ok(output)
                    }
                }

            }
            fn part_with_sizes_mut<'a: 'b, 'b>(
                &self,
                inp: &'b mut ColumnWrapper<'a>,
                chunks_size: &Vec<usize>,
            ) -> Result<Vec<ColumnWrapper<'b>>, ErrorDesc>{

                type V=Vec<$tr>;
                let (data_input, index_input, bitmap_input) = inp.all_mut::<V>()?;

                let total_len=chunks_size.iter().sum::<usize>();

                if let Some(_) = index_input {
                    Err("Can't partition a column with an index into mutable parts")?
                };

                if data_input.len()!=total_len {
                    Err(format!("Trying to partition a data object with length {} into parts with total length {}", data_input.len(), total_len))?
                };

                let mut output: Vec<ColumnWrapper<'b>>=Vec::with_capacity(chunks_size.len());
                let mut cur_slice_data=ChunksSizedMut{v: data_input.as_mut_slice()};

                if let Some(bitmap)=bitmap_input{
                    if bitmap.len()!=total_len{
                        Err(format!("Trying to partition a data object with bitmap index of length {} into parts with total length {}", bitmap.len(), total_len))?
                    };
                    let mut cur_slice_bitmap=ChunksSizedMut{v: bitmap.bits.as_mut_slice()};
                    chunks_size.iter().for_each(|i|{
                        let r_data=cur_slice_data.next_exact(*i).unwrap();
                        let r_bitmap=cur_slice_bitmap.next_exact(*i).unwrap();
                        output.push(ColumnWrapper::new_slice_mut(r_data).with_bitmap_slice_mut(r_bitmap));
                    });
                } else {
                    chunks_size.iter().for_each(|i|{
                        let r=cur_slice_data.next_exact(*i).unwrap();
                        output.push(ColumnWrapper::new_slice_mut(r));
                    });
                }
                Ok(output)
            }
        }
    }
    )+)
}

binary_operation_impl! {

u64 u32 u16 u8 bool usize

}

//{ usize u8 u16 u32 u64 u128 isize i8 i16 i32 i64 i128 f32 f64 }

//binary_operation_impl! { (u64,u8) (u64,u16) (u64,u32) (u64,u64) }

pub fn load_part_dict(part_dict: &mut PartitionDictionary) {
    //dict.insert(s, columnadd_onwedcolumnvecu64_vecu64);7
    binary_operation_load! {part_dict;

        u64 u32 u16 u8 bool usize

    };
}
