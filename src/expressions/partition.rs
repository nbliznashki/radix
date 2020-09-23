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
    fn is_empty(&self) -> bool {
        self.v.len() == 0
    }
}

pub trait ColumnPartitionInner {
    fn part<'a: 'b, 'b>(
        &self,
        inp: &'b ColumnWrapper<'a>,
        chunk_size: usize,
    ) -> Vec<ColumnWrapper<'b>>;
    fn part_mut<'a: 'b, 'b>(
        &self,
        inp: &'b mut ColumnWrapper<'a>,
        chunk_size: usize,
    ) -> Vec<ColumnWrapper<'b>>;
    fn part_with_sizes<'a: 'b, 'b>(
        &self,
        inp: &'b ColumnWrapper<'a>,
        chunks_size: &Vec<usize>,
    ) -> Vec<ColumnWrapper<'b>>;
    fn part_with_sizes_mut<'a: 'b, 'b>(
        &self,
        inp: &'b mut ColumnWrapper<'a>,
        chunks_size: &Vec<usize>,
    ) -> Vec<ColumnWrapper<'b>>;
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
                fn part<'a: 'b, 'b>(&self, inp: &'b ColumnWrapper<'a>, chunk_size: usize) -> Vec<ColumnWrapper<'b>>
                {
                type V=Vec<$tr>;
                let (data_input, index_input, bitmap_input) = (inp.downcast_ref::<V>(), inp.index(), inp.bitmap());

                //Not supported at the moment
                //TO-DO: fix that
                assert_eq!(bitmap_input, &None);
                assert_eq!(index_input, &None);

                let output: Vec<ColumnWrapper<'b>>=data_input.chunks(chunk_size).map(|c| ColumnWrapper::new_slice(c, None, None)).collect();

                output
            }

            fn part_mut<'a: 'b, 'b>(&self, inp: &'b mut ColumnWrapper<'a>, chunk_size: usize) -> Vec<ColumnWrapper<'b>>
                {
                type V=Vec<$tr>;
                let (data_input, index_input, bitmap_input) = inp.all_mut::<V>();



                //Not supported at the moment
                //TO-DO: fix that
                assert_eq!(bitmap_input, &None);
                assert_eq!(index_input, &None);

                let output: Vec<ColumnWrapper<'b>>=data_input.chunks_mut(chunk_size).map(|c| ColumnWrapper::new_slice_mut(c, None, None)).collect();

                output
            }

            fn part_with_sizes<'a: 'b, 'b>(
                &self,
                inp: &'b ColumnWrapper<'a>,
                chunks_size: &Vec<usize>,
            ) -> Vec<ColumnWrapper<'b>>{

                type V=Vec<$tr>;
                let (data_input, index_input, bitmap_input) = (inp.downcast_ref::<V>(), inp.index(), inp.bitmap());

                //Not supported at the moment
                //TO-DO: fix that
                assert_eq!(bitmap_input, &None);
                assert_eq!(index_input, &None);

                let mut output: Vec<ColumnWrapper<'b>>=Vec::with_capacity(chunks_size.len());
                let mut cur_slice=data_input.as_slice();

                chunks_size.iter().for_each(|l|{
                    let (l,r)=cur_slice.split_at(*l);
                    cur_slice=r;
                    output.push(ColumnWrapper::new_slice(l, None, None));
                });

                output

            }
            fn part_with_sizes_mut<'a: 'b, 'b>(
                &self,
                inp: &'b mut ColumnWrapper<'a>,
                chunks_size: &Vec<usize>,
            ) -> Vec<ColumnWrapper<'b>>{

                type V=Vec<$tr>;
                let (data_input, index_input, bitmap_input) = inp.all_mut::<V>();

                //Not supported at the moment
                //TO-DO: fix that
                assert_eq!(bitmap_input, &None);
                assert_eq!(index_input, &None);

                let mut output: Vec<ColumnWrapper<'b>>=Vec::with_capacity(chunks_size.len());
                let mut cur_slice=ChunksSizedMut{v: data_input.as_mut_slice()};

                chunks_size.iter().for_each(|i|{
                    let r=cur_slice.next_exact(*i).unwrap();
                    output.push(ColumnWrapper::new_slice_mut(r, None, None));
                });
                assert!(cur_slice.is_empty());
                output
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
