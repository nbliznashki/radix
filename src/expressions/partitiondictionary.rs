use crate::Signature;

use std::collections::HashMap;

use paste::paste;

pub trait ColumnPartitionInner {
    fn part<'a: 'b, 'b>(
        &self,
        inp: &'b InputTypes<'a>,
        chunk_size: usize,
    ) -> Vec<ColumnWrapper<'b>>;
}

pub type PartitionDictionary = HashMap<Signature, Box<dyn ColumnPartitionInner>>;

//pub type PartitionOperation<'a, 'b> = fn(&'b InputTypes<'a>, usize) -> Vec<ColumnWrapper<'b>>;

//pub type PartitionDictionary<'a, 'b> = HashMap<Signature, PartitionOperation<'a, 'b>>;

use crate::*;

#[allow(dead_code)]
const OP: &str = "part";

macro_rules! binary_operation_load {
    ($dict:ident; $($tr:ty)+) => ($(
            let signature=sig![OP;Vec<$tr>];
            $dict.insert(
                signature, Box::new(
                    {let d: paste!{[<ColumnPartition $tr>]}=paste!{[<ColumnPartition $tr>]::new()};
                    d}
                )
            );
    )+)
}

macro_rules! binary_operation_impl {
    ($($tr:ty)+) => ($(
        paste!{
            struct [<ColumnPartition $tr>]{}
            impl [<ColumnPartition $tr>]{
                fn new()->Self{
                    Self{}
                }
            }
            impl ColumnPartitionInner for [<ColumnPartition $tr>]
            {
                fn part<'a: 'b, 'b>(&self, inp: &'b InputTypes<'a>, chunk_size: usize) -> Vec<ColumnWrapper<'b>>
                {
                type V=Vec<$tr>;
                let (data_input, index_input, bitmap_input) = match inp {
                    InputTypes::Ref(a) => (
                        a.downcast_ref::<V>(),
                        a.index(),
                        a.bitmap(),
                    ),
                    InputTypes::Owned(a) => (
                        a.downcast_ref::<V>(),
                        a.index(),
                        a.bitmap(),
                    ),
                };

                //Not supported at the moment
                //TO-DO: fix that
                assert_eq!(bitmap_input, &None);
                assert_eq!(index_input, &None);

                let output: Vec<ColumnWrapper<'b>>=data_input.chunks(chunk_size).map(|c| ColumnWrapper::new_slice(c, None, None)).collect();

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
