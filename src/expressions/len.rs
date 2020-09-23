use crate::Signature;

use std::collections::HashMap;

use paste::paste;

pub type LenOperation = fn(&ColumnWrapper) -> usize;

pub type LenDictionary = HashMap<Signature, LenOperation>;

use crate::*;

#[allow(dead_code)]
const OP: &str = "len";

macro_rules! binary_operation_load {
    ($dict:ident; $($tr:ty)+) => ($(
        {
            type V=Vec<$tr>;
            let signature=sig![OP;V];
            $dict.insert(signature, paste!{[<new_vec $tr>]});
        }
        {
            type V=[$tr];
            let signature=sig![OP;V];
            $dict.insert(signature, paste!{[<new_slice $tr>]});
        }
    )+)
}

macro_rules! binary_operation_impl {
    ($($tr:ty)+) => ($(
        paste!{
            fn [<new_vec $tr>](c: &ColumnWrapper)->usize
            {
                type T=$tr;
                type V=Vec<T>;
                c.downcast_ref::<V>().len()
            }
        }
        paste!{
            fn [<new_slice $tr>](c: &ColumnWrapper)->usize
            {
                type T=$tr;
                type V=[T];
                c.downcast_slice_ref::<V>().len()
            }
        }
    )+)
}

binary_operation_impl! {

u64 u32 u16 u8 bool usize

}

//{ usize u8 u16 u32 u64 u128 isize i8 i16 i32 i64 i128 f32 f64 }

//binary_operation_impl! { (u64,u8) (u64,u16) (u64,u32) (u64,u64) }

pub fn load_len_dict(len_dict: &mut LenDictionary) {
    //dict.insert(s, columnadd_onwedcolumnvecu64_vecu64);7
    binary_operation_load! {len_dict;

        u64 u32 u16 u8 bool usize

    };
}
