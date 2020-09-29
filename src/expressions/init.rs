use crate::Signature;

use std::collections::HashMap;

use paste::paste;

pub type InitOperation = fn() -> ColumnWrapper<'static>;

pub type InitDictionary = HashMap<Signature, InitOperation>;

use crate::*;

#[allow(dead_code)]
const OP: &str = "new";

macro_rules! binary_operation_load {
    ($dict:ident; $($tr:ty)+) => ($(
            let signature=sig![OP;Vec<$tr>];
            $dict.insert(signature, paste!{[<new_vec $tr>]});
    )+)
}

macro_rules! binary_operation_impl {
    ($($tr:ty)+) => ($(
        paste!{

            #[allow(dead_code)]
            fn [<new_vec $tr>]()->ColumnWrapper<'static> {
                ColumnWrapper::new(Vec::<$tr>::new())
            }

        }
    )+)
}

binary_operation_impl! {

u64 u32 u16 u8 bool usize

}

//{ usize u8 u16 u32 u64 u128 isize i8 i16 i32 i64 i128 f32 f64 }

//binary_operation_impl! { (u64,u8) (u64,u16) (u64,u32) (u64,u64) }

pub fn load_init_dict(init_dict: &mut InitDictionary) {
    //dict.insert(s, columnadd_onwedcolumnvecu64_vecu64);7
    binary_operation_load! {init_dict;

        u64 u32 u16 u8 bool usize

    };
}
