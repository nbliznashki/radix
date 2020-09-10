use crate::Signature;
use std::any::{Any, TypeId};
use std::collections::HashMap;

use crate::OwnedColumn;
use concat_idents::concat_idents;

pub type InitOutput = Box<dyn Any + Send + Sync + 'static>;
pub type InitOperation = fn() -> InitOutput;

pub type InitDictionary = HashMap<Signature, InitOperation>;

use crate::*;

#[allow(dead_code)]
const OP: &str = "new";

macro_rules! binary_operation_load {
    ($dict:ident; $($tr:ty)+) => ($(
        concat_idents!(fn_name = new, _, ownedcolumnvec,$tr {
            let signature=sig![OP;OwnedColumn<Vec<$tr>>];
            $dict.insert(signature, fn_name);
        });
    )+)
}

macro_rules! binary_operation_impl {
    ($($tr:ty)+) => ($(
        concat_idents!(fn_name = new, _, ownedcolumnvec,$tr {
            #[allow(dead_code)]
            fn fn_name()->InitOutput {
                Box::new(OwnedColumn::<Vec<$tr>>::new(vec![], None, None))
            }

    });
    )+)
}

binary_operation_impl! {

u64 u32 u16 u8 bool

}

//{ usize u8 u16 u32 u64 u128 isize i8 i16 i32 i64 i128 f32 f64 }

//binary_operation_impl! { (u64,u8) (u64,u16) (u64,u32) (u64,u64) }

pub fn load_init_dict(init_dict: &mut InitDictionary) {
    //dict.insert(s, columnadd_onwedcolumnvecu64_vecu64);7
    binary_operation_load! {init_dict;

        u64 u32 u16 u8 bool

    };
}
