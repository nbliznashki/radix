use crate::bitmap::Bitmap;
use crate::{Column, ColumnMut, OwnedColumn};
use concat_idents::concat_idents;
use core::any::{Any, TypeId};

use std::ops::Add;

use rayon::prelude::*;

use crate::*;

#[allow(dead_code)]
const OP: &str = "+";

macro_rules! operation_load {
    ($dict:ident; $(($tl:ty, $tr:ty))+) => ($(
        concat_idents!(fn_name = add, _, ownedcolumnvec,$tl,_,vec,$tr {
            let signature=sig![OP;OwnedColumn<Vec<$tl>>; OwnedColumn<Vec<$tl>>, OwnedColumn<Vec<$tr>>];
            $dict.insert(signature, fn_name);
        });
    )+)
}

macro_rules! operation_impl {
    ($(($tl:ty, $tr:ty))+) => ($(
        concat_idents!(fn_name = add, _, ownedcolumnvec,$tl,_,vec,$tr {
            #[allow(dead_code)]
            fn fn_name(left: &mut dyn Any, right: Vec<InputTypes>) {



                type T1=$tl;
                type T2=$tr;

                let output = left.downcast_mut::<OwnedColumn<Vec<T1>>>().unwrap();
                let down_lhs = match &right[0] {
                    InputTypes::Ref(a)=>a.downcast_ref::<OwnedColumn<Vec<T1>>>().unwrap(),
                    InputTypes::Owned(a)=>a.downcast_ref::<OwnedColumn<Vec<T1>>>().unwrap()
                };

                let down_rhs = match &right[1] {
                    InputTypes::Ref(a)=>a.downcast_ref::<OwnedColumn<Vec<T2>>>().unwrap(),
                    InputTypes::Owned(a)=>a.downcast_ref::<OwnedColumn<Vec<T2>>>().unwrap()
                };

                let index_rhs=down_rhs.index();
                let data_rhs=down_rhs.col();
                let bitmap_rhs=down_rhs.bitmap();

                let index_lhs=down_lhs.index();
                let data_lhs=down_lhs.col();
                let bitmap_lhs=down_lhs.bitmap();


                let len_rhs = if let Some(ind) = index_rhs {
                    ind.len()
                } else {
                    data_rhs.len()
                };

                let len_lhs = if let Some(ind) = index_lhs {
                    ind.len()
                } else {
                    data_lhs.len()
                };

                assert_eq!(len_rhs, len_lhs);

                //Clean up
                let (data_output, index_output, bitmap_output) = output.all_mut();
                data_output.truncate(0);
                *index_output=None;
                *bitmap_output=None;
                //Reserve enough storage for result
                data_output.reserve(len_lhs);


                let bits_new=if (bitmap_lhs.is_some()) || (bitmap_rhs.is_some()){
                    let mut v: Vec<u8>=Vec::new();

                    if let Some(bitm_lhs)=&bitmap_lhs{
                        if let Some(ind_lhs)=&index_lhs{
                            v.par_extend(ind_lhs.par_iter().map(|i| bitm_lhs.bits[*i]))
                        } else {
                            v.par_extend(bitm_lhs.bits.par_iter())
                        }
                    };
                    if v.len()==0 {
                        if let Some(bitm_rhs)=&bitmap_rhs{
                            if let Some(ind_rhs)=&index_rhs{
                              v.par_extend(ind_rhs.par_iter().map(|i| bitm_rhs.bits[*i]))
                            } else {
                              v.par_extend(bitm_rhs.bits.par_iter())
                            }
                        };
                    } else {
                        if let Some(bitm_rhs)=&bitmap_rhs{
                            if let Some(ind_rhs)=&index_rhs{
                              v.par_iter_mut().zip_eq(ind_rhs.par_iter()).for_each(|(b,i)| *b&=bitm_rhs.bits[*i])
                            } else {
                                v.par_iter_mut().zip_eq(bitm_rhs.bits.par_iter()).for_each(|(b,bl)| *b&=*bl)
                            }
                        };
                    }

                    Some(v)
                } else
                {None};

                println!("{:?}", bits_new);
                println!("{:?}", bitmap_lhs);
                println!("{:?}", bitmap_rhs);

                match (index_lhs, index_rhs, &bits_new){
                    (None, None, None)=>{
                        data_output.par_extend(
                            data_lhs.par_iter().zip_eq(data_rhs.par_iter()).map(|(lv, rv)| (*lv).add(T1::from(*rv)))
                        );
                    },
                    (Some(ind_lhs), None, None)=>{
                        data_output.par_extend(
                            ind_lhs.par_iter().zip_eq(data_rhs.par_iter()).map(|(li, rv)| (data_lhs[*li]).add(T1::from(*rv)))
                        );
                    },
                    (None, Some(ind_rhs), None)=>{
                        data_output.par_extend(
                            data_lhs.par_iter().zip_eq(ind_rhs.par_iter()).map(|(lv, ri)| (*lv).add(T1::from(data_lhs[*ri])))
                        );
                    },
                    (Some(ind_lhs), Some(ind_rhs), None)=>{
                        data_output.par_extend(
                            ind_lhs.par_iter().zip_eq(ind_rhs.par_iter()).map(|(li, ri)| (data_lhs[*li]).add(T1::from(data_lhs[*ri])))
                        );
                    },


                    (None, None, Some(bits))=>{
                        data_output.par_extend(
                            data_lhs
                            .par_iter()
                            .zip_eq(data_rhs.par_iter())
                            .zip_eq(bits.par_iter())
                            .map(|((lv, rv), b)|
                                if *b!=0 {
                                    (*lv).add(T1::from(*rv))
                                } else {Default::default()}
                            )
                        );
                    },
                    (Some(ind_lhs), None, Some(bits))=>{
                        data_output.par_extend(
                            ind_lhs
                            .par_iter()
                            .zip_eq(data_rhs.par_iter())
                            .zip_eq(bits.par_iter())
                            .map(|((li, rv), b)|
                                if *b!=0 {
                                    (data_lhs[*li]).add(T1::from(*rv))
                                }   else {Default::default()}
                            )
                        );
                    },
                    (None, Some(ind_rhs), Some(bits))=>{
                        data_output.par_extend(
                            data_lhs
                            .par_iter()
                            .zip_eq(ind_rhs.par_iter())
                            .zip_eq(bits.par_iter())
                            .map(|((lv, ri),b)|
                                if *b!=0 {
                                    (*lv).add(T1::from(data_lhs[*ri]))
                                } else {Default::default()}
                        )
                        );
                    },
                    (Some(ind_lhs), Some(ind_rhs), Some(bits))=>{
                        data_output.par_extend(
                            ind_lhs
                            .par_iter()
                            .zip_eq(ind_rhs.par_iter())
                            .zip_eq(bits.par_iter())
                            .map(|((li, ri),b)|
                                if *b!=0 {
                                    (data_lhs[*li]).add(T1::from(data_lhs[*ri]))
                                } else {Default::default()}
                            )
                        );
                    },

                };

                if let Some(bits)=bits_new{
                    *bitmap_output=Some(Bitmap{bits})
                };


            }

    });
    )+)
}

operation_impl! {

(u64, u64) (u64, u32) (u64, u16) (u64, u8) (u64, bool)

}

//{ usize u8 u16 u32 u64 u128 isize i8 i16 i32 i64 i128 f32 f64 }

//operation_impl! { (u64,u8) (u64,u16) (u64,u32) (u64,u64) }

pub(crate) fn load_op_dict(dict: &mut OpDictionary) {
    //dict.insert(s, columnadd_onwedcolumnvecu64_vecu64);7
    operation_load! {dict;

            (u64, u64) (u64, u32) (u64, u16) (u64, u8) (u64, bool)

    };
}
