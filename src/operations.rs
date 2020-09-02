use crate::{columnpartition::*, columnu8::*};

use std::convert::Into;
use std::ops::{Deref, DerefMut};

use rayon::prelude::*;

pub trait ColumnAdd<T1, U1, T2, U2> {
    fn cadd(
        data_left: &U1,
        data_right: &U2,
        index_left: &ColumnIndex,
        index_right: &ColumnIndex,
        parallel: bool,
    ) -> (Vec<T1>, ColumnIndex)
    where
        //Common trait bounds
        U1: ColumnPartition<U1, T1>,
        U2: ColumnPartition<U2, T2>,
        U1: Deref<Target = [T1]>,
        U2: Deref<Target = [T2]>,
        T1: Send + Sync,
        T2: Send + Sync,
        //Operation specific trait bounds
        T1: From<T2>,
        T1: std::ops::Add<Output = T1>,
        T1: Copy,
        T2: Copy,
    {
        let vec_left = data_left.get_col().deref();
        let vec_right = data_right.get_col().deref();

        match (index_left, index_right) {
            (None, None) => {
                let mut output: Vec<T1> = Vec::with_capacity(vec_left.len());
                if parallel {
                    let res_iter = vec_left
                        .par_iter()
                        .zip_eq(vec_right.par_iter())
                        .map(|(value_left, value_right)| *value_left + (*value_right).into());
                    output.par_extend(res_iter);
                } else {
                    assert_eq!(vec_left.len(), vec_right.len());
                    let res_iter = vec_left
                        .iter()
                        .zip(vec_right.iter())
                        .map(|(value_left, value_right)| *value_left + (*value_right).into());
                    output.extend(res_iter);
                };
                (output, None)
            }
            (None, Some(index_right)) => {
                let mut output: Vec<T1> = Vec::with_capacity(vec_left.len());
                if parallel {
                    let res_iter = vec_left.par_iter().zip_eq(index_right.par_iter()).map(
                        |(value_left, index_right)| *value_left + (vec_right[*index_right]).into(),
                    );
                    output.par_extend(res_iter);
                } else {
                    assert_eq!(vec_left.len(), index_right.len());
                    let res_iter =
                        vec_left
                            .iter()
                            .zip(index_right.iter())
                            .map(|(value_left, index_right)| {
                                *value_left + (vec_right[*index_right]).into()
                            });
                    output.extend(res_iter);
                };

                (output, Some(index_right.clone()))
            }

            (Some(index_left), None) => {
                let mut output: Vec<T1> = Vec::with_capacity(vec_right.len());
                if parallel {
                    let res_iter = index_left.par_iter().zip_eq(vec_right.par_iter()).map(
                        |(index_left, value_right)| vec_left[*index_left] + (*value_right).into(),
                    );
                    output.par_extend(res_iter);
                } else {
                    assert_eq!(index_left.len(), vec_right.len());
                    let res_iter =
                        index_left
                            .iter()
                            .zip(vec_right.iter())
                            .map(|(index_left, value_right)| {
                                vec_left[*index_left] + (*value_right).into()
                            });
                    output.extend(res_iter);
                };

                (output, Some(index_left.clone()))
            }

            (_, _) => panic!(),
        }
    }
}

pub trait ColumnAddInPlace<T1, U1, T2, U2> {
    fn cadd_inplace(data_left: &mut U1, data_right: &U2, parallel: bool)
    where
        //Common trait bounds
        U1: ColumnPartition<U1, T1>,
        U2: ColumnPartition<U2, T2>,
        U1: DerefMut<Target = [T1]>,
        U2: Deref<Target = [T2]>,
        T1: Send + Sync,
        T2: Send + Sync,
        //Operation specific trait bounds
        T1: From<T2>,
        T1: std::ops::AddAssign,
        //T1: Copy,
        T2: Copy,
    {
        let vec_left = data_left.get_col_mut().deref_mut();
        let vec_right = data_right.get_col().deref();

        if parallel {
            vec_left
                .par_iter_mut()
                .zip_eq(vec_right.par_iter())
                .for_each(|(value_left, value_right)| *value_left += (*value_right).into());
        } else {
            assert_eq!(vec_left.len(), vec_right.len());
            vec_left
                .iter_mut()
                .zip(vec_right.iter())
                .for_each(|(value_left, value_right)| *value_left += (*value_right).into());
        };
    }
}
