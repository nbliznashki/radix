use crate::bucketcolumn::*;
use crate::columnu8::*;
use crate::hashcolumn::*;
use rayon::prelude::*;
use std::cell::UnsafeCell;
use std::hash::{BuildHasher, Hash, Hasher};
use std::mem::{self, MaybeUninit};

pub trait ColumnFlatten<T> {
    fn flatten(self) -> Vec<T>
    where
        T: Send + Sync;
}
