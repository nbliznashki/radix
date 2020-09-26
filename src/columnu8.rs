use crate::bitmap::*;

use std::mem::MaybeUninit;

#[derive(Debug, PartialEq)]
pub struct ColumnU8 {
    pub(crate) data: Vec<u8>,
    pub(crate) start_pos: Vec<usize>,
    pub(crate) len: Vec<usize>,
}

pub(crate) struct MaybeColumnU8 {
    pub(crate) data: Vec<MaybeUninit<u8>>,
    pub(crate) start_pos: Vec<MaybeUninit<usize>>,
    pub(crate) len: Vec<MaybeUninit<usize>>,
}

#[derive(Debug, PartialEq)]
pub enum PartitionedColumn<T> {
    FixedLenType(Vec<Vec<T>>, ColumnIndexPartitioned, Vec<Option<Bitmap>>),
    VariableLenType(Vec<ColumnU8>, ColumnIndexPartitioned, Vec<Option<Bitmap>>),
}

#[derive(Debug, PartialEq)]
pub enum FlattenedColumn<T> {
    FixedLenType(Vec<T>, Option<Bitmap>),
    VariableLenType(Vec<String>, Option<Bitmap>),
    VariableLenTypeU8(ColumnU8, Option<Bitmap>),
}

pub type ColumnIndex = Option<Vec<usize>>;

pub type ColumnIndexRef<'a> = Option<&'a [usize]>;

pub type MaybeColumnIndex = Vec<MaybeUninit<usize>>;
pub type ColumnIndexUnwrapped = Vec<usize>;

pub type ColumnIndexPartitioned = Vec<ColumnIndex>;
#[derive(Debug)]
pub struct ColumnIndexFlattenMap {
    pub index_flattened: ColumnIndex,
    pub index_copy_map: Vec<Option<Vec<usize>>>,
    pub data_target_len: Vec<usize>,
    pub target_write_offset: Vec<usize>,
    pub target_total_len: usize,
}
