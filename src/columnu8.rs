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
    FixedLenType(Vec<Vec<T>>),
    VariableLenType(Vec<ColumnU8>),
}

#[derive(Debug, PartialEq)]
pub enum FlattenedColumn<T> {
    FixedLenType(Vec<T>),
    VariableLenType(Vec<String>),
    VariableLenTypeU8(ColumnU8),
}

pub struct StringVec {
    pub strvec: Vec<String>,
}

pub type ColumnIndex = Option<Vec<Option<usize>>>;

pub type MaybeColumnIndex = Vec<MaybeUninit<Option<usize>>>;
pub type ColumnIndexUnwrapped = Vec<Option<usize>>;

pub type ColumnIndexPartitioned = Vec<ColumnIndex>;
#[derive(Debug)]
pub struct ColumnIndexFlattenMap {
    pub index_flattened: ColumnIndex,
    pub index_copy_map: Vec<Option<Vec<usize>>>,
    pub data_target_len: Vec<usize>,
    pub target_write_offset: Vec<usize>,
    pub target_total_len: usize,
}
