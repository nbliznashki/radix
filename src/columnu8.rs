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

pub struct StringVec {
    pub strvec: Vec<String>,
}

pub type ColumnIndex = Option<Vec<Option<usize>>>;

pub type ColumnIndexPartitioned = Vec<ColumnIndex>;
