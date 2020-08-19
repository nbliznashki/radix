use std::ops::Deref;

#[derive(Debug, PartialEq)]
pub struct HashColumn {
    pub(crate) data: Vec<u64>,
    pub(crate) index: Option<Vec<Option<usize>>>,
}

impl Deref for HashColumn {
    type Target = Vec<u64>;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

pub struct HashColumnPartitioned {
    pub(crate) data: Vec<Vec<u64>>,
}

impl<'a> Deref for HashColumnPartitioned {
    type Target = Vec<Vec<u64>>;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}
