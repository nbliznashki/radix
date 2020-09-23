use std::ops::{Deref, DerefMut};

use crate::{ColumnWrapper, HashColumn, OpDictionary};

pub struct HashColumnPartitioned<'a> {
    data: Vec<HashColumn<'a>>,
}

impl<'a> Deref for HashColumnPartitioned<'a> {
    type Target = Vec<HashColumn<'a>>;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}

impl<'a> DerefMut for HashColumnPartitioned<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}

impl<'a> HashColumnPartitioned<'a> {}
