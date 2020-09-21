use crate::columnu8::*;
use crate::{bitmap::*, ColumnWrapper, InputTypes, OpDictionary, Signature};

use std::{
    any::TypeId,
    convert::TryFrom,
    ops::{Deref, DerefMut},
};

pub type HashData = Vec<u64>;

#[derive(Debug)]
pub struct HashColumn<'a> {
    pub(crate) data: ColumnWrapper<'a>,
}

impl<'a> From<HashColumn<'a>> for ColumnWrapper<'a> {
    fn from(hash: HashColumn<'a>) -> Self {
        hash.data
    }
}

impl<'a> TryFrom<ColumnWrapper<'a>> for HashColumn<'a> {
    type Error = &'static str;
    fn try_from(c: ColumnWrapper<'a>) -> Result<Self, Self::Error> {
        if c.index().is_some() {
            return Err("A Hash column can't have an index");
        }
        if c.typeid() == TypeId::of::<HashData>() {
            let (v, index, bitmap) = c.all_unwrap::<HashData>();
            Ok(HashColumn {
                data: ColumnWrapper::new(v, index, bitmap),
            })
        } else {
            return Err("A Hash column should have a different data type");
        }
    }
}

impl<'a> Deref for HashColumn<'a> {
    type Target = Vec<u64>;
    fn deref(&self) -> &Self::Target {
        self.data.downcast_ref::<HashData>()
    }
}

impl<'a> DerefMut for HashColumn<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.data.downcast_mut::<HashData>()
    }
}

impl<'a> HashColumn<'a> {
    pub fn new(data: Vec<u64>, bitmap: Option<Bitmap>) -> Self {
        Self {
            data: ColumnWrapper::new(data, None, bitmap),
        }
    }
    pub fn new_ref(data: &'a Vec<u64>, bitmap: Option<Bitmap>) -> Self {
        Self {
            data: ColumnWrapper::new_ref(data, None, bitmap),
        }
    }

    pub fn bitmap(&self) -> &Option<Bitmap> {
        self.data.bitmap()
    }
    pub fn bitmap_mut(&mut self) -> &mut Option<Bitmap> {
        self.data.bitmap_mut()
    }

    pub fn hash_c(c: &ColumnWrapper, dict: &OpDictionary) -> Self {
        let v: HashData = Vec::new();
        let signature = Signature::new("hash=", vec![c.typeid()], vec![c.typename().clone()]);
        let op = dict
            .get(&signature)
            .unwrap_or_else(|| panic!("Operation not found in dictionary: {:?}", signature));
        let mut output = ColumnWrapper::<'static>::new(v, None, None);
        (op.f)(&mut output, vec![InputTypes::Ref(c)]);
        HashColumn::<'static>::try_from(output)
            .unwrap_or_else(|_| panic!("Failed to convert a ColumnWrapper to HashColumn"))
    }

    pub fn hashadd_c(&mut self, c: &ColumnWrapper, dict: &OpDictionary) {
        let v: HashData = Vec::new();
        let signature = Signature::new("hash+=", vec![c.typeid()], vec![c.typename().clone()]);
        let op = dict
            .get(&signature)
            .unwrap_or_else(|| panic!("Operation not found in dictionary: {:?}", signature));
        let mut output = ColumnWrapper::<'static>::new(v, None, None);
        (op.f)(&mut self.data, vec![InputTypes::Ref(c)]);
    }
}

pub struct HashColumnPartitioned {
    pub(crate) data: Vec<Vec<u64>>,
    pub(crate) index: ColumnIndexPartitioned,
    pub(crate) bitmap: Vec<Option<Bitmap>>,
}

impl<'a> Deref for HashColumnPartitioned {
    type Target = Vec<Vec<u64>>;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}
