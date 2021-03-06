use crate::{bitmap::*, ColumnWrapper, InputTypes, OpDictionary, Signature};
use crate::{columnu8::*, ErrorDesc};

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
    type Error = ErrorDesc;
    fn try_from(c: ColumnWrapper<'a>) -> Result<Self, Self::Error> {
        if c.index().is_some() {
            Err("A Hash column can't have an index")?;
        }
        if c.typeid() == TypeId::of::<HashData>() {
            let (v, index, bitmap) = c.all_unwrap::<HashData>()?;
            Ok(HashColumn {
                data: ColumnWrapper::new(v).with_index(index).with_bitmap(bitmap),
            })
        } else {
            Err("A Hash column should have a different data type")?
        }
    }
}

impl<'a> Deref for HashColumn<'a> {
    type Target = Vec<u64>;
    fn deref(&self) -> &Self::Target {
        //Should never fail, and if it fails a panic should follow
        self.data.downcast_ref::<HashData>().unwrap()
    }
}

impl<'a> DerefMut for HashColumn<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        //Should never fail, and if it fails a panic should follow
        self.data.downcast_mut::<HashData>().unwrap()
    }
}

impl<'a> HashColumn<'a> {
    pub fn new(data: Vec<u64>) -> Self {
        Self {
            data: ColumnWrapper::new(data),
        }
    }
    pub fn new_ref(data: &'a Vec<u64>) -> Self {
        Self {
            data: ColumnWrapper::new_ref(data),
        }
    }

    pub fn with_bitmap(self, b: Option<Bitmap>) -> Self {
        Self {
            data: self.data.with_bitmap(b),
        }
    }

    pub fn with_bitmap_ref(self, b: &'a Option<Bitmap>) -> Self {
        Self {
            data: self.data.with_bitmap_ref(b),
        }
    }

    pub fn bitmap(&self) -> Option<&[u8]> {
        self.data.bitmap()
    }
    pub fn bitmap_mut(&mut self) -> &mut Option<Bitmap> {
        self.data.bitmap_mut()
    }

    pub fn hash_c(c: &ColumnWrapper, dict: &OpDictionary) -> Result<Self, ErrorDesc> {
        let v: HashData = Vec::new();
        let signature = Signature::new("hash=", vec![c.typeid()], vec![c.typename().clone()]);
        let op = dict.get(&signature);
        if let Some(op) = op {
            let mut output = ColumnWrapper::<'static>::new(v);
            (op.f)(&mut output, vec![InputTypes::Ref(c)])?;
            HashColumn::<'static>::try_from(output)
        } else {
            Err(format!(
                "Operation not found in dictionary: {:?}",
                signature
            ))?
        }
    }

    pub fn hashadd_c(&mut self, c: &ColumnWrapper, dict: &OpDictionary) -> Result<(), ErrorDesc> {
        let signature = Signature::new("hash+=", vec![c.typeid()], vec![c.typename().clone()]);
        let op = dict.get(&signature);
        if let Some(op) = op {
            (op.f)(&mut self.data, vec![InputTypes::Ref(c)])?;
            Ok(())
        } else {
            Err(format!(
                "Operation not found in dictionary: {:?}",
                signature
            ))?
        }
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
