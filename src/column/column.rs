use crate::bitmap::*;
use std::ops::Deref;
pub trait Column<V> {
    fn col(&self) -> &V;
    fn index(&self) -> &Option<Vec<usize>>;
    fn bitmap(&self) -> &Option<Bitmap>;
}

pub trait ColumnMut<V>: Column<V> {
    fn col_mut(&mut self) -> &mut V;
    fn index_mut(&mut self) -> &mut Option<Vec<usize>>;
    fn bitmap_mut(&mut self) -> &mut Option<Bitmap>;
}

pub struct OwnedColumn<V> {
    col: V,
    index: Option<Vec<usize>>,
    bitmap: Option<Bitmap>,
}

impl<V> OwnedColumn<V> {
    pub fn new<T>(col: V, index: Option<Vec<usize>>, bitmap: Option<Bitmap>) -> Self
    where
        V: Deref<Target = [T]>,
    {
        //Validate that the bitmap and the data have the same length
        bitmap
            .iter()
            .for_each(|b| assert_eq!((*col).len(), b.len()));

        Self { col, index, bitmap }
    }
}

impl<V> Column<V> for OwnedColumn<V> {
    fn col(&self) -> &V {
        &self.col
    }
    fn index(&self) -> &Option<Vec<usize>> {
        &self.index
    }
    fn bitmap(&self) -> &Option<Bitmap> {
        &self.bitmap
    }
}

impl<V> ColumnMut<V> for OwnedColumn<V> {
    fn col_mut(&mut self) -> &mut V {
        &mut self.col
    }
    fn index_mut(&mut self) -> &mut Option<Vec<usize>> {
        &mut self.index
    }
    fn bitmap_mut(&mut self) -> &mut Option<Bitmap> {
        &mut self.bitmap
    }
}

pub struct RefColumn<V: 'static> {
    col: &'static V,
    index: &'static Option<Vec<usize>>,
    bitmap: &'static Option<Bitmap>,
}

impl<V> Column<V> for RefColumn<V> {
    fn col(&self) -> &V {
        self.col
    }
    fn index(&self) -> &Option<Vec<usize>> {
        self.index
    }
    fn bitmap(&self) -> &Option<Bitmap> {
        self.bitmap
    }
}

impl<V> RefColumn<V> {
    pub fn new<T>(
        col: &'static V,
        index: &'static Option<Vec<usize>>,
        bitmap: &'static Option<Bitmap>,
    ) -> Self
    where
        V: Deref<Target = [T]>,
    {
        //Validate that the bitmap and the data have the same length
        bitmap
            .iter()
            .for_each(|b| assert_eq!((*col).len(), b.len()));

        Self { col, index, bitmap }
    }
}
