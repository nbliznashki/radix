use crate::bitmap::*;
use core::any::Any;
use std::sync::Arc;
use std::{any::TypeId, ops::Deref};
pub trait Column<V> {
    fn col(&self) -> &V;
    fn index(&self) -> &Option<Vec<usize>>;
    fn bitmap(&self) -> &Option<Bitmap>;
}

pub struct ColumnWrapper {
    column: Arc<dyn Any + Send + Sync>,
    index: Option<Vec<usize>>,
    bitmap: Option<Bitmap>,
    typeid: TypeId,
    name: String,
}

impl ColumnWrapper {
    pub fn new<T, V>(col: V, index: Option<Vec<usize>>, bitmap: Option<Bitmap>) -> Self
    where
        V: Deref<Target = [T]>,
        V: Send + Sync + 'static,
    {
        //Validate that the bitmap and the data have the same length
        bitmap
            .iter()
            .for_each(|b| assert_eq!((*col).len(), b.len()));

        let typeid = TypeId::of::<V>();
        Self {
            column: Arc::new(col),
            index,
            bitmap,
            typeid,
            name: "".to_string(),
        }
    }

    pub fn new_with_name<T, V>(
        col: V,
        index: Option<Vec<usize>>,
        bitmap: Option<Bitmap>,
        name: &str,
    ) -> Self
    where
        V: Deref<Target = [T]>,
        V: Send + Sync + 'static,
    {
        let mut cw = ColumnWrapper::new(col, index, bitmap);
        cw.rename(name);
        cw
    }
    pub fn rename(&mut self, name: &str) {
        self.name = name.to_string()
    }

    pub fn name(&self) -> &String {
        &self.name
    }

    pub fn typeid(&self) -> TypeId {
        self.typeid
    }

    pub fn unwrap<V>(self) -> V
    where
        V: Send + Sync + 'static,
    {
        match Arc::try_unwrap(self.column.downcast::<V>().unwrap()) {
            Ok(res) => res,
            _ => panic!("Downcast of Arc failed due to non-exclusive reference"),
        }
    }

    pub fn downcast_ref<V>(&self) -> &V
    where
        V: 'static,
    {
        (*self.column).downcast_ref::<V>().unwrap()
    }

    pub fn downcast_mut<V>(&mut self) -> &mut V
    where
        V: 'static,
    {
        {
            (Arc::get_mut(&mut self.column).unwrap())
                .downcast_mut::<V>()
                .unwrap()
        }
    }
    pub fn index(&self) -> &Option<Vec<usize>> {
        &self.index
    }
    pub fn bitmap(&self) -> &Option<Bitmap> {
        &self.bitmap
    }

    pub fn index_mut(&mut self) -> &mut Option<Vec<usize>> {
        &mut self.index
    }
    pub fn bitmap_mut(&mut self) -> &mut Option<Bitmap> {
        &mut self.bitmap
    }
    pub fn all_mut<V>(&mut self) -> (&mut V, &mut Option<Vec<usize>>, &mut Option<Bitmap>)
    where
        V: 'static,
    {
        let (col, ind, bmap) = (&mut self.column, &mut self.index, &mut self.bitmap);
        let col = (Arc::get_mut(col).unwrap()).downcast_mut::<V>().unwrap();
        (col, ind, bmap)
    }
}

pub trait ColumnMut<V>: Column<V> {
    fn col_mut(&mut self) -> &mut V;
    fn index_mut(&mut self) -> &mut Option<Vec<usize>>;
    fn bitmap_mut(&mut self) -> &mut Option<Bitmap>;
    fn all_mut(&mut self) -> (&mut V, &mut Option<Vec<usize>>, &mut Option<Bitmap>);
}
#[derive(Clone)]
pub struct OwnedColumn<V> {
    col: V,
    index: Option<Vec<usize>>,
    bitmap: Option<Bitmap>,
    typeid: TypeId,
    name: String,
}

impl<V: 'static> OwnedColumn<V> {
    pub fn new<T>(col: V, index: Option<Vec<usize>>, bitmap: Option<Bitmap>) -> Self
    where
        V: Deref<Target = [T]>,
    {
        //Validate that the bitmap and the data have the same length
        bitmap
            .iter()
            .for_each(|b| assert_eq!((*col).len(), b.len()));

        let typeid = TypeId::of::<V>();
        Self {
            col,
            index,
            bitmap,
            typeid,
            name: "".to_string(),
        }
    }
    pub fn new_with_name<T>(
        col: V,
        index: Option<Vec<usize>>,
        bitmap: Option<Bitmap>,
        name: &str,
    ) -> Self
    where
        V: Deref<Target = [T]>,
    {
        //Validate that the bitmap and the data have the same length
        bitmap
            .iter()
            .for_each(|b| assert_eq!((*col).len(), b.len()));

        let typeid = TypeId::of::<V>();
        Self {
            col,
            index,
            bitmap,
            typeid,
            name: name.to_string(),
        }
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
    fn all_mut(&mut self) -> (&mut V, &mut Option<Vec<usize>>, &mut Option<Bitmap>) {
        (&mut self.col, &mut self.index, &mut self.bitmap)
    }
}
