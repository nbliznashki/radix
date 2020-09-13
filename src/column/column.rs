use crate::bitmap::*;
use core::any::Any;
use std::{any::TypeId, ops::Deref};
use std::{ops::DerefMut, sync::Arc};
pub trait Column<V> {
    fn col(&self) -> &V;
    fn index(&self) -> &Option<Vec<usize>>;
    fn bitmap(&self) -> &Option<Bitmap>;
}

enum ColumnData<'a> {
    Ref(&'a (dyn Any + Send + Sync)),
    RefMut(&'a mut (dyn Any + Send + Sync)),
    Owned(Arc<dyn Any + Send + Sync>),
}

pub struct ColumnWrapper<'a> {
    column: ColumnData<'a>,
    index: Option<Vec<usize>>,
    bitmap: Option<Bitmap>,
    typeid: TypeId,
    typename: String,
    name: Option<String>,
}

impl<'a> ColumnWrapper<'a> {
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
        let typename = std::any::type_name::<V>();
        Self {
            column: ColumnData::Owned(Arc::new(col)),
            index,
            bitmap,
            typeid,
            typename: typename.to_string(),
            name: None,
        }
    }

    pub fn new_ref<T, V>(col: &'a V, index: Option<Vec<usize>>, bitmap: Option<Bitmap>) -> Self
    where
        V: Deref<Target = [T]>,
        V: Send + Sync + 'static,
    {
        //Validate that the bitmap and the data have the same length
        bitmap
            .iter()
            .for_each(|b| assert_eq!((*col).len(), b.len()));

        let typeid = TypeId::of::<V>();
        let typename = std::any::type_name::<V>();
        Self {
            column: ColumnData::Ref(col),
            index,
            bitmap,
            typeid,
            typename: typename.to_string(),
            name: None,
        }
    }

    pub fn new_ref_mut<T, V>(
        col: &'a mut V,
        index: Option<Vec<usize>>,
        bitmap: Option<Bitmap>,
    ) -> Self
    where
        V: Deref<Target = [T]>,
        V: Send + Sync + 'static,
    {
        //Validate that the bitmap and the data have the same length
        bitmap
            .iter()
            .for_each(|b| assert_eq!((*col).len(), b.len()));

        let typeid = TypeId::of::<V>();
        let typename = std::any::type_name::<V>();
        Self {
            column: ColumnData::RefMut(col),
            index,
            bitmap,
            typeid,
            typename: typename.to_string(),
            name: None,
        }
    }
    pub fn with_name(mut self, name: &str) -> Self {
        self.name = Some(name.to_string());
        self
    }

    pub fn name(&self) -> &Option<String> {
        &self.name
    }

    pub fn typeid(&self) -> TypeId {
        self.typeid
    }

    pub fn typename(&self) -> &String {
        &self.typename
    }

    pub fn unwrap<V>(self) -> V
    where
        V: Send + Sync + 'static,
    {
        let (typename, col) = (self.typename, self.column);

        match col {
            ColumnData::Owned(col) => {
                match Arc::try_unwrap(col.downcast::<V>().unwrap_or_else(|_| {
                    panic!(
                        "Downcast failed. Source type is {}, target type is {}",
                        typename,
                        std::any::type_name::<V>()
                    )
                })) {
                    Ok(res) => res,
                    _ => panic!("Downcast of Arc failed due to non-exclusive reference"),
                }
            }
            _ => panic!("Cannot downcast a non-owned column to owned column"),
        }
    }

    pub fn downcast_mut<'b, V>(&'b mut self) -> &'b mut V
    where
        V: 'static,
        'a: 'b,
    {
        let (typename, col) = (&self.typename, &mut self.column);
        let col_mut_ref: &mut (dyn Any + Send + Sync) = match col {
            ColumnData::RefMut(col) => *col,
            ColumnData::Owned(col) => Arc::get_mut(col)
                .unwrap_or_else(|| panic!("Downcast of Arc failed due to non-exclusive reference")),
            _ => panic!("Cannot downcast a non-owned or non-ref mut column to ref mut column"),
        };

        col_mut_ref.downcast_mut::<V>().unwrap_or_else(|| {
            panic!(
                "Downcast failed. Source type is {}, target type is {}",
                typename,
                std::any::type_name::<V>()
            )
        })
    }

    pub fn downcast_ref<V>(&self) -> &V
    where
        V: 'static,
    {
        let (typename, col) = (&self.typename, &self.column);
        let col_ref: &(dyn Any + Send + Sync) = match col {
            ColumnData::Ref(col) => *col,
            ColumnData::RefMut(col) => *col,
            ColumnData::Owned(col) => &(**col),
        };

        col_ref.downcast_ref::<V>().unwrap_or_else(|| {
            panic!(
                "Downcast failed. Source type is {}, target type is {}",
                self.typename,
                std::any::type_name::<V>()
            )
        })
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

        let col_mut_ref: &mut (dyn Any + Send + Sync) = match col {
            ColumnData::RefMut(col) => *col,
            ColumnData::Owned(col) => Arc::get_mut(col)
                .unwrap_or_else(|| panic!("Downcast of Arc failed due to non-exclusive reference")),
            _ => panic!("Cannot downcast a non-owned or non-ref mut column to ref mut column"),
        };

        let col = col_mut_ref.downcast_mut::<V>().unwrap();
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
