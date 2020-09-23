use crate::{bitmap::*, ColumnIndex, ColumnU8, SliceMarker, SliceRef, SliceRefMut};
use core::any::Any;
use std::{any::TypeId, ops::Deref};

pub trait Column<V> {
    fn col(&self) -> &V;
    fn index(&self) -> &Option<Vec<usize>>;
    fn bitmap(&self) -> &Option<Bitmap>;
}

/// Source code copied from std::boxed::into_boxed_slice()
/// As of 13.09.2020, the feature is not stabilized. Tracking issue = "71582".
/// Converts a `Box<T>` into a `Box<[T]>`
///
/// This conversion does not allocate on the heap and happens in place.
///
fn copy_of_into_boxed_slice<T>(boxed: Box<T>) -> Box<[T]> {
    // *mut T and *mut [T; 1] have the same size and alignment
    unsafe { Box::from_raw(Box::into_raw(boxed) as *mut [T; 1]) }
}

#[derive(Debug)]
pub(crate) enum ColumnData<'a> {
    Ref(&'a (dyn Any + Send + Sync)),
    RefMut(&'a mut (dyn Any + Send + Sync)),
    Owned(Box<dyn Any + Send + Sync>),
    SliceRef(SliceRef<'a>),
    SliceRefMut(SliceRefMut<'a>),
}

#[derive(Debug)]
pub struct ColumnWrapper<'a> {
    column: ColumnData<'a>,
    index: ColumnIndex,
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
            column: ColumnData::Owned(Box::new(col)),
            index,
            bitmap,
            typeid,
            typename: typename.to_string(),
            name: None,
        }
    }

    pub fn new_ref<T, V>(col: &'a V, index: Option<Vec<usize>>, bitmap: Option<Bitmap>) -> Self
    where
        V: Send + Sync + 'static,
        V: Deref<Target = [T]>,
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

    /*
        pub(crate) fn new<T>(s: &'a [T]) -> Self
    where
        T: 'static + Sync,
    {
        SliceRef {
            type_id: std::any::TypeId::of::<[T]>(),
            len: s.len(),
            ptr: s.as_ptr() as *const u8,
            phantom: PhantomData,
        }
    }
    */
    pub fn new_slice<T>(col: &'a [T], index: Option<Vec<usize>>, bitmap: Option<Bitmap>) -> Self
    where
        T: 'static + Sync,
    {
        //Validate that the bitmap and the data have the same length

        let typeid = TypeId::of::<[T]>();
        let typename = std::any::type_name::<[T]>();
        Self {
            column: ColumnData::SliceRef(SliceRef::new::<T>(col)),
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
        V: Send + Sync + 'static,
        V: Deref<Target = [T]>,
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

    pub fn new_slice_mut<T>(
        col: &'a mut [T],
        index: Option<Vec<usize>>,
        bitmap: Option<Bitmap>,
    ) -> Self
    where
        T: 'static + Sync,
    {
        //Validate that the bitmap and the data have the same length

        let typeid = TypeId::of::<[T]>();
        let typename = std::any::type_name::<[T]>();
        Self {
            column: ColumnData::SliceRefMut(SliceRefMut::new::<T>(col)),
            index,
            bitmap,
            typeid,
            typename: typename.to_string(),
            name: None,
        }
    }

    pub fn new_ref_u8(
        col: &'a ColumnU8,
        index: Option<Vec<usize>>,
        bitmap: Option<Bitmap>,
    ) -> Self {
        //Validate that the bitmap and the data have the same length
        bitmap
            .iter()
            .for_each(|b| assert_eq!((*col).len.len(), b.len()));

        let typeid = TypeId::of::<ColumnU8>();
        let typename = std::any::type_name::<ColumnU8>();
        Self {
            column: ColumnData::Ref(col),
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
                let col = col as Box<dyn Any>;
                let col = col.downcast::<V>().unwrap_or_else(|_| {
                    panic!(
                        "Downcast failed. Source type is {}, target type is {}",
                        typename,
                        std::any::type_name::<V>()
                    )
                });
                let col = copy_of_into_boxed_slice(col);
                let mut res: Vec<V> = col.into();
                let res = res.pop().unwrap();
                res
            }
            _ => panic!("Cannot downcast a non-owned column to owned column"),
        }
    }

    pub fn downcast_mut<V>(&mut self) -> &mut V
    where
        V: 'static,
    {
        let (typename, col) = (&self.typename, &mut self.column);
        let col_mut_ref = match col {
            ColumnData::RefMut(col) => &mut **col,
            ColumnData::Owned(col) => &mut **col,
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
        let col_ref = match col {
            ColumnData::Ref(col) => &(**col),
            ColumnData::RefMut(col) => &(**col),
            ColumnData::Owned(col) => &(**col),
            _=>panic!("downcast_ref can only be used with Ref, RefMut, and Owned variants of ColumnWrapper")
        };

        col_ref.downcast_ref::<V>().unwrap_or_else(|| {
            panic!(
                "Downcast failed. Source type is {}, target type is {}",
                typename,
                std::any::type_name::<V>()
            )
        })
    }

    pub fn downcast_slice_ref<V: SliceMarker<V> + ?Sized>(
        &self,
    ) -> &[<V as SliceMarker<V>>::Element]
    where
        <V as SliceMarker<V>>::Element: 'static + Sync,
    {
        let (typename, col) = (&self.typename, &self.column);
        let col_ref_downcasted = match col {
            ColumnData::SliceRef(col) => col.downcast_ref::<V>(),
            ColumnData::SliceRefMut(col) => col.downcast_ref::<V>(),
            _=>panic!("downcast_slice_ref can only be used with SliceRef and SliceRefMut variants of ColumnWrapper")
        };

        col_ref_downcasted.unwrap_or_else(|| {
            panic!(
                "Slice downcast failed. Source type is {}, target type is {}",
                typename,
                std::any::type_name::<[<V as SliceMarker<V>>::Element]>()
            )
        })
    }

    pub fn downcast_slice_mut<V: SliceMarker<V> + ?Sized>(
        &mut self,
    ) -> &mut [<V as SliceMarker<V>>::Element]
    where
        <V as SliceMarker<V>>::Element: 'static + Sync,
    {
        let (typename, col) = (&mut self.typename, &mut self.column);
        let col_ref_downcasted = match  col {
            ColumnData::SliceRefMut(col) => col.downcast_mut::<V>(),
            _=>panic!("downcast_slice_ref can only be used with SliceRef and SliceRefMut variants of ColumnWrapper")
        };

        col_ref_downcasted.unwrap_or_else(|| {
            panic!(
                "Slice downcast failed. Source type is {}, target type is {}",
                typename,
                std::any::type_name::<[<V as SliceMarker<V>>::Element]>()
            )
        })
    }

    pub fn slice_all_mut<V: SliceMarker<V> + ?Sized>(
        &mut self,
    ) -> (
        &mut [<V as SliceMarker<V>>::Element],
        &mut Option<Vec<usize>>,
        &mut Option<Bitmap>,
    )
    where
        <V as SliceMarker<V>>::Element: 'static + Sync,
    {
        let (col, ind, bmap, typename, _typeid) = (
            &mut self.column,
            &mut self.index,
            &mut self.bitmap,
            &self.typename,
            &self.typeid,
        );

        let col_ref_downcasted = match col {
            ColumnData::SliceRefMut(col) => col.downcast_mut::<V>(),
            _=>panic!("downcast_slice_ref can only be used with SliceRef and SliceRefMut variants of ColumnWrapper")
        };

        let col = col_ref_downcasted.unwrap_or_else(|| {
            panic!(
                "Slice downcast failed. Source type is {}, target type is {}",
                typename,
                std::any::type_name::<[<V as SliceMarker<V>>::Element]>(),
            )
        });
        (col, ind, bmap)
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
        let (col, ind, bmap, typename, _typeid) = (
            &mut self.column,
            &mut self.index,
            &mut self.bitmap,
            &self.typename,
            &self.typeid,
        );

        let col_mut_ref = match col {
            ColumnData::RefMut(col) => &mut (**col),
            ColumnData::Owned(col) => &mut (**col),

            _ => panic!("Cannot downcast a non-owned or non-ref mut column to ref mut column"),
        };

        let col = col_mut_ref.downcast_mut::<V>().unwrap_or_else(|| {
            panic!(
                "Downcast failed. Source type is {}, target type is {}",
                typename,
                std::any::type_name::<V>(),
            )
        });
        (col, ind, bmap)
    }

    pub fn all_unwrap<V>(self) -> (V, Option<Vec<usize>>, Option<Bitmap>)
    where
        V: 'static,
    {
        let (col, ind, bmap, typename) = (self.column, self.index, self.bitmap, self.typename);

        match col {
            ColumnData::Owned(col) => {
                let col = col as Box<dyn Any>;
                let col = col.downcast::<V>().unwrap_or_else(|_| {
                    panic!(
                        "Downcast failed. Source type is {}, target type is {}",
                        typename,
                        std::any::type_name::<V>()
                    )
                });
                let col = copy_of_into_boxed_slice(col);
                let mut res: Vec<V> = col.into();
                let res = res.pop().unwrap();
                (res, ind, bmap)
            }
            _ => panic!("Cannot downcast a non-owned column to owned column"),
        }
    }

    ///Applies a sub-index to a column.
    ///Let's say we have a column with an index, and we want to iterate
    ///Over it using a different index, e.g.
    ///Originally:
    ///data=["a", "b", "c"], index=[0,0,1] --> Output: "a", "a", "b"
    ///However, now we want to iterate over the output, and only take the 0th and 2nd item:
    ///subindex=[0,2]--> Output: "a", "b"
    ///This function replaces the orginal index [0,0,1] with a new index [0,1].
    ///The orginial index is returned.
    ///This functionality is needed for joining tables, where we need a mechanism to index
    ///an already indexed column for a second time.

    pub fn re_index(&mut self, index: &Vec<usize>) -> ColumnIndex {
        let current_index = self.index_mut().take();

        let v = match &current_index {
            Some(cur_ind) => index.iter().map(|i| cur_ind[*i]).collect(),
            None => index.clone(),
        };
        *self.index_mut() = Some(v);
        current_index
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
