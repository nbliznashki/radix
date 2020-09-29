use crate::{
    bitmap::*, ColumnIndex, ColumnIndexRef, ColumnU8, Dictionary, Signature, SliceMarker, SliceRef,
    SliceRefMut,
};
use core::any::Any;
use std::{any::TypeId, ops::Deref};

pub type ErrorDesc = Box<dyn std::error::Error>;

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
pub(crate) enum IndexData<'a> {
    Ref(&'a ColumnIndex),
    RefMut(&'a mut ColumnIndex),
    Owned(ColumnIndex),
    SliceRef(&'a [usize]),
}

impl<'a> IndexData<'a> {
    fn to_slice<'b>(&'b self) -> ColumnIndexRef<'b>
    where
        'a: 'b,
    {
        match self {
            IndexData::Ref(None) => None,
            IndexData::RefMut(None) => None,
            IndexData::Owned(None) => None,
            IndexData::Ref(Some(s)) => Some(s),
            IndexData::RefMut(Some(s)) => Some(s),
            IndexData::Owned(Some(s)) => Some(s),
            IndexData::SliceRef(s) => Some(*s),
        }
    }
    fn to_vec_mut(&mut self) -> &mut ColumnIndex {
        match self {
            IndexData::SliceRef(s) => {
                *self = IndexData::Owned(Some(s.to_vec()));
                match self {
                    IndexData::Owned(v) => v,
                    _ => unreachable!(),
                }
            }
            IndexData::Ref(s) => {
                *self = IndexData::Owned(s.clone());
                match self {
                    IndexData::Owned(v) => v,
                    _ => unreachable!(),
                }
            }
            IndexData::RefMut(s) => s,
            IndexData::Owned(s) => s,
        }
    }
    fn into_owned(self) -> ColumnIndex {
        match self {
            IndexData::SliceRef(s) => Some(s.to_vec()),
            IndexData::Ref(s) => s.clone(),
            IndexData::RefMut(s) => s.clone(),
            IndexData::Owned(s) => s,
        }
    }
}
#[derive(Debug)]
pub(crate) enum BitmapData<'a> {
    Ref(&'a Option<Bitmap>),
    RefMut(&'a mut Option<Bitmap>),
    Owned(Option<Bitmap>),
    SliceRef(&'a [u8]),
    SliceRefMut(&'a mut [u8]),
}

impl<'a> BitmapData<'a> {
    fn to_slice<'b>(&'b self) -> Option<&'b [u8]>
    where
        'a: 'b,
    {
        match self {
            BitmapData::Ref(None) => None,
            BitmapData::RefMut(None) => None,
            BitmapData::Owned(None) => None,
            BitmapData::Ref(Some(s)) => Some(s.bits.as_slice()),
            BitmapData::RefMut(Some(s)) => Some(s.bits.as_slice()),
            BitmapData::Owned(Some(s)) => Some(s.bits.as_slice()),
            BitmapData::SliceRef(s) => Some(*s),
            BitmapData::SliceRefMut(s) => Some(*s),
        }
    }
    fn to_vec_mut(&mut self) -> &mut Option<Bitmap> {
        match self {
            BitmapData::Ref(s) => {
                *self = BitmapData::Owned(s.clone());
                match self {
                    BitmapData::Owned(v) => v,
                    _ => unreachable!(),
                }
            }
            BitmapData::RefMut(s) => s,
            BitmapData::Owned(s) => s,
            BitmapData::SliceRef(s) => {
                *self = BitmapData::Owned(Some(Bitmap::from(s.to_vec())));
                match self {
                    BitmapData::Owned(v) => v,
                    _ => unreachable!(),
                }
            }
            BitmapData::SliceRefMut(s) => {
                *self = BitmapData::Owned(Some(Bitmap::from(s.to_vec())));
                match self {
                    BitmapData::Owned(v) => v,
                    _ => unreachable!(),
                }
            }
        }
    }
    fn into_owned(self) -> Option<Bitmap> {
        match self {
            BitmapData::Ref(s) => s.clone(),
            BitmapData::RefMut(s) => s.clone(),
            BitmapData::Owned(s) => s,
            BitmapData::SliceRef(s) => Some(Bitmap::from(s.to_vec())),
            BitmapData::SliceRefMut(s) => Some(Bitmap::from(s.to_vec())),
        }
    }
}

#[derive(Debug)]
pub struct ColumnWrapper<'a> {
    column: ColumnData<'a>,
    index: IndexData<'a>,
    bitmap: BitmapData<'a>,
    typeid: TypeId,
    typename: String,
    name: Option<String>,
}

impl<'a> ColumnWrapper<'a> {
    pub(crate) fn copy_inner_as_ref<'b>(&'b self) -> Result<ColumnWrapper<'b>, ErrorDesc>
    where
        'a: 'b,
    {
        let col = match &self.column {
            ColumnData::Ref(col) => Some(*col),
            ColumnData::RefMut(col) => Some(&(**col)),
            ColumnData::Owned(col) => Some(&(**col)),
            _ => None,
        };

        if let Some(col) = col {
            let bitmap = match self.bitmap() {
                None => BitmapData::Owned(None),
                Some(s) => BitmapData::SliceRef(s),
            };

            Ok(ColumnWrapper {
                column: ColumnData::Ref(col),
                index: IndexData::Owned(None),
                bitmap,
                typeid: self.typeid,
                typename: self.typename.clone(),
                name: None,
            })
        } else {
            Err("Only Ref, RefMut and Owned variants of ColumnData support representing the inner data as a reference")?
        }
    }

    pub fn new<T, V>(col: V) -> Self
    where
        V: Deref<Target = [T]>,
        V: Send + Sync + 'static,
    {
        let typeid = TypeId::of::<V>();
        let typename = std::any::type_name::<V>();
        Self {
            column: ColumnData::Owned(Box::new(col)),
            index: IndexData::Owned(None),
            bitmap: BitmapData::Owned(None),
            typeid,
            typename: typename.to_string(),
            name: None,
        }
    }

    pub fn new_ref<T, V>(col: &'a V) -> Self
    where
        V: Send + Sync + 'static,
        V: Deref<Target = [T]>,
    {
        let typeid = TypeId::of::<V>();
        let typename = std::any::type_name::<V>();
        Self {
            column: ColumnData::Ref(col),
            index: IndexData::Owned(None),
            bitmap: BitmapData::Owned(None),
            typeid,
            typename: typename.to_string(),
            name: None,
        }
    }

    pub fn new_ref_mut<T, V>(col: &'a mut V) -> Self
    where
        V: Send + Sync + 'static,
        V: Deref<Target = [T]>,
    {
        let typeid = TypeId::of::<V>();
        let typename = std::any::type_name::<V>();
        Self {
            column: ColumnData::RefMut(col),
            index: IndexData::Owned(None),
            bitmap: BitmapData::Owned(None),
            typeid,
            typename: typename.to_string(),
            name: None,
        }
    }

    pub fn new_slice<T>(col: &'a [T]) -> Self
    where
        T: 'static + Sync,
    {
        let typeid = TypeId::of::<[T]>();
        let typename = std::any::type_name::<[T]>();
        Self {
            column: ColumnData::SliceRef(SliceRef::new::<T>(col)),
            index: IndexData::Owned(None),
            bitmap: BitmapData::Owned(None),
            typeid,
            typename: typename.to_string(),
            name: None,
        }
    }

    pub fn new_slice_mut<T>(col: &'a mut [T]) -> Self
    where
        T: 'static + Sync,
    {
        //Validate that the bitmap and the data have the same length

        let typeid = TypeId::of::<[T]>();
        let typename = std::any::type_name::<[T]>();
        Self {
            column: ColumnData::SliceRefMut(SliceRefMut::new::<T>(col)),
            index: IndexData::Owned(None),
            bitmap: BitmapData::Owned(None),
            typeid,
            typename: typename.to_string(),
            name: None,
        }
    }

    pub fn new_ref_u8(col: &'a ColumnU8) -> Self {
        let typeid = TypeId::of::<ColumnU8>();
        let typename = std::any::type_name::<ColumnU8>();
        Self {
            column: ColumnData::Ref(col),
            index: IndexData::Owned(None),
            bitmap: BitmapData::Owned(None),
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
    pub fn with_index(mut self, i: ColumnIndex) -> Self {
        self.index = IndexData::Owned(i);
        self
    }

    pub fn with_index_ref(mut self, i: &'a ColumnIndex) -> Self {
        self.index = IndexData::Ref(i);
        self
    }

    pub fn with_index_ref_mut(mut self, i: &'a mut ColumnIndex) -> Self {
        self.index = IndexData::RefMut(i);
        self
    }

    pub fn with_index_slice(mut self, i: &'a [usize]) -> Self {
        self.index = IndexData::SliceRef(i);
        self
    }

    pub fn with_bitmap(mut self, b: Option<Bitmap>) -> Self {
        self.bitmap = BitmapData::Owned(b);
        self
    }

    pub fn with_bitmap_ref(mut self, b: &'a Option<Bitmap>) -> Self {
        self.bitmap = BitmapData::Ref(b);
        self
    }

    pub fn with_bitmap_ref_mut(mut self, b: &'a mut Option<Bitmap>) -> Self {
        self.bitmap = BitmapData::RefMut(b);
        self
    }

    pub fn with_bitmap_slice(mut self, b: &'a [u8]) -> Self {
        self.bitmap = BitmapData::SliceRef(b);
        self
    }

    pub fn with_bitmap_slice_mut(mut self, b: &'a mut [u8]) -> Self {
        self.bitmap = BitmapData::SliceRefMut(b);
        self
    }

    pub fn index(&self) -> ColumnIndexRef {
        self.index.to_slice()
    }
    pub fn bitmap(&self) -> Option<&[u8]> {
        self.bitmap.to_slice()
    }

    pub fn index_mut(&mut self) -> &mut Option<Vec<usize>> {
        self.index.to_vec_mut()
    }
    pub fn bitmap_mut(&mut self) -> &mut Option<Bitmap> {
        self.bitmap.to_vec_mut()
    }

    pub fn unwrap<V>(self) -> Result<V, ErrorDesc>
    where
        V: Send + Sync + 'static,
    {
        let (typename, col) = (self.typename, self.column);

        match col {
            ColumnData::Owned(col) => {
                let col = col as Box<dyn Any>;
                let col = col.downcast::<V>().map_err(|_| {
                    format!(
                        "Downcast failed. Source type is {}, target type is {}",
                        typename,
                        std::any::type_name::<V>()
                    )
                })?;

                let col = copy_of_into_boxed_slice(col);
                let mut res: Vec<V> = col.into();
                //Should never fail
                let res = res.pop().unwrap();
                Ok(res)
            }
            _ => Err("Cannot downcast a non-owned column to owned column")?,
        }
    }

    pub fn downcast_mut<V>(&mut self) -> Result<&mut V, ErrorDesc>
    where
        V: 'static,
    {
        let (typename, col) = (&self.typename, &mut self.column);
        let col_mut_ref = match col {
            ColumnData::RefMut(col) => Some(&mut **col),
            ColumnData::Owned(col) => Some(&mut **col),
            _ => None,
        };
        if let Some(col_mut_ref) = col_mut_ref {
            let col = col_mut_ref.downcast_mut::<V>();
            if let Some(col) = col {
                Ok(col)
            } else {
                Err(format!(
                    "Downcast failed. Source type is {}, target type is {}",
                    typename,
                    std::any::type_name::<V>()
                ))?
            }
        } else {
            Err("Cannot downcast a non-owned or non-ref mut column to ref mut column")?
        }
    }

    pub fn downcast_ref<V>(&self) -> Result<&V, ErrorDesc>
    where
        V: 'static,
    {
        let (typename, col) = (&self.typename, &self.column);
        let col_ref = match col {
            ColumnData::Ref(col) => Some(&(**col)),
            ColumnData::RefMut(col) => Some(&(**col)),
            ColumnData::Owned(col) => Some(&(**col)),
            _ => None,
        };

        if let Some(col_ref) = col_ref {
            if let Some(col) = col_ref.downcast_ref::<V>() {
                Ok(col)
            } else {
                Err(format!(
                    "Downcast failed. Source type is {}, target type is {}",
                    typename,
                    std::any::type_name::<V>()
                ))?
            }
        } else {
            Err("downcast_ref can only be used with Ref, RefMut, and Owned variants of ColumnWrapper")?
        }
    }

    pub fn downcast_slice_ref<V: SliceMarker<V> + ?Sized>(
        &self,
    ) -> Result<&[<V as SliceMarker<V>>::Element], ErrorDesc>
    where
        <V as SliceMarker<V>>::Element: 'static + Sync,
    {
        let (typename, col) = (&self.typename, &self.column);
        let col_ref_downcasted = match col {
            ColumnData::SliceRef(col) => Some(col.downcast_ref::<V>()),
            ColumnData::SliceRefMut(col) => Some(col.downcast_ref::<V>()),
            _ => None,
        };

        if let Some(col_ref_downcasted) = col_ref_downcasted {
            if let Some(col) = col_ref_downcasted {
                Ok(col)
            } else {
                Err(format!(
                    "Slice downcast failed. Source type is {}, target type is {}",
                    typename,
                    std::any::type_name::<[<V as SliceMarker<V>>::Element]>()
                ))?
            }
        } else {
            Err("downcast_slice_ref can only be used with SliceRef and SliceRefMut variants of ColumnWrapper")?
        }
    }

    pub fn downcast_slice_mut<V: SliceMarker<V> + ?Sized>(
        &mut self,
    ) -> Result<&mut [<V as SliceMarker<V>>::Element], ErrorDesc>
    where
        <V as SliceMarker<V>>::Element: 'static + Sync,
    {
        let (typename, col) = (&mut self.typename, &mut self.column);

        if let ColumnData::SliceRefMut(col) = col {
            let col_ref_downcasted = col.downcast_mut::<V>();
            if let Some(col) = col_ref_downcasted {
                Ok(col)
            } else {
                Err(format!(
                    "Slice downcast failed. Source type is {}, target type is {}",
                    typename,
                    std::any::type_name::<[<V as SliceMarker<V>>::Element]>()
                ))?
            }
        } else {
            Err("downcast_slice_mut can only be used withSliceRefMut variant of ColumnWrapper")?
        }
    }

    pub fn slice_all_mut<V: SliceMarker<V> + ?Sized>(
        &mut self,
    ) -> Result<
        (
            &mut [<V as SliceMarker<V>>::Element],
            &mut Option<Vec<usize>>,
            &mut Option<Bitmap>,
        ),
        ErrorDesc,
    >
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

        if let ColumnData::SliceRefMut(col) = col {
            let col_ref_downcasted = col.downcast_mut::<V>();
            if let Some(col) = col_ref_downcasted {
                Ok((col, ind.to_vec_mut(), bmap.to_vec_mut()))
            } else {
                Err(format!(
                    "Slice downcast failed. Source type is {}, target type is {}",
                    typename,
                    std::any::type_name::<[<V as SliceMarker<V>>::Element]>(),
                ))?
            }
        } else {
            Err("downcast_slice_ref can only be used with SliceRef and SliceRefMut variants of ColumnWrapper".to_string())?
        }
    }

    pub fn all_mut<V>(
        &mut self,
    ) -> Result<(&mut V, &mut Option<Vec<usize>>, &mut Option<Bitmap>), ErrorDesc>
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

        match col {
            ColumnData::RefMut(col) => {
                let col_mut_ref = (&mut (**col)).downcast_mut::<V>();
                match col_mut_ref {
                    Some(col) => Ok((col, ind.to_vec_mut(), bmap.to_vec_mut())),
                    None => Err(format!(
                        "Downcast failed. Source type is {}, target type is {}",
                        typename,
                        std::any::type_name::<V>(),
                    ))?,
                }
            }
            ColumnData::Owned(col) => {
                let col_mut_ref = (&mut (**col)).downcast_mut::<V>();
                match col_mut_ref {
                    Some(col) => Ok((col, ind.to_vec_mut(), bmap.to_vec_mut())),
                    None => Err(format!(
                        "Downcast failed. Source type is {}, target type is {}",
                        typename,
                        std::any::type_name::<V>(),
                    ))?,
                }
            }
            _ => Err("Cannot downcast a non-owned or non-ref mut column to ref mut column")?,
        }
    }

    pub fn all_unwrap<V>(self) -> Result<(V, Option<Vec<usize>>, Option<Bitmap>), ErrorDesc>
    where
        V: 'static,
    {
        let (col, ind, bmap, typename) = (self.column, self.index, self.bitmap, self.typename);

        match col {
            ColumnData::Owned(col) => {
                let col = col as Box<dyn Any>;
                let col = col.downcast::<V>().map_err(|_| {
                    format!(
                        "Downcast failed. Source type is {}, target type is {}",
                        typename,
                        std::any::type_name::<V>()
                    )
                })?;

                let col = copy_of_into_boxed_slice(col);
                let mut res: Vec<V> = col.into();
                //Should never fail
                let res = res.pop().unwrap();
                Ok((res, ind.into_owned(), bmap.into_owned()))
            }
            _ => Err("Cannot downcast a non-owned column to owned column")?,
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
    ///Returns the internal length of the data stored in the column
    ///without taking into consideration the index
    /// ```
    ///use radix::*;
    ///let col4: Vec<u64> = vec![1, 5, 6, 4, 5, 6, 4, 5, 6, 8];
    ///let dict: Dictionary = Dictionary::new();
    ///
    ///let len_orig = col4.len();
    ///let mut col4 = ColumnWrapper::new(col4);
    ///let len_data = col4.len_data(&dict);
    ///assert_eq!(len_orig, len_data.unwrap());
    ///```
    pub fn len_data(&self, dict: &Dictionary) -> Result<usize, ErrorDesc> {
        let signature = Signature::new("len", vec![self.typeid], vec![self.typename.clone()]);
        let len_data = dict.len_data.get(&signature);
        if let Some(len_data) = len_data {
            len_data(&self)
        } else {
            Err(format!(
                "Following operation not found in dictionary: {:?}",
                signature
            ))?
        }
    }
    ///Returns the internal length of the data stored in the column,
    ///taking into consideration the index
    /// ```
    ///use radix::*;
    ///let col4: Vec<u64> = vec![1, 5, 6, 4, 5, 6, 4, 5, 6, 8];
    ///let dict: Dictionary = Dictionary::new();
    ///
    ///let mut col4 = ColumnWrapper::new(col4).with_index(Some(vec![0,0,1]));
    ///let len_data = col4.len(&dict);
    ///assert_eq!(3, len_data.unwrap());
    ///```
    pub fn len(&self, dict: &Dictionary) -> Result<usize, ErrorDesc> {
        match self.index() {
            Some(ind) => Ok(ind.len()),
            None => self.len_data(dict),
        }
    }

    pub fn part(
        &self,
        chunk_size: usize,
        dict: &Dictionary,
    ) -> Result<Vec<ColumnWrapper>, ErrorDesc> {
        let signature = Signature::new("part", vec![self.typeid], vec![self.typename.clone()]);
        if let Some(op) = dict.part.get(&signature) {
            op.part(self, chunk_size)
        } else {
            Err(format!(
                "Following operation not found in dictionary: {:?}",
                signature
            ))?
        }
    }

    pub fn part_mut(
        &mut self,
        chunk_size: usize,
        dict: &Dictionary,
    ) -> Result<Vec<ColumnWrapper>, ErrorDesc> {
        let signature = Signature::new("part", vec![self.typeid], vec![self.typename.clone()]);

        if let Some(op) = dict.part.get(&signature) {
            op.part_mut(self, chunk_size)
        } else {
            Err(format!(
                "Following operation not found in dictionary: {:?}",
                signature
            ))?
        }
    }

    pub fn part_with_sizes(
        &self,
        chunks_size: &Vec<usize>,
        dict: &Dictionary,
    ) -> Result<Vec<ColumnWrapper>, ErrorDesc> {
        let signature = Signature::new("part", vec![self.typeid], vec![self.typename.clone()]);
        if let Some(op) = dict.part.get(&signature) {
            op.part_with_sizes(self, &chunks_size)
        } else {
            Err(format!(
                "Following operation not found in dictionary: {:?}",
                signature
            ))?
        }
    }

    pub fn part_with_sizes_mut(
        &mut self,
        chunks_size: &Vec<usize>,
        dict: &Dictionary,
    ) -> Result<Vec<ColumnWrapper>, ErrorDesc> {
        let signature = Signature::new("part", vec![self.typeid], vec![self.typename.clone()]);
        if let Some(op) = dict.part.get(&signature) {
            op.part_with_sizes_mut(self, &chunks_size)
        } else {
            Err(format!(
                "Following operation not found in dictionary: {:?}",
                signature
            ))?
        }
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
