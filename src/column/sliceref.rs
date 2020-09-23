use std::marker::PhantomData;

#[derive(Debug)]
pub(crate) struct SliceRef<'a> {
    type_id: std::any::TypeId,
    len: usize,
    ptr: *const u8,
    phantom: std::marker::PhantomData<&'a u8>,
}

pub unsafe trait SliceMarker<V: ?Sized> {
    type Element;
}

unsafe impl<T: 'static + Sync> SliceMarker<[T]> for [T] {
    type Element = T;
}

impl<'a> SliceRef<'a> {
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
    pub(crate) fn is<V: SliceMarker<V> + ?Sized>(&self) -> bool
    where
        <V as SliceMarker<V>>::Element: 'static + Sync,
    {
        // Get `TypeId` of the type this function is instantiated with.
        let t = std::any::TypeId::of::<[<V as SliceMarker<V>>::Element]>();

        // Get `TypeId` of the type in the trait object (`self`).
        let concrete = self.type_id;

        // Compare both `TypeId`s on equality.
        t == concrete
    }
    pub(crate) fn downcast_ref<V: SliceMarker<V> + ?Sized>(
        &self,
    ) -> Option<&[<V as SliceMarker<V>>::Element]>
    where
        <V as SliceMarker<V>>::Element: 'static + Sync,
    {
        if self.is::<V>() {
            let ptr = self.ptr;
            let len = self.len;
            // SAFETY: just checked whether we are pointing to the correct type, and we can rely on
            // that check for memory safety because we have implemented Any for all types; no other
            // impls can exist as they would conflict with our impl.
            unsafe {
                let ptr = ptr as *const <V as SliceMarker<V>>::Element;
                Some(std::slice::from_raw_parts(ptr, len))
            }
        } else {
            None
        }
    }
}

unsafe impl<'a> Sync for SliceRef<'a> {}
unsafe impl<'a> Sync for SliceRefMut<'a> {}

#[derive(Debug)]
pub(crate) struct SliceRefMut<'a> {
    type_id: std::any::TypeId,
    len: usize,
    ptr: *mut u8,
    phantom: std::marker::PhantomData<&'a u8>,
}

impl<'a> SliceRefMut<'a> {
    pub(crate) fn new<T>(s: &'a mut [T]) -> Self
    where
        T: 'static + Sync,
    {
        SliceRefMut {
            type_id: std::any::TypeId::of::<[T]>(),
            len: s.len(),
            ptr: s.as_mut_ptr() as *mut u8,
            phantom: PhantomData,
        }
    }
    pub(crate) fn is<V: SliceMarker<V> + ?Sized>(&self) -> bool
    where
        <V as SliceMarker<V>>::Element: 'static + Sync,
    {
        // Get `TypeId` of the type this function is instantiated with.
        let t = std::any::TypeId::of::<[<V as SliceMarker<V>>::Element]>();

        // Get `TypeId` of the type in the trait object (`self`).
        let concrete = self.type_id;

        // Compare both `TypeId`s on equality.
        t == concrete
    }
    pub(crate) fn downcast_mut<V: SliceMarker<V> + ?Sized>(
        &mut self,
    ) -> Option<&mut [<V as SliceMarker<V>>::Element]>
    where
        <V as SliceMarker<V>>::Element: 'static + Sync,
    {
        if self.is::<V>() {
            let ptr = self.ptr;
            let len = self.len;
            // SAFETY: just checked whether we are pointing to the correct type, and we can rely on
            // that check for memory safety because we have implemented Any for all types; no other
            // impls can exist as they would conflict with our impl.
            unsafe {
                let ptr = ptr as *mut <V as SliceMarker<V>>::Element;
                Some(std::slice::from_raw_parts_mut(ptr, len))
            }
        } else {
            None
        }
    }
    pub(crate) fn downcast_ref<V: SliceMarker<V> + ?Sized>(
        &self,
    ) -> Option<&[<V as SliceMarker<V>>::Element]>
    where
        <V as SliceMarker<V>>::Element: 'static + Sync,
    {
        if self.is::<V>() {
            let ptr = self.ptr;
            let len = self.len;
            // SAFETY: just checked whether we are pointing to the correct type, and we can rely on
            // that check for memory safety because we have implemented Any for all types; no other
            // impls can exist as they would conflict with our impl.
            unsafe {
                let ptr = ptr as *const <V as SliceMarker<V>>::Element;
                Some(std::slice::from_raw_parts(ptr, len))
            }
        } else {
            None
        }
    }
}
