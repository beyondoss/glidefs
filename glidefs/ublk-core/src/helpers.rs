use std::ops::{Deref, DerefMut};
use std::panic::{RefUnwindSafe, UnwindSafe};
use std::sync::atomic::{AtomicBool, Ordering};

pub fn type_of_this<T>(_: &T) -> String {
    std::any::type_name::<T>().to_string()
}

/// Slice like buffer, which address is aligned with 4096.
///
pub struct IoBuf<T> {
    ptr: *mut T,
    size: usize,
    mlocked: AtomicBool,
}

// SAFETY: `IoBuf<T>` owns a single heap allocation it allocated itself and
// frees on drop; the raw `ptr` is never aliased. It is therefore safe to move
// across threads (`Send`) and share by `&` (`Sync`) exactly when the contained
// `T` is itself `Send`/`Sync` — we bound on `T` rather than blanket-impl so an
// `IoBuf<Rc<_>>` does not silently become thread-safe. `mlocked` is an
// `AtomicBool`, so `mlock`/`munlock` taking `&self` mutate it race-free even
// when the same `IoBuf` is shared across threads — it does not widen these
// guarantees.
unsafe impl<T: Send> Send for IoBuf<T> {}
unsafe impl<T: Sync> Sync for IoBuf<T> {}

// `AtomicBool` is already `RefUnwindSafe`/`UnwindSafe`, but the raw `ptr` is
// not. The pointer only ever addresses a self-owned allocation and is not
// observable in a torn state across a panic boundary, so asserting unwind
// safety here is sound and lets `IoBuf` cross `catch_unwind`.
impl<T> RefUnwindSafe for IoBuf<T> {}
impl<T> UnwindSafe for IoBuf<T> {}

impl<T> IoBuf<T> {
    pub fn new(size: usize) -> Self {
        // `alloc` with a zero-sized layout is undefined behavior, so reject
        // it before allocating rather than after.
        assert!(size != 0, "IoBuf size must be non-zero");

        let layout = std::alloc::Layout::from_size_align(size, 4096).unwrap();
        // SAFETY: `layout` has a non-zero size (asserted above).
        let ptr = unsafe { std::alloc::alloc(layout) } as *mut T;

        // `alloc` returns null on allocation failure; dereferencing that null
        // later (Deref, zero_buf, as_slice, …) would be UB, so abort cleanly.
        if ptr.is_null() {
            std::alloc::handle_alloc_error(layout);
        }

        IoBuf {
            ptr,
            size,
            mlocked: AtomicBool::new(false),
        }
    }

    /// Check if the buffer is currently locked in memory
    pub fn is_mlocked(&self) -> bool {
        self.mlocked.load(Ordering::Relaxed)
    }

    /// Lock the buffer in memory using mlock
    /// Returns true if successful, false otherwise
    pub fn mlock(&self) -> bool {
        if self.mlocked.load(Ordering::Relaxed) {
            return true; // Already locked
        }

        let mlock_result = unsafe { libc::mlock(self.ptr as *const libc::c_void, self.size) };

        if mlock_result == 0 {
            self.mlocked.store(true, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// Unlock the buffer from memory using munlock
    /// Returns true if successful, false otherwise
    pub fn munlock(&self) -> bool {
        if !self.mlocked.load(Ordering::Relaxed) {
            return true; // Already unlocked
        }

        let munlock_result = unsafe { libc::munlock(self.ptr as *const libc::c_void, self.size) };

        if munlock_result == 0 {
            self.mlocked.store(false, Ordering::Relaxed);
            true
        } else {
            false
        }
    }

    /// how many elements in this buffer
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        let elem_size = core::mem::size_of::<T>();
        self.size / elem_size
    }

    /// Return raw address of this buffer
    pub fn as_ptr(&self) -> *const T {
        self.ptr
    }

    /// Return mutable raw address of this buffer
    pub fn as_mut_ptr(&self) -> *mut T {
        self.ptr
    }

    /// fill zero for every bits of this buffer
    pub fn zero_buf(&mut self) {
        unsafe {
            std::ptr::write_bytes(self.as_mut_ptr(), 0, self.len());
        }
    }

    /// Get a safe immutable slice reference to the buffer contents.
    ///
    /// This method provides safe slice access by leveraging the existing Deref
    /// implementation, eliminating the need for unsafe raw pointer operations.
    /// The returned slice is guaranteed to be valid for the lifetime of the IoBuf.
    ///
    /// # Safety Benefits
    /// - Bounds checking through slice operations
    /// - Compile-time lifetime verification
    /// - No manual pointer arithmetic required
    /// - Rust's memory safety guarantees apply
    pub fn as_slice(&self) -> &[T] {
        &*self
    }

    /// Get a safe mutable slice reference to the buffer contents.
    ///
    /// This method provides safe mutable slice access by leveraging the existing
    /// DerefMut implementation, eliminating the need for unsafe raw pointer operations.
    /// The returned slice is guaranteed to be valid for the lifetime of the IoBuf.
    ///
    /// # Safety Benefits
    /// - Bounds checking through slice operations  
    /// - Compile-time lifetime verification
    /// - No manual pointer arithmetic required
    /// - Rust's memory safety guarantees apply
    pub fn as_mut_slice(&mut self) -> &mut [T] {
        &mut *self
    }

    /// Get a safe immutable subslice of the buffer.
    ///
    /// This method provides safe access to a portion of the buffer by leveraging
    /// the existing Deref implementation and standard slice indexing. This eliminates
    /// the need for unsafe pointer arithmetic and provides automatic bounds checking.
    ///
    /// # Arguments
    /// * `range` - A range specifying the subslice bounds (e.g., `0..10`, `5..`, `..20`)
    ///
    /// # Panics
    /// Panics if the range is out of bounds, following standard slice behavior.
    ///
    /// # Safety Benefits
    /// - Automatic bounds checking
    /// - No unsafe pointer operations
    /// - Leverages Rust's slice safety guarantees
    pub fn subslice<R>(&self, range: R) -> &[T]
    where
        R: std::slice::SliceIndex<[T], Output = [T]>,
    {
        &self[range]
    }

    /// Get a safe mutable subslice of the buffer.
    ///
    /// This method provides safe mutable access to a portion of the buffer by leveraging
    /// the existing DerefMut implementation and standard slice indexing. This eliminates
    /// the need for unsafe pointer arithmetic and provides automatic bounds checking.
    ///
    /// # Arguments
    /// * `range` - A range specifying the subslice bounds (e.g., `0..10`, `5..`, `..20`)
    ///
    /// # Panics
    /// Panics if the range is out of bounds, following standard slice behavior.
    ///
    /// # Safety Benefits
    /// - Automatic bounds checking
    /// - No unsafe pointer operations  
    /// - Leverages Rust's slice safety guarantees
    pub fn subslice_mut<R>(&mut self, range: R) -> &mut [T]
    where
        R: std::slice::SliceIndex<[T], Output = [T]>,
    {
        &mut self[range]
    }
}

impl<T> std::fmt::Debug for IoBuf<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ptr {:?} size {} element type {}",
            self.ptr,
            self.size,
            type_of_this(unsafe { &*self.ptr })
        )
    }
}

/// Slice reference of this buffer
impl<T> Deref for IoBuf<T> {
    type Target = [T];
    fn deref(&self) -> &[T] {
        let elem_size = core::mem::size_of::<T>();
        unsafe { std::slice::from_raw_parts(self.ptr, self.size / elem_size) }
    }
}

/// Mutable slice reference of this buffer
impl<T> DerefMut for IoBuf<T> {
    fn deref_mut(&mut self) -> &mut [T] {
        let elem_size = core::mem::size_of::<T>();
        unsafe { std::slice::from_raw_parts_mut(self.ptr, self.size / elem_size) }
    }
}

/// Free buffer with same alloc layout
impl<T> Drop for IoBuf<T> {
    fn drop(&mut self) {
        // munlock the buffer if it was mlocked. `&mut self` in Drop means no
        // other thread can observe `mlocked`, so a plain relaxed load suffices.
        if *self.mlocked.get_mut() {
            unsafe {
                libc::munlock(self.ptr as *const libc::c_void, self.size);
            }
        }

        let layout = std::alloc::Layout::from_size_align(self.size, 4096).unwrap();
        unsafe { std::alloc::dealloc(self.ptr as *mut u8, layout) };
    }
}

#[macro_export]
macro_rules! zero_io_buf {
    ($buffer:expr) => {{
        unsafe {
            std::ptr::write_bytes($buffer.as_mut_ptr(), 0, $buffer.len());
        }
    }};
}
