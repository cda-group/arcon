use crate::error::*;
use arcon_allocator::{Alloc, AllocId, Allocator};
use crossbeam_utils::CachePadded;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};

/// A reusable buffer allocated through [Allocator]
///
/// Assumes a single-writer, single-reader setup.
#[derive(Debug)]
pub struct EventBuffer<T> {
    /// A raw pointer to our allocated memory block
    ptr: *mut T,
    /// Reference to the allocator
    ///
    /// Used to dealloc `ptr` when the EventBuffer is dropped
    allocator: Arc<Mutex<Allocator>>,
    /// A unique identifier for the allocation
    id: AllocId,
    /// How many data elements there are in `ptr`
    capacity: usize,
    /// Flag indicating whether the buffer is available or not
    free: CachePadded<AtomicBool>,
}

impl<T> EventBuffer<T> {
    /// Creates a new EventBuffer
    pub fn new(capacity: usize, allocator: Arc<Mutex<Allocator>>) -> ArconResult<EventBuffer<T>> {
        let mut a = allocator.lock().unwrap();

        match unsafe { a.alloc::<T>(capacity) } {
            Ok(Alloc(id, ptr)) => Ok(EventBuffer {
                ptr: ptr as *mut T,
                allocator: allocator.clone(),
                id,
                capacity,
                free: AtomicBool::new(true).into(),
            }),
            Err(err) => Err(Error::Unsupported {
                msg: err.to_string(),
            }),
        }
    }

    /// Returns a const pointer to the underlying buffer
    pub fn as_ptr(&self) -> *const T {
        self.ptr
    }

    /// Returns a mutable pointer to the underlying buffer
    pub fn as_mut_ptr(&self) -> *mut T {
        self.ptr
    }

    /// Pushes an item onto the buffer at ptr[len]
    ///
    /// It is up to the writer to keep track of len.
    /// But the function will ensure it does not write beyond
    /// the capacity of the buffer..
    #[inline]
    pub fn push(&self, value: T, len: usize) -> Option<T> {
        if len >= self.capacity {
            Some(value)
        } else {
            unsafe {
                std::ptr::write(self.ptr.add(len), value);
            };
            None
        }
    }
    /// Returns the capacity behind the data ptr
    #[inline]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Set free flag to true
    ///
    /// Should only be called by the reader
    #[inline]
    pub fn release(&self) {
        self.free.store(true, Ordering::Relaxed);
    }

    /// Attempt to reserve the EventBuffer
    ///
    /// Should only be called from the writer
    #[inline]
    pub fn try_reserve(&self) -> bool {
        match self
            .free
            .compare_exchange_weak(true, false, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(res) => res,
            Err(res) => res,
        }
    }
}

unsafe impl<T: Sync> Sync for EventBuffer<T> {}
unsafe impl<T: Send> Send for EventBuffer<T> {}

impl<T> Drop for EventBuffer<T> {
    fn drop(&mut self) {
        let mut allocator = self.allocator.lock().unwrap();
        // Instruct the allocator to dealloc
        unsafe { allocator.dealloc(self.id) };
    }
}

/// An EventBuffer writer
#[derive(Debug)]
#[allow(dead_code)]
pub struct BufferWriter<T> {
    /// Reference to the underlying buffer
    buffer: Arc<EventBuffer<T>>,
    /// Current writer index
    len: usize,
    /// Capacity of the allocated buffer
    capacity: usize,
}

impl<T> BufferWriter<T> {
    /// Creates a new BufferWriter
    #[inline]
    pub fn new(buffer: Arc<EventBuffer<T>>, len: usize, capacity: usize) -> BufferWriter<T> {
        BufferWriter {
            buffer,
            len,
            capacity,
        }
    }
    /// Pushes an item onto the buffer
    ///
    /// Returns back the element as Some(value) if it tries to write beyond the buffers capacity
    #[inline]
    pub fn push(&mut self, value: T) -> Option<T> {
        if let Some(v) = (*self.buffer).push(value, self.len) {
            Some(v)
        } else {
            self.len += 1;
            None
        }
    }

    /// Return a const ptr to the underlying buffer
    #[inline]
    pub fn as_ptr(&self) -> *const T {
        self.buffer.as_ptr()
    }

    /// Generate a reader
    #[inline]
    pub fn reader(&self) -> BufferReader<T> {
        BufferReader {
            buffer: self.buffer.clone(),
            len: self.len,
        }
    }

    /// Copy data from another BufferWriter
    pub fn copy_from_writer(&mut self, other: &BufferWriter<T>) {
        let other_ptr = other.as_ptr();
        let other_len = other.len();

        unsafe {
            std::ptr::copy(other_ptr, (*self.buffer).as_mut_ptr(), other_len);
        };
        self.len = other_len;
    }

    /// Returns current position in the Buffer
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

/// An EventBuffer reader
///
/// Once dropped, it will notify the [BufferPool]
/// that the buffer is available once again.
#[derive(Debug, Clone)]
pub struct BufferReader<T> {
    /// Reference to the EventBuffer
    buffer: Arc<EventBuffer<T>>,
    /// Total events in the buffer
    ///
    /// Does not have to be the same as buffer.capacity()
    len: usize,
}

impl<T> BufferReader<T> {
    /// Tells the BufferPool that the EventBuffer is available again
    ///
    /// Is called once the BufferReader is dropped
    #[inline]
    fn release(&self) {
        (*self.buffer).release();
    }

    /// Return ptr to the underlying buffer
    #[inline]
    pub fn as_ptr(&self) -> *const T {
        self.buffer.as_ptr()
    }

    /// Return reader as slice
    #[inline]
    pub fn as_slice(&self) -> &[T] {
        unsafe { std::slice::from_raw_parts(self.as_ptr(), self.len) }
    }

    /// Convert into Vec
    #[inline]
    pub fn to_vec(&self) -> Vec<T> {
        let mut dst = Vec::with_capacity(self.len);
        unsafe {
            dst.set_len(self.len);
            std::ptr::copy(self.as_ptr(), dst.as_mut_ptr(), self.len);
        };
        dst
    }

    /// Length of buffer
    #[inline]
    pub fn len(&self) -> usize {
        self.len
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}
impl<T> Drop for BufferReader<T> {
    fn drop(&mut self) {
        self.release();
    }
}

// Grabbed from SmallVec's implementation
/// Turn BufferReader into an Iterator
pub struct IntoIter<A> {
    reader: BufferReader<A>,
    current: usize,
    end: usize,
}

impl<A> Iterator for IntoIter<A> {
    type Item = A;

    #[inline]
    fn next(&mut self) -> Option<A> {
        if self.current == self.end {
            None
        } else {
            unsafe {
                let current = self.current;
                self.current += 1;
                Some(std::ptr::read(self.reader.as_ptr().add(current)))
            }
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let size = self.end - self.current;
        (size, Some(size))
    }
}

impl<A> IntoIterator for BufferReader<A> {
    type IntoIter = IntoIter<A>;
    type Item = A;
    fn into_iter(self) -> Self::IntoIter {
        let len = self.len();
        IntoIter {
            reader: self,
            current: 0,
            end: len,
        }
    }
}

/// A Vec to BufferReader converter for [arcon] tests
#[cfg(test)]
impl<T> From<Vec<T>> for BufferReader<T> {
    fn from(v: Vec<T>) -> BufferReader<T> {
        let event_buffer = EventBuffer::<T>::new(v.len(), crate::test_utils::ALLOCATOR.clone())
            .expect("Failed to alloc memory");
        let mut writer = BufferWriter {
            buffer: Arc::new(event_buffer),
            len: 0,
            capacity: v.len(),
        };
        for value in v.into_iter() {
            writer.push(value);
        }

        writer.reader()
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct PoolInfo {
    pub(crate) buffer_size: usize,
    pub(crate) capacity: usize,
    pub(crate) limit: usize,
    pub(crate) allocator: Arc<Mutex<Allocator>>,
}

impl PoolInfo {
    pub fn new(
        buffer_size: usize,
        capacity: usize,
        limit: usize,
        allocator: Arc<Mutex<Allocator>>,
    ) -> PoolInfo {
        PoolInfo {
            buffer_size,
            capacity,
            limit,
            allocator,
        }
    }
}

/// A preallocated pool of EventBuffers
#[allow(dead_code)]
pub struct BufferPool<T> {
    /// Reference to an Arcon Allocator
    allocator: Arc<Mutex<Allocator>>,
    /// Size per buffer
    buffer_size: usize,
    /// Vec of buffers in the pool
    buffers: Vec<Arc<EventBuffer<T>>>,
    /// Index of which buffer is next in line.
    curr_buffer: usize,
}
impl<T> BufferPool<T> {
    /// Create a new BufferPool
    #[inline]
    pub fn new(
        capacity: usize,
        buffer_size: usize,
        allocator: Arc<Mutex<Allocator>>,
    ) -> ArconResult<BufferPool<T>> {
        let mut buffers: Vec<Arc<EventBuffer<T>>> = Vec::with_capacity(capacity);

        // Allocate and add EventBuffers to our pool
        for _ in 0..capacity {
            let buffer: EventBuffer<T> = EventBuffer::new(buffer_size, allocator.clone())?;
            buffers.push(Arc::new(buffer));
        }

        Ok(BufferPool {
            allocator,
            buffer_size,
            buffers,
            curr_buffer: 0,
        })
    }

    /// Attempt to fetch a BufferWriter
    ///
    /// Returns None if it fails to find Writer for the current index
    #[inline]
    pub fn try_get(&mut self) -> Option<BufferWriter<T>> {
        let buf = &self.buffers[self.curr_buffer];
        let mut opt = None;
        if buf.try_reserve() {
            opt = Some(BufferWriter::new(buf.clone(), 0, buf.capacity()))
        }

        self.index_incr();

        opt
    }

    /// Busy waiting for a BufferWriter
    ///
    /// Should be used carefully
    #[inline]
    pub fn get(&mut self) -> BufferWriter<T> {
        loop {
            match self.try_get() {
                None => {}
                Some(v) => return v,
            }
        }
    }

    /// Bumps the buffer index
    ///
    /// If we have reached the capacity, we simply
    /// reset to zero again.
    #[inline]
    fn index_incr(&mut self) {
        self.curr_buffer += 1;
        if self.curr_buffer == self.buffers.capacity() {
            // Reset
            self.curr_buffer = 0;
        }
    }

    /// Returns the capacity of the BufferPool
    #[inline]
    #[allow(dead_code)]
    pub fn capacity(&self) -> usize {
        self.buffers.capacity()
    }

    /// Returns the size each buffer holds
    #[inline]
    #[allow(dead_code)]
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[test]
    fn event_buffer_test() {
        let total_bytes = 1024;
        let allocator = Arc::new(Mutex::new(Allocator::new(total_bytes)));
        let items: Vec<u64> = vec![10, 20, 30, 40, 50, 60, 70, 80, 90, 100];

        {
            let buffer: EventBuffer<u64> = EventBuffer::new(10, allocator.clone()).unwrap();
            let mut writer = BufferWriter {
                buffer: Arc::new(buffer),
                len: 0,
                capacity: items.len(),
            };

            for item in items.clone() {
                writer.push(item);
            }

            // reached buffer limit, we should get the value back.
            assert_eq!(writer.push(10_u64), Some(10));

            let reader = writer.reader();

            assert_eq!(*reader.as_slice(), *items);
        }
        // Buffer is dropped, check allocator
        let a = allocator.lock().unwrap();
        assert_eq!(a.total_allocations(), 1);
        assert_eq!(a.bytes_remaining(), total_bytes);
    }

    #[test]
    fn buffer_pool_test() {
        let allocator = Arc::new(Mutex::new(Allocator::new(10024)));
        let buffer_size = 100;
        let pool_capacity = 2;
        let mut pool: BufferPool<u64> =
            BufferPool::new(pool_capacity, buffer_size, allocator).unwrap();

        let mut buffer = pool.try_get().unwrap();

        for i in 0..buffer_size {
            buffer.push(i as u64);
        }

        let reader_one = buffer.reader();

        let data = reader_one.as_slice();
        assert_eq!(data.len(), buffer_size);

        let buffer = pool.try_get().unwrap();

        {
            let reader_two = buffer.reader();

            let data = reader_two.as_slice();
            assert_eq!(data.len(), 0);

            // No available buffers at this point
            assert!(pool.try_get().is_none());
        }
        // reader_two is dropped at this point.
        // its underlying buffer should be returned to the pool

        assert!(pool.try_get().is_some());
    }
}
