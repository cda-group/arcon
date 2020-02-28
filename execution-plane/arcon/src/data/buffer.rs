// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::allocator::{AllocId, AllocResult, ArconAllocator};
use arcon_error::*;
use crossbeam_utils::CachePadded;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// A reusable buffer allocated through [ArconAllocator]
///
/// Assumes a Single-writer, single-reader setup.
#[derive(Debug)]
pub struct EventBuffer<T> {
    /// A raw pointer to our allocated memory block
    ptr: *mut T,
    /// Reference to the allocator
    ///
    /// Used to dealloc `ptr` when the EventBuffer is dropped
    allocator: Arc<Mutex<ArconAllocator>>,
    /// A unique identifier for the allocation
    id: AllocId,
    /// How many data elements there are in `ptr`
    capacity: usize,
    /// Flag indicating whether the buffer is available or not
    free: CachePadded<AtomicBool>,
}

impl<T> EventBuffer<T> {
    /// Creates a new EventBuffer
    pub fn new(
        capacity: usize,
        allocator: Arc<Mutex<ArconAllocator>>,
    ) -> ArconResult<EventBuffer<T>> {
        let mut a = allocator.lock().unwrap();

        if let AllocResult::Alloc(id, ptr) = unsafe { a.alloc::<T>(capacity) } {
            Ok(EventBuffer {
                ptr: ptr as *mut T,
                allocator: allocator.clone(),
                id,
                capacity,
                free: AtomicBool::new(true).into(),
            })
        } else {
            arcon_err!("EventBuffer Alloc err")
        }
    }

    /// Returns a const pointer to the underlying buffer
    pub fn as_ptr(&self) -> *const T {
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
        self.free.compare_and_swap(true, false, Ordering::Relaxed)
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
    pub fn new(buffer: Arc<EventBuffer<T>>, len: usize, capacity: usize) -> BufferWriter<T> {
        BufferWriter {
            buffer,
            len,
            capacity,
        }
    }
    /// Pushes an item onto the buffer
    ///
    /// Returns false if it tries to write beyond the buffers capacity
    pub fn push(&mut self, value: T) -> Option<T> {
        if let Some(v) = (*self.buffer).push(value, self.len) {
            Some(v)
        } else {
            self.len += 1;
            None
        }
    }
    /// Generate a reader
    ///
    /// Drops the BufferWriter making it no longer accessible
    pub fn reader(self) -> BufferReader<T> {
        BufferReader {
            buffer: self.buffer,
            len: self.len,
        }
    }

    /// Returns current position in the Buffer
    pub fn len(&self) -> usize {
        self.len
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

    /// Length of buffer
    #[inline]
    pub fn len(&self) -> usize {
        self.len
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
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[test]
    fn event_buffer_test() {
        let total_bytes = 1024;
        let allocator = Arc::new(Mutex::new(ArconAllocator::new(total_bytes)));
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
            assert_eq!(writer.push(10 as u64), Some(10));

            let reader = writer.reader();

            assert_eq!(*reader.as_slice(), *items);
        }
        // Buffer is dropped, check allocator
        let a = allocator.lock().unwrap();
        assert_eq!(a.total_allocations(), 1);
        assert_eq!(a.bytes_remaining(), total_bytes);
    }
}
