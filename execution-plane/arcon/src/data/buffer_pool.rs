// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use crate::allocator::ArconAllocator;
use crate::data::buffer::{BufferWriter, EventBuffer};
use arcon_error::*;
use std::sync::{Arc, Mutex};

/// A preallocated pool of EventBuffers
pub struct BufferPool<T> {
    /// Amount of buffers in the pool
    capacity: usize,
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
        allocator: Arc<Mutex<ArconAllocator>>,
    ) -> ArconResult<BufferPool<T>> {
        let mut buffers: Vec<Arc<EventBuffer<T>>> = Vec::with_capacity(capacity);

        // Allocate and add EventBuffers to our pool
        for _ in 0..capacity {
            let buffer: EventBuffer<T> = EventBuffer::new(buffer_size, allocator.clone())?;
            buffers.push(Arc::new(buffer));
        }

        Ok(BufferPool {
            capacity,
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
        if buf.is_free() {
            // We take it....
            buf.reserve();
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
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Returns the size each buffer holds
    #[inline]
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::allocator::*;

    #[test]
    fn buffer_pool_test() {
        let allocator = Arc::new(Mutex::new(ArconAllocator::new(10024)));
        let buffer_size = 100;
        let pool_capacity = 2;
        let mut pool: BufferPool<u64> =
            BufferPool::new(pool_capacity, buffer_size, allocator.clone()).unwrap();

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
            assert_eq!(pool.try_get().is_none(), true);
        }
        // reader_two is dropped at this point.
        // its underlying buffer should be returned to the pool

        assert_eq!(pool.try_get().is_some(), true);
    }
}
