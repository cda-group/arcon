// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use arcon_error::*;
use fxhash::FxHashMap;
use std::alloc::{GlobalAlloc, Layout, System};

/// Type alias for a unique Alloc identifier
pub type AllocId = u64;
/// Type alias for alloc pointers
pub type AllocPtr = *mut u8;

/// An Allocator for [arcon]
///
/// The allocator is not meant to handle all heap allocations
/// during the execution. However, it is intended to be used to manage
/// memory for different sections of [arcon]. This includes message buffers,
/// network buffers, state backends...
#[derive(Debug)]
pub struct ArconAllocator {
    /// HashMap keeping track of allocations
    allocations: FxHashMap<AllocId, (AllocPtr, Layout)>,
    /// Memory limit
    limit: usize,
    /// Total allocations made so far
    alloc_counter: u64,
    /// Bytes allocated currently
    curr_alloc: usize,
}

impl ArconAllocator {
    /// Creates a new ArconAllocator with the given memory limit size
    pub fn new(limit: usize) -> ArconAllocator {
        ArconAllocator {
            allocations: FxHashMap::default(),
            limit,
            alloc_counter: 0,
            curr_alloc: 0,
        }
    }
    /// Allocate memory block of type T with given capacity
    pub unsafe fn alloc<T>(&mut self, capacity: usize) -> ArconResult<(AllocId, AllocPtr)> {
        if capacity == 0 {
            return arcon_err!("{}", "Cannot alloc for 0 sized pointer");
        }
        let (size, align) = (std::mem::size_of::<T>(), std::mem::align_of::<T>());

        let required_bytes = capacity
            .checked_mul(size)
            .ok_or(arcon_err_kind!("{}", "Capacity overflow"))?;

        if self.curr_alloc + required_bytes > self.limit {
            return arcon_err!("{}", "ArconAllocator is OOM");
        }

        let layout = Layout::from_size_align_unchecked(required_bytes, align);
        let mem = System.alloc(layout);

        if mem.is_null() {
            return arcon_err!("{}", "SystemAllocator is OOM");
        }

        self.curr_alloc += layout.size();
        let id = self.alloc_counter;
        self.alloc_counter += 1;
        self.allocations.insert(id, (mem, layout));

        Ok((id, mem))
    }
    /// Deallocate memory through the given AllocId
    pub unsafe fn dealloc(&mut self, id: AllocId) {
        if let Some((ptr, layout)) = self.allocations.remove(&id) {
            System.dealloc(ptr, layout);
            self.curr_alloc -= layout.size();
        }
    }
    /// Returns amount of bytes that are currently allocated
    pub fn allocated_bytes(&self) -> usize {
        self.curr_alloc
    }
    /// Returns total allocations made so far
    pub fn total_allocations(&self) -> u64 {
        self.alloc_counter
    }
    /// Returns how much bytes are available to allocate
    pub fn bytes_remaining(&self) -> usize {
        self.limit - self.curr_alloc
    }
}

impl Drop for ArconAllocator {
    fn drop(&mut self) {
        // Just to make sure we are not leaving any outstanding allocations
        let ids: Vec<AllocId> = self.allocations.keys().map(|i| i.clone()).collect();
        for id in ids {
            unsafe {
                self.dealloc(id);
            }
        }
    }
}

unsafe impl Send for ArconAllocator {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[test]
    fn simple_allocator_test() {
        let total_bytes = 1024;
        let allocator = Arc::new(Mutex::new(ArconAllocator::new(total_bytes)));

        let mut a = allocator.lock().unwrap();

        assert_eq!(a.allocated_bytes(), 0);

        let (id_one, _) = unsafe { a.alloc::<u64>(100).unwrap() };
        // 100 * 8
        assert_eq!(a.allocated_bytes(), 800);

        let (id_two, _) = unsafe { a.alloc::<i32>(50).unwrap() };
        // 50 * 4
        assert_eq!(a.allocated_bytes(), 1000);

        // At this point, we will receive an out-of-memory error..
        assert_eq!(unsafe { a.alloc::<f32>(500).is_err() }, true);

        // dealloc
        unsafe { a.dealloc(id_one) };
        unsafe { a.dealloc(id_two) };

        // final assertations
        assert_eq!(a.allocated_bytes(), 0);
        assert_eq!(a.total_allocations(), 2);
        assert_eq!(a.bytes_remaining(), total_bytes);
    }
}
