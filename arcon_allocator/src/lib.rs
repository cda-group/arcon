// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use fxhash::FxHashMap;
use std::alloc::{GlobalAlloc, Layout, System};

/// Type alias for a unique Alloc identifier
pub type AllocId = (u32, u64);
/// Type alias for alloc pointers
pub type AllocPtr = *mut u8;

/// An Enum containing all possible results from an alloc call
#[derive(Debug)]
pub enum AllocResult {
    /// A Successful allocation
    Alloc(AllocId, AllocPtr),
    /// The Allocator is Out-of-memory
    ///
    /// Returns how many bytes are currently available for allocation
    ArconOOM(usize),
    /// System Allocator failed to allocate memory
    ///
    /// This is most likely an Out-of-memory issue
    SystemOOM,
    /// A Capacity error
    CapacityErr(String),
}

/// An Allocator for arcon.
///
/// The allocator is not meant to handle all heap allocations
/// during the execution. However, it is intended to be used to manage
/// memory for different sections of the runtime. This includes message buffers,
/// network buffers, and state indexes.
#[derive(Debug)]
pub struct Allocator {
    /// HashMap keeping track of allocations
    allocations: FxHashMap<AllocId, (AllocPtr, Layout)>,
    /// Memory limit
    limit: usize,
    /// Current alloc epoch
    alloc_epoch: u32,
    /// Total allocations in the current epoch
    alloc_counter: u64,
    /// Bytes allocated currently
    curr_alloc: usize,
}

impl Allocator {
    /// Creates a new Allocator with the given memory limit size
    pub fn new(limit: usize) -> Allocator {
        Allocator {
            allocations: FxHashMap::default(),
            limit,
            alloc_epoch: 1,
            alloc_counter: 0,
            curr_alloc: 0,
        }
    }
    /// Allocate memory block of type T with given capacity
    ///
    /// # Safety
    /// It is up to the caller to ensure `dealloc` with the generated AllocId
    pub unsafe fn alloc<T>(&mut self, capacity: usize) -> AllocResult {
        if capacity == 0 {
            return AllocResult::CapacityErr("Cannot alloc for 0 sized pointer".into());
        }
        let (size, align) = (std::mem::size_of::<T>(), std::mem::align_of::<T>());

        let required_bytes = match capacity.checked_mul(size) {
            Some(v) => v,
            None => return AllocResult::CapacityErr("Capacity overflow".into()),
        };

        if self.curr_alloc + required_bytes > self.limit {
            return AllocResult::ArconOOM(self.bytes_remaining());
        }

        let layout = Layout::from_size_align_unchecked(required_bytes, align);
        let mem = System.alloc(layout);

        if mem.is_null() {
            return AllocResult::SystemOOM;
        }

        self.curr_alloc += layout.size();

        if self.alloc_counter == u64::max_value() {
            self.alloc_epoch += 1;
            self.alloc_counter = 0;
        }

        let id = self.alloc_counter;
        self.alloc_counter += 1;
        self.allocations
            .insert((self.alloc_epoch, id), (mem, layout));

        AllocResult::Alloc((self.alloc_epoch, id), mem)
    }
    /// Deallocate memory through the given AllocId
    ///
    /// # Safety
    /// It is up to the caller to ensure that the given AllocId is active
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
    pub fn total_allocations(&self) -> u128 {
        if self.alloc_epoch == 1 {
            self.alloc_counter.into()
        } else {
            // sum up previous epoch sums
            let epoch_sum: u128 = ((self.alloc_epoch - 1) as u64 * u64::max_value()).into();
            // add current epoch allocs to sum
            epoch_sum + self.alloc_counter as u128
        }
    }
    /// Returns how much bytes are available to allocate
    pub fn bytes_remaining(&self) -> usize {
        self.limit - self.curr_alloc
    }
}

impl Drop for Allocator {
    fn drop(&mut self) {
        // Just to make sure we are not leaving any outstanding allocations
        let ids: Vec<AllocId> = self.allocations.keys().copied().collect();
        for id in ids {
            unsafe {
                self.dealloc(id);
            }
        }
    }
}

unsafe impl Send for Allocator {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[test]
    fn simple_allocator_test() {
        let total_bytes = 1024;
        let allocator = Arc::new(Mutex::new(Allocator::new(total_bytes)));

        let mut a = allocator.lock().unwrap();

        assert_eq!(a.allocated_bytes(), 0);

        let id_one: AllocId = match unsafe { a.alloc::<u64>(100) } {
            AllocResult::Alloc(id, _) => id,
            _ => panic!("not supposed to happen"),
        };
        // 100 * 8
        assert_eq!(a.allocated_bytes(), 800);

        let id_two: AllocId = match unsafe { a.alloc::<i32>(50) } {
            AllocResult::Alloc(id, _) => id,
            _ => panic!("not supposed to happen"),
        };
        // 50 * 4
        assert_eq!(a.allocated_bytes(), 1000);

        // At this point, we will receive an out-of-memory error..
        match unsafe { a.alloc::<f32>(500) } {
            AllocResult::ArconOOM(remaining_bytes) => assert_eq!(remaining_bytes, 24),
            _ => panic!("not supposed to happen"),
        };

        // dealloc
        unsafe { a.dealloc(id_one) };
        unsafe { a.dealloc(id_two) };

        // final assertations
        assert_eq!(a.allocated_bytes(), 0);
        assert_eq!(a.total_allocations(), 2);
        assert_eq!(a.bytes_remaining(), total_bytes);
    }
}
