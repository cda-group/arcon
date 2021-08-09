// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use fxhash::FxHashMap;
use snafu::Snafu;
use std::alloc::{GlobalAlloc, Layout, System};

#[cfg(feature = "allocator_metrics")]
use metrics::{gauge, increment_counter, register_counter, register_gauge};

/// Type alias for a unique Alloc identifier
pub type AllocId = (u32, u64);
/// Type alias for alloc pointers
pub type AllocPtr = *mut u8;

pub type AllocResult = std::result::Result<Alloc, AllocError>;

#[derive(Debug)]
pub struct Alloc(pub AllocId, pub AllocPtr);

/// An Enum containing all possible results from an alloc call
#[derive(Debug, Snafu)]
pub enum AllocError {
    /// The Allocator is Out-of-memory
    ///
    /// Returns how many bytes are currently available for allocation
    #[snafu(display("Arcon Allocator is out of memory, {} bytes remaining!", bytes,))]
    ArconOOM { bytes: usize },
    /// System Allocator failed to allocate memory
    ///
    /// This is most likely an Out-of-memory issue
    #[snafu(display("System Allocator failed to allocate memory"))]
    SystemOOM,
    /// A Capacity error
    #[snafu(display("Capacity Error {}", msg))]
    CapacityErr { msg: String },
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
        #[cfg(feature = "allocator_metrics")]
        {
            register_gauge!("arcon_allocator_total_bytes");
            register_gauge!("arcon_allocator_bytes_remaining");
            register_counter!("arcon_allocator_alloc_counter");
        }

        #[cfg(feature = "allocator_metrics")]
        gauge!("arcon_allocator_total_bytes", limit as f64);

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
            return Err(AllocError::CapacityErr {
                msg: "Cannot alloc for 0 sized pointer".into(),
            });
        }
        let (size, align) = (std::mem::size_of::<T>(), std::mem::align_of::<T>());

        let required_bytes = match capacity.checked_mul(size) {
            Some(v) => v,
            None => {
                return Err(AllocError::CapacityErr {
                    msg: "Capacity overflow".into(),
                })
            }
        };

        if self.curr_alloc + required_bytes > self.limit {
            return Err(AllocError::ArconOOM {
                bytes: self.bytes_remaining(),
            });
        }

        let layout = Layout::from_size_align_unchecked(required_bytes, align);
        let mem = System.alloc(layout);

        if mem.is_null() {
            return Err(AllocError::SystemOOM);
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

        #[cfg(feature = "allocator_metrics")]
        {
            increment_counter!("arcon_allocator_alloc_counter");
            gauge!(
                "arcon_allocator_bytes_remaining",
                self.bytes_remaining() as f64
            );
        }

        Ok(Alloc((self.alloc_epoch, id), mem))
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
            Ok(Alloc(id, _)) => id,
            _ => panic!("not supposed to happen"),
        };
        // 100 * 8
        assert_eq!(a.allocated_bytes(), 800);

        let id_two: AllocId = match unsafe { a.alloc::<i32>(50) } {
            Ok(Alloc(id, _)) => id,
            _ => panic!("not supposed to happen"),
        };
        // 50 * 4
        assert_eq!(a.allocated_bytes(), 1000);

        // At this point, we will receive an out-of-memory error..
        match unsafe { a.alloc::<f32>(500) } {
            Err(AllocError::ArconOOM { bytes }) => assert_eq!(bytes, 24),
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
