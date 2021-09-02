use crate::error::*;
use arcon_allocator::{Alloc, AllocId, Allocator};
use kompact::net::buffers::Chunk;
use std::sync::{Arc, Mutex};

/// A Buffer backed by the [ArconAllocator]
///
/// Is intended to be used by Kompact's network implementation
pub(crate) struct NetworkBuffer {
    /// A raw pointer to our allocated memory block
    ptr: *mut u8,
    /// Reference to the allocator
    ///
    /// Used to dealloc `ptr` when the NetworkBuffer is dropped
    allocator: Arc<Mutex<Allocator>>,
    /// A unique identifier for the allocation
    id: AllocId,
    /// How many data elements there are in `ptr`
    capacity: usize,
}

impl NetworkBuffer {
    /// Creates a new NetworkBuffer
    #[inline]
    #[allow(dead_code)]
    pub fn new(capacity: usize, allocator: Arc<Mutex<Allocator>>) -> ArconResult<NetworkBuffer> {
        let mut a = allocator.lock().unwrap();

        match unsafe { a.alloc::<u8>(capacity) } {
            Ok(Alloc(id, ptr)) => Ok(NetworkBuffer {
                ptr,
                allocator: allocator.clone(),
                id,
                capacity,
            }),
            Err(err) => Err(Error::Unsupported {
                msg: err.to_string(),
            }),
        }
    }

    /// Returns the capacity of the buffer
    #[inline]
    #[allow(dead_code)]
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

impl Drop for NetworkBuffer {
    fn drop(&mut self) {
        let mut allocator = self.allocator.lock().unwrap();
        // Instruct the allocator to dealloc
        unsafe { allocator.dealloc(self.id) };
    }
}

unsafe impl Send for NetworkBuffer {}

impl Chunk for NetworkBuffer {
    fn as_mut_ptr(&mut self) -> *mut u8 {
        self.ptr
    }
    fn len(&self) -> usize {
        self.capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn network_buffer_test() {
        // This test does not do much. Just need to ensure allocation and the drop of the NetworkBuffer works correctly.
        let total_bytes = 1024;
        let allocator = Arc::new(Mutex::new(Allocator::new(total_bytes)));
        {
            let buffer: NetworkBuffer = NetworkBuffer::new(512, allocator.clone()).unwrap();
            assert_eq!(buffer.capacity(), 512);
        }
        // Buffer is dropped, check allocator
        let a = allocator.lock().unwrap();
        assert_eq!(a.total_allocations(), 1);
        assert_eq!(a.bytes_remaining(), total_bytes);
    }
}
