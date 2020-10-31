// Copyright (c) 2016 Amanieu d'Antras
// SPDX-License-Identifier: MIT

// Modifications Copyright (c) KTH Royal Institute of Technology
// SPDX-License-Identifier: AGPL-3.0-only

use crate::index::hash::bitmask::BitMaskIter;
use core::{
    alloc::Layout,
    hint,
    intrinsics::{likely, unlikely},
    iter::FusedIterator,
    marker::PhantomData,
    mem,
    ptr::NonNull,
};
use std::alloc::{alloc, dealloc, handle_alloc_error};

use crate::{
    data::{Key, Value},
    index::hash::{bitmask::BitMask, imp::Group},
};

/// Augments `AllocErr` with a `CapacityOverflow` variant.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum CollectionAllocErr {
    /// Error due to the computed capacity exceeding the collection's maximum
    /// (usually `isize::MAX` bytes).
    CapacityOverflow,
    /// Error due to the allocator.
    AllocErr {
        /// The layout of the allocation request that failed.
        layout: Layout,
    },
}

#[inline]
unsafe fn offset_from<T>(to: *const T, from: *const T) -> usize {
    (to as usize - from as usize) / mem::size_of::<T>()
}

/// Whether memory allocation errors should return an error or abort.
#[derive(Copy, Clone)]
#[allow(dead_code)]
enum Fallibility {
    Fallible,
    Infallible,
}

impl Fallibility {
    /// Error to return on capacity overflow.
    #[inline]
    fn capacity_overflow(self) -> CollectionAllocErr {
        match self {
            Fallibility::Fallible => CollectionAllocErr::CapacityOverflow,
            Fallibility::Infallible => panic!("Hash table capacity overflow"),
        }
    }

    /// Error to return on allocation error.
    #[inline]
    fn alloc_err(self, layout: Layout) -> CollectionAllocErr {
        match self {
            Fallibility::Fallible => CollectionAllocErr::AllocErr { layout },
            Fallibility::Infallible => handle_alloc_error(layout),
        }
    }
}

/// Control byte value for an empty bucket.
pub(crate) const EMPTY: u8 = 0b1111_1111;

/// Meta bytes for a modified bucket.
pub(crate) const MODIFIED: u8 = 0b1111_1110;
pub(crate) const MODIFIED_TOUCHED: u8 = 0b1000_0001;

/// Meta bytes for a safe bucket.
pub(crate) const SAFE: u8 = 0b0000_0000;
pub(crate) const SAFE_TOUCHED: u8 = 0b0000_0010;

/// Checks whether a meta byte represents a safe bucket (top bit is clear).
#[inline]
fn is_safe(meta: u8) -> bool {
    meta & 0x80 == 0
}

/// Primary hash function, used to select the initial bucket to probe from.
#[inline]
#[allow(clippy::cast_possible_truncation)]
fn h1(hash: u64) -> usize {
    // On 32-bit platforms we simply ignore the higher hash bits.
    hash as usize
}

/// Secondary hash function, saved in the low 7 bits of the control byte.
#[inline]
#[allow(clippy::cast_possible_truncation)]
fn h2(hash: u64) -> u8 {
    // Grab the top 7 bits of the hash. While the hash is normally a full 64-bit
    // value, some hash functions (such as FxHash) produce a usize result
    // instead, which means that the top 32 bits are 0 on 32-bit platforms.
    let hash_len = usize::min(mem::size_of::<usize>(), mem::size_of::<u64>());
    let top7 = hash >> (hash_len * 8 - 7);
    (top7 & 0x7f) as u8 // truncation
}

/// Probe sequence based on triangular numbers, which is guaranteed (since our
/// table size is a power of two) to visit every group of elements exactly once.
///
/// A triangular probe has us jump by 1 more group every time. So first we
/// jump by 1 group (meaning we just continue our linear scan), then 2 groups
/// (skipping over 1 group), then 3 groups (skipping over 2 groups), and so on.
///
/// Proof that the probe will visit every group in the table:
/// <https://fgiesen.wordpress.com/2015/02/22/triangular-numbers-mod-2n/>
struct ProbeSeq {
    bucket_mask: usize,
    pos: usize,
    stride: usize,
    probe_counter: usize,
    probe_limit: usize,
}

impl Iterator for ProbeSeq {
    type Item = usize;

    #[inline]
    fn next(&mut self) -> Option<usize> {
        if self.probe_counter >= self.probe_limit || self.stride >= self.bucket_mask {
            return None;
        }

        let result = self.pos;
        self.stride += Group::WIDTH;
        self.pos += self.stride;
        self.pos &= self.bucket_mask;
        self.probe_counter += 1;
        Some(result)
    }
}

#[inline]
fn calculate_layout<A, B>(
    mod_buckets: usize,
    read_buckets: usize,
) -> Option<(Layout, usize, usize, usize)> {
    debug_assert!(mod_buckets.is_power_of_two());
    debug_assert!(read_buckets.is_power_of_two());

    // Array of buckets
    let mod_data = Layout::array::<A>(mod_buckets).ok()?;
    let read_data = Layout::array::<B>(read_buckets).ok()?;

    let (bucket_layout, bucket_offset) = mod_data.extend(read_data).ok()?;

    // Array of control bytes. This must be aligned to the group size.
    //
    // We add `Group::WIDTH` control bytes at the end of the array which
    // replicate the bytes at the start of the array and thus avoids the need to
    // perform bounds-checking while probing.
    //
    // There is no possible overflow here since buckets is a power of two and
    // Group::WIDTH is a small number.
    let mod_ctrl =
        unsafe { Layout::from_size_align_unchecked(mod_buckets + Group::WIDTH, Group::WIDTH) };
    let read_ctrl =
        unsafe { Layout::from_size_align_unchecked(read_buckets + Group::WIDTH, Group::WIDTH) };

    let (ctrl, read_ctrl_offset) = mod_ctrl.extend(read_ctrl).ok()?;

    let (full_layout, ctrl_offset) = bucket_layout.extend(ctrl).ok()?;
    Some((full_layout, bucket_offset, ctrl_offset, read_ctrl_offset))
}

/// Returns a Layout which describes the meta bytes of the hash table.
fn meta_layout(buckets: usize) -> Option<Layout> {
    debug_assert!(buckets.is_power_of_two());
    let ctrl = unsafe { Layout::from_size_align_unchecked(buckets + Group::WIDTH, Group::WIDTH) };
    Some(ctrl)
}

/// A reference to a hash table bucket containing a `T`.
///
/// This is usually just a pointer to the element itself. However if the element
/// is a ZST, then we instead track the index of the element in the table so
/// that `erase` works properly.
pub struct Bucket<T> {
    // Actually it is pointer to next element than element itself
    // this is needed to maintain pointer arithmetic invariants
    // keeping direct pointer to element introduces difficulty.
    // Using `NonNull` for variance and niche layout
    ptr: NonNull<T>,
}

impl<T> Clone for Bucket<T> {
    #[inline]
    fn clone(&self) -> Self {
        Self { ptr: self.ptr }
    }
}

impl<T> Bucket<T> {
    #[inline]
    unsafe fn from_base_index(base: NonNull<T>, index: usize) -> Self {
        let ptr = if mem::size_of::<T>() == 0 {
            // won't overflow because index must be less than length
            (index + 1) as *mut T
        } else {
            base.as_ptr().sub(index)
        };
        Self {
            ptr: NonNull::new_unchecked(ptr),
        }
    }
    #[inline]
    pub unsafe fn as_ptr(&self) -> *mut T {
        if mem::size_of::<T>() == 0 {
            // Just return an arbitrary ZST pointer which is properly aligned
            mem::align_of::<T>() as *mut T
        } else {
            self.ptr.as_ptr().sub(1)
        }
    }
    #[inline]
    unsafe fn next_n(&self, offset: usize) -> Self {
        let ptr = if mem::size_of::<T>() == 0 {
            (self.ptr.as_ptr() as usize + offset) as *mut T
        } else {
            self.ptr.as_ptr().sub(offset)
        };
        Self {
            ptr: NonNull::new_unchecked(ptr),
        }
    }
    #[inline]
    pub unsafe fn drop(&self) {
        self.as_ptr().drop_in_place();
    }
    #[inline]
    pub unsafe fn read(&self) -> T {
        self.as_ptr().read()
    }
    #[inline]
    pub unsafe fn write(&self, val: T) {
        self.as_ptr().write(val);
    }
    #[inline]
    pub unsafe fn as_ref<'a>(&self) -> &'a T {
        &*self.as_ptr()
    }
    #[inline]
    pub unsafe fn as_mut<'a>(&self) -> &'a mut T {
        &mut *self.as_ptr()
    }
}

/// In-memory Special Purpose Hash Table.
///
/// The table is split up into two lanes, MOD and READ.
/// New insertions or modification of READ lane buckets
/// go into the MOD lane.
///
/// A [TableModIterator] is used to scan all active modified buckets
/// in the MOD lane in order to make them durable in a state backend.
pub struct RawTable<K, V>
where
    K: Key,
    V: Value,
{
    // [mod buckets | read buckets] [mod ctrl | read ctrl]
    // meta bytes for mod:          [meta bytes]
    mod_ctrl: NonNull<u8>,
    /// Mask to get an index from a hash value.
    mod_mask: usize,

    read_ctrl: NonNull<u8>,
    /// Mask to get an index from a hash value.
    read_mask: usize,

    /// Pointer to offset between mod and read buckets.
    mod_bucket: NonNull<u8>,

    /// Meta bytes for the mod lane
    mod_meta: NonNull<u8>,
    /// Counter keeping track of current total of modified buckets
    mod_counter: usize,
    /// Number of elements in the table, only really used by len()
    items: usize,
    // Tell dropck that we own instances of (K, V)
    marker: PhantomData<(K, V)>,
}

impl<K, V> RawTable<K, V>
where
    K: Key,
    V: Value,
{
    /// Allocates a new hash table with the given number of buckets.
    ///
    /// The control bytes are left uninitialized.
    #[inline]
    unsafe fn new_uninitialized(
        mod_lane_buckets: usize,
        read_lane_buckets: usize,
        fallability: Fallibility,
    ) -> Result<Self, CollectionAllocErr> {
        debug_assert!(mod_lane_buckets.is_power_of_two());
        debug_assert!(read_lane_buckets.is_power_of_two());

        // Avoid `Option::ok_or_else` because it bloats LLVM IR.
        let (layout, bucket_offset, ctrl_offset, read_ctrl_offset) =
            match calculate_layout::<(K, V, u64), (K, V)>(mod_lane_buckets, read_lane_buckets) {
                Some(lco) => lco,
                None => return Err(fallability.capacity_overflow()),
            };

        let table_alloc = match NonNull::new(alloc(layout)) {
            Some(ptr) => ptr,
            None => return Err(fallability.alloc_err(layout)),
        };

        let mod_ctrl = NonNull::new_unchecked(table_alloc.as_ptr().add(ctrl_offset));
        let read_ctrl =
            NonNull::new_unchecked(table_alloc.as_ptr().add(ctrl_offset + read_ctrl_offset));
        let mod_bucket = NonNull::new_unchecked(mod_ctrl.as_ptr().sub(ctrl_offset - bucket_offset));

        let layout = match meta_layout(mod_lane_buckets) {
            Some(lco) => lco,
            None => return Err(fallability.capacity_overflow()),
        };
        let mod_meta = match NonNull::new(alloc(layout)) {
            Some(ptr) => ptr,
            None => return Err(fallability.alloc_err(layout)),
        };

        Ok(Self {
            items: 0,
            mod_ctrl,
            read_ctrl,
            read_mask: read_lane_buckets - 1,
            mod_meta,
            mod_counter: 0,
            mod_mask: mod_lane_buckets - 1,
            mod_bucket,
            marker: PhantomData,
        })
    }

    /// Attempts to allocate a new hash table with at least enough capacity
    /// for inserting the given number of elements without reallocating.
    fn try_with_capacity(
        read_capacity: usize,
        mod_capacity: usize,
        fallability: Fallibility,
    ) -> Result<Self, CollectionAllocErr> {
        assert!(read_capacity > 32, "Capacity size must be larger than 32");
        assert!(mod_capacity > 32, "Capacity size must be larger than 32");

        unsafe {
            let result = Self::new_uninitialized(mod_capacity, read_capacity, fallability)?;

            // initialise bytes
            result
                .mod_ctrl(0)
                .write_bytes(EMPTY, result.mod_ctrl_bytes());
            result.ctrl(0).write_bytes(EMPTY, result.read_ctrl_bytes());
            result.meta(0).write_bytes(SAFE, result.mod_ctrl_bytes());

            Ok(result)
        }
    }

    /// Allocates a new hash table with at least enough capacity for inserting
    /// the given number of elements without reallocating.
    pub fn with_capacity(mod_capacity: usize, read_capacity: usize) -> Self {
        Self::try_with_capacity(read_capacity, mod_capacity, Fallibility::Infallible)
            .unwrap_or_else(|_| unsafe { hint::unreachable_unchecked() })
    }

    /// Deallocates the table without dropping any entries.
    #[inline]
    unsafe fn free_buckets(&mut self) {
        let (layout, _, ctrl_offset, _) =
            calculate_layout::<(K, V, u64), (K, V)>(self.mod_buckets(), self.read_buckets())
                .unwrap_or_else(|| hint::unreachable_unchecked());
        dealloc(self.mod_ctrl.as_ptr().sub(ctrl_offset), layout);

        let mod_meta_layout =
            meta_layout(self.mod_buckets()).unwrap_or_else(|| hint::unreachable_unchecked());
        dealloc(self.mod_meta.as_ptr(), mod_meta_layout);
    }

    /// Returns pointer to one past last element of data table.
    #[inline]
    pub unsafe fn data_end(&self) -> NonNull<(K, V)> {
        NonNull::new_unchecked(self.mod_ctrl.as_ptr() as *mut (K, V))
    }

    /// Returns pointer to one past last element of data table.
    #[inline]
    pub unsafe fn mod_data_end(&self) -> NonNull<(K, V, u64)> {
        NonNull::new_unchecked(self.mod_bucket.as_ptr() as *mut (K, V, u64))
    }

    /// Returns a pointer to a control byte.
    #[inline]
    unsafe fn ctrl(&self, index: usize) -> *mut u8 {
        debug_assert!(index < self.read_ctrl_bytes());
        self.read_ctrl.as_ptr().add(index)
    }

    /// Returns a pointer to a mod lane control byte.
    #[inline]
    unsafe fn mod_ctrl(&self, index: usize) -> *mut u8 {
        debug_assert!(index < self.mod_ctrl_bytes());
        self.mod_ctrl.as_ptr().add(index)
    }

    /// Returns a pointer to a meta byte
    #[inline]
    unsafe fn meta(&self, index: usize) -> *mut u8 {
        debug_assert!(index < self.mod_ctrl_bytes());
        self.mod_meta.as_ptr().add(index)
    }

    /// Returns a pointer to an element in the table.
    #[inline]
    pub unsafe fn read_bucket(&self, index: usize) -> Bucket<(K, V)> {
        Bucket::from_base_index(
            NonNull::new_unchecked(self.mod_ctrl.as_ptr() as *mut (K, V)),
            index,
        )
    }

    /// Returns a pointer to an element in the table.
    #[inline]
    pub unsafe fn mod_bucket(&self, index: usize) -> Bucket<(K, V, u64)> {
        Bucket::from_base_index(
            NonNull::new_unchecked(self.mod_bucket.as_ptr() as *mut (K, V, u64)),
            index,
        )
    }

    #[inline]
    fn probe_read(&self, hash: u64) -> ProbeSeq {
        ProbeSeq {
            bucket_mask: self.read_mask,
            pos: h1(hash) & self.read_mask,
            stride: 0,
            probe_counter: 0,
            probe_limit: 16, // TODO: This is somewhat random choice, fix..
        }
    }

    #[inline]
    fn probe_mod(&self, hash: u64) -> ProbeSeq {
        ProbeSeq {
            bucket_mask: self.mod_mask,
            pos: h1(hash) & self.mod_mask,
            stride: 0,
            probe_counter: 0,
            probe_limit: 16, // TODO: This is somewhat random choice, fix..
        }
    }

    #[inline]
    unsafe fn set_meta(&self, index: usize, meta: u8) {
        self.set_lane_byte(index, meta, self.mod_meta.as_ptr(), self.mod_mask);
    }

    #[inline]
    unsafe fn set_mod_ctrl(&self, index: usize, ctrl: u8) {
        debug_assert!(index < self.mod_ctrl_bytes());
        self.set_lane_byte(index, ctrl, self.mod_ctrl.as_ptr(), self.mod_mask);
    }

    #[inline]
    unsafe fn set_read_ctrl(&self, index: usize, ctrl: u8) {
        debug_assert!(index < self.read_ctrl_bytes());
        self.set_lane_byte(index, ctrl, self.read_ctrl.as_ptr(), self.read_mask);
    }

    /// Sets a control byte, and possibly also the replicated control byte at
    /// the end of the array.
    #[inline]
    unsafe fn set_lane_byte(&self, index: usize, ctrl: u8, ptr: *mut u8, mask: usize) {
        // Replicate the first Group::WIDTH control bytes at the end of
        // the array without using a branch:
        // - If index >= Group::WIDTH then index == index2.
        // - Otherwise index2 == self.bucket_mask + 1 + index.
        //
        // The very last replicated control byte is never actually read because
        // we mask the initial index for unaligned loads, but we write it
        // anyways because it makes the set_ctrl implementation simpler.
        //
        // If there are fewer buckets than Group::WIDTH then this code will
        // replicate the buckets at the end of the trailing group. For example
        // with 2 buckets and a group size of 4, the control bytes will look
        // like this:
        //
        //     Real    |             Replicated
        // ---------------------------------------------
        // | [A] | [B] | [EMPTY] | [EMPTY] | [A] | [B] |
        // ---------------------------------------------
        let index2 = ((index.wrapping_sub(Group::WIDTH)) & mask) + Group::WIDTH;
        *ptr.add(index) = ctrl;
        *ptr.add(index2) = ctrl;
    }

    /// Common search function for both MOD and READ Lanes to find an EMPTY or DELETED bucket.
    #[inline]
    unsafe fn find_insert_slot(
        &self,
        probe: impl Iterator<Item = usize>,
        mask: usize,
        ptr: *mut u8,
    ) -> Option<usize> {
        for pos in probe {
            let group = Group::load(ptr.add(pos));
            if let Some(bit) = group.match_empty().lowest_set_bit() {
                let result = (pos + bit) & mask;
                return Some(result);
            }
        }
        None
    }

    /// Inserts a new element into the READ lane.
    #[inline(always)]
    pub fn insert_read_lane(&mut self, hash: u64, record: (K, V)) {
        unsafe {
            if let Some(index) = self.find_insert_slot(
                self.probe_read(hash),
                self.read_mask,
                self.read_ctrl.as_ptr(),
            ) {
                let bucket = self.read_bucket(index);
                self.set_read_ctrl(index, h2(hash));
                bucket.write(record);
            } else {
                // READ lane is full for this particular probe sequence
                //
                // Load 1 Group for the first probe POS and clear all FULL ctrl bytes and then insert
                // `record` into a slot.
                let pos = self.probe_read(hash).next().unwrap();
                let group = Group::load(self.read_ctrl.as_ptr().add(pos));
                for bit in group.match_full() {
                    let index = (pos + bit) & self.read_mask;
                    let bucket = self.read_bucket(index);
                    let _ = bucket.drop();
                    self.set_read_ctrl(index, EMPTY);
                }

                // bit = 0-15, here we pick 0.
                let insert_index = (pos + 0) & self.read_mask;
                let bucket = self.read_bucket(insert_index);
                self.set_read_ctrl(insert_index, h2(hash));
                // write the data to the bucket
                bucket.write(record);
            }
        }
    }

    /// Inserts a new element into the MOD lane.
    ///
    ///
    /// If `find_insert_slot` fails to find a suitable position for insertion,
    /// a ProbeModIterator is then returned in order to make some space.
    #[inline(always)]
    pub fn insert_mod_lane<'a>(
        &'a mut self,
        hash: u64,
        value: (K, V),
    ) -> Option<(ProbeModIterator<K, V>, (K, V))> {
        unsafe {
            let index = match self.find_insert_slot(
                self.probe_mod(hash),
                self.mod_mask,
                self.mod_ctrl.as_ptr(),
            ) {
                Some(index) => index,
                None => {
                    return Some((ProbeModIterator::new(self, hash), value));
                }
            };

            // Set ctrl and meta bytes
            self.set_mod_ctrl(index, h2(hash));
            self.set_meta(index, MODIFIED);

            let bucket = self.mod_bucket(index);
            bucket.write((value.0, value.1, hash));
            self.mod_counter += 1;
            None
        }
    }

    /// Probes in worst case both lanes to find a record to remove.
    #[inline]
    pub fn remove(&mut self, hash: u64, mut eq: impl FnMut((&K, &V)) -> bool) -> Option<(K, V)> {
        unsafe {
            for pos in self.probe_mod(hash) {
                let group = Group::load(self.mod_ctrl(pos));
                for bit in group.match_byte(h2(hash)) {
                    let index = (pos + bit) & self.mod_mask;
                    let bucket = self.mod_bucket(index);
                    let &(ref key, ref value, _) = bucket.as_ref();
                    if likely(eq((key, value))) {
                        // take ownership
                        let (key, value, _) = bucket.read();
                        // clear lane bytes
                        self.set_mod_ctrl(index, EMPTY);
                        self.set_meta(index, SAFE);
                        return Some((key, value));
                    }
                }
            }
            // otherwise attempt to take from READ lane
            self.take_read_lane(hash, eq)
        }
    }

    #[inline(always)]
    pub fn find_mod_lane_mut(
        &mut self,
        hash: u64,
        mut eq: impl FnMut((&K, &V)) -> bool,
    ) -> Option<&mut V> {
        unsafe {
            for pos in self.probe_mod(hash) {
                let group = Group::load(self.mod_ctrl(pos));
                for bit in group.match_byte(h2(hash)) {
                    let index = (pos + bit) & self.mod_mask;
                    let bucket = self.mod_bucket(index);
                    let &mut (ref key, ref mut value, _) = bucket.as_mut();
                    if likely(eq((key, value))) {
                        // If the meta byte is safe, then increase modification
                        // counter as we are setting the meta to MODIFIED_TOUCHED.
                        if is_safe(*self.meta(index)) {
                            self.mod_counter += 1;
                        }

                        self.set_meta(index, MODIFIED_TOUCHED);
                        return Some(value);
                    }
                }
            }
            None
        }
    }

    /// Probes the READ lane and returns an owned bucket record if found.
    ///
    /// Used mainly to migrate a Read-Modify-Write record to the MOD lane.
    #[inline(always)]
    pub fn take_read_lane(
        &mut self,
        hash: u64,
        mut eq: impl FnMut((&K, &V)) -> bool,
    ) -> Option<(K, V)> {
        unsafe {
            for pos in self.probe_read(hash) {
                let group = Group::load(self.ctrl(pos));
                for bit in group.match_byte(h2(hash)) {
                    let index = (pos + bit) & self.read_mask;
                    let bucket = self.read_bucket(index);
                    let &(ref key, ref value) = bucket.as_ref();
                    if likely(eq((key, value))) {
                        self.set_read_ctrl(index, EMPTY);
                        return Some(bucket.read());
                    }
                }
            }
        }
        return None;
    }

    /// Searches for an element in the table.
    #[inline(always)]
    pub fn find(&self, hash: u64, mut eq: impl FnMut((&K, &V)) -> bool) -> Option<(&K, &V)> {
        unsafe {
            // Probe Mod Lane first...
            for pos in self.probe_mod(hash) {
                let group = Group::load(self.mod_ctrl(pos));
                for bit in group.match_byte(h2(hash)) {
                    let index = (pos + bit) & self.mod_mask;
                    let bucket = self.mod_bucket(index);
                    let &(ref key, ref value, _) = bucket.as_ref();
                    if likely(eq((key, value))) {
                        if is_safe(*self.meta(index)) {
                            self.set_meta(index, SAFE_TOUCHED);
                        }
                        return Some((key, value));
                    }
                }
            }

            // Probe read lane
            for pos in self.probe_read(hash) {
                let group = Group::load(self.ctrl(pos));
                for bit in group.match_byte(h2(hash)) {
                    let index = (pos + bit) & self.read_mask;
                    let bucket = self.read_bucket(index);
                    let &(ref key, ref value) = bucket.as_ref();
                    if likely(eq((key, value))) {
                        return Some((key, value));
                    }
                }
            }
        }
        return None;
    }

    /// Returns the number of elements in the table.
    #[inline]
    pub fn len(&self) -> usize {
        self.items
    }

    /// Returns the number of buckets in the MOD lane.
    #[inline]
    pub fn mod_buckets(&self) -> usize {
        self.mod_mask + 1
    }

    /// Returns the number of buckets in the READ lane.
    #[inline]
    pub fn read_buckets(&self) -> usize {
        self.read_mask + 1
    }

    /// Returns the number of control bytes in the READ lane.
    #[inline]
    fn read_ctrl_bytes(&self) -> usize {
        self.read_mask + 1 + Group::WIDTH
    }

    /// Returns the number of control bytes in the MOD lane
    #[inline]
    fn mod_ctrl_bytes(&self) -> usize {
        self.mod_mask + 1 + Group::WIDTH
    }

    /// Returns a TableModIterator that starts scanning the Mod lane for modified buckets.
    ///
    /// Safety: It is up to the caller to properly persist the buckets from the [TableModIterator]
    #[inline]
    pub(crate) unsafe fn iter_modified<'a>(&'a mut self) -> TableModIterator<'a, K, V> {
        TableModIterator::new(self, 0)
    }

    // Returns an iterator over every element in the table. It is up to
    // the caller to ensure that the `RawTable` outlives the `RawIter`.
    // Because we cannot make the `next` method unsafe on the `RawIter`
    // struct, we have to make the `iter` method unsafe.
    #[inline]
    pub unsafe fn mod_lane_iter(&self) -> RawIter<(K, V, u64)> {
        let data = Bucket::from_base_index(self.mod_data_end(), 0);
        RawIter {
            iter: RawIterRange::new(self.mod_ctrl.as_ptr(), data, self.mod_buckets()),
            items: self.mod_buckets(),
        }
    }

    #[inline]
    pub unsafe fn read_lane_iter(&self) -> RawIter<(K, V)> {
        let data = Bucket::from_base_index(self.data_end(), 0);
        RawIter {
            iter: RawIterRange::new(self.read_ctrl.as_ptr(), data, self.read_buckets()),
            items: self.read_buckets(),
        }
    }
}

unsafe impl<K, V> Send for RawTable<K, V>
where
    K: Key + Send,
    V: Value + Send,
{
}
unsafe impl<K, V> Sync for RawTable<K, V>
where
    K: Key + Send,
    V: Value + Send,
{
}

impl<K, V> Drop for RawTable<K, V>
where
    K: Key,
    V: Value,
{
    #[inline]
    fn drop(&mut self) {
        unsafe {
            if mem::needs_drop::<(K, V)>() {
                for item in self.mod_lane_iter() {
                    item.drop();
                }
                for item in self.read_lane_iter() {
                    item.drop();
                }
            }
            self.free_buckets();
        }
    }
}

/// Iterator that returns modified buckets over a ProbeSeq
pub struct ProbeModIterator<'a, K, V>
where
    K: Key,
    V: Value,
{
    pub(crate) iter: RawModIterator<'a, K, V>,
    hash: u64,
    _marker: std::marker::PhantomData<&'a ()>,
}

impl<'a, K, V> ProbeModIterator<'a, K, V>
where
    K: Key,
    V: Value,
{
    fn new(table: &'a mut RawTable<K, V>, hash: u64) -> Self {
        unsafe {
            let mut probe_seq = table.probe_mod(hash);
            let pos = probe_seq.next().unwrap();
            let group = Group::load(table.meta(pos));
            let bitmask = group.match_modified().into_iter();
            let iter = RawModIterator {
                table,
                probe_seq,
                group,
                pos,
                bitmask,
                erased_buckets: 0,
            };
            ProbeModIterator {
                iter,
                hash,
                _marker: PhantomData,
            }
        }
    }
}

impl<'a, K: 'a, V: 'a> Iterator for ProbeModIterator<'a, K, V>
where
    K: Key,
    V: Value,
{
    type Item = (&'a K, &'a V);

    #[inline]
    fn next(&mut self) -> Option<(&'a K, &'a V)> {
        if let Some(bucket) = self.iter.next() {
            Some(bucket)
        } else {
            // NOTE: it may happen that all buckets in the probe
            // were MODIFIED_TOUCHED and were thus converted into SAFE_TOUCHED.
            // If this is the case, we need to iterate over a few SAFE_TOUCHED buckets
            // and erase them from the table.
            if unlikely(self.iter.erased_buckets == 0) {
                let mut cleared_groups = 0;
                let raw_table = &mut self.iter.table;
                for pos in raw_table.probe_mod(self.hash) {
                    unsafe {
                        let group = Group::load(raw_table.meta(pos));
                        let bitmask = group.match_byte(SAFE_TOUCHED);
                        if likely(bitmask.any_bit_set()) {
                            cleared_groups += 1;
                        }

                        for bit in bitmask {
                            let index = (pos + bit) & raw_table.mod_mask;
                            let bucket = raw_table.mod_bucket(index);
                            // take ownership of bucket data
                            let (key, value, hash) = bucket.read();
                            raw_table.insert_read_lane(hash, (key, value));
                            raw_table.set_mod_ctrl(index, EMPTY);
                            raw_table.set_meta(index, SAFE);
                        }

                        // In order to avoid clearing to many SAFE_TOUCHED buckets,
                        // we exit after having cleared from > 1 groups.
                        if cleared_groups > 1 {
                            return None;
                        }
                    };
                }
            }
            None
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

pub struct RawModIterator<'a, K, V>
where
    K: Key,
    V: Value,
{
    /// Mutable Reference to the RawTable
    table: &'a mut RawTable<K, V>,
    /// The Probe Sequence that is followed
    probe_seq: ProbeSeq,
    /// Current group
    group: Group,
    /// Current position of the group
    pos: usize,
    /// The Active BitMask Iterator
    bitmask: BitMaskIter,
    /// Counter keeping track of erased buckets
    ///
    /// If the resulting counter is 0, we must run another
    /// cleaning process and this time erase SAFE_TOUCHED bytes.
    erased_buckets: usize,
}

impl<'a, K, V> Iterator for RawModIterator<'a, K, V>
where
    K: Key,
    V: Value,
{
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<(&'a K, &'a V)> {
        unsafe {
            loop {
                if let Some(bit) = self.bitmask.next() {
                    let index = (self.pos + bit) & self.table.mod_mask;
                    self.table.mod_counter -= 1;
                    let bucket = self.table.mod_bucket(index);
                    let (ref key, ref value, _) = bucket.as_ref();
                    return Some((key, value));
                }

                // Converts the bytes in-place:
                // - SAFE => SAFE,
                // - SAFE_TOUCHED => SAFE
                // - MODIFIED => SAFE
                // - MODIFIED_TOUCHED => SAFE_TOUCHED
                self.group = self.group.convert_mod_to_safe(self.table.meta(self.pos));

                for bit in self.group.match_byte(SAFE) {
                    // for every SAFE meta byte, erase and make space for new inserts..
                    let index = (self.pos + bit) & self.table.mod_mask;
                    let bucket = self.table.mod_bucket(index);
                    self.table.set_mod_ctrl(index, EMPTY);
                    // take ownership of bucket data
                    let (key, value, hash) = bucket.read();
                    self.table.insert_read_lane(hash, (key, value));
                    self.erased_buckets += 1;
                }

                if let Some(pos) = self.probe_seq.next() {
                    self.pos = pos;
                    self.group = Group::load(self.table.meta(self.pos));
                    self.bitmask = self.group.match_modified().into_iter();
                } else {
                    return None;
                }
            }
        }
    }
}

/// Iterator that scans modified buckets from the start of the Mod Lane
pub struct TableModIterator<'a, K, V>
where
    K: Key,
    V: Value,
{
    /// Mutable Reference to the RawTable
    table: &'a mut RawTable<K, V>,
    /// Current group
    group: Group,
    /// Current group position
    pos: usize,
    /// The Active BitMask Iterator
    bitmask: BitMaskIter,
    /// Number of buckets in Table
    buckets: usize,
}

impl<'a, K, V> TableModIterator<'a, K, V>
where
    K: Key,
    V: Value,
{
    fn new(table: &'a mut RawTable<K, V>, pos: usize) -> Self {
        debug_assert_eq!(pos % Group::WIDTH, 0);
        unsafe {
            let group = Group::load_aligned(table.meta(pos));
            let bitmask = group.match_modified().into_iter();
            let buckets = table.mod_buckets();
            TableModIterator {
                table,
                group,
                pos,
                bitmask,
                buckets,
            }
        }
    }
}

impl<'a, K, V> Iterator for TableModIterator<'a, K, V>
where
    K: Key,
    V: Value,
{
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<(&'a K, &'a V)> {
        unsafe {
            loop {
                if let Some(bit) = self.bitmask.next() {
                    let index = (self.pos + bit) & self.table.mod_mask;
                    self.table.mod_counter -= 1;
                    let bucket = self.table.mod_bucket(index);
                    let (ref key, ref value, _) = bucket.as_ref();
                    return Some((key, value));
                }

                // Converts the bytes in-place:
                // - SAFE => SAFE,
                // - SAFE_TOUCHED => SAFE
                // - MODIFIED => SAFE
                // - MODIFIED_TOUCHED => SAFE_TOUCHED
                self.group = self.group.convert_mod_to_safe(self.table.meta(self.pos));

                // If we have reached zero modified buckets,
                // no point in scanning further..
                if self.table.mod_counter == 0 {
                    return None;
                }

                if self.pos < self.buckets {
                    self.pos += Group::WIDTH;
                    self.group = Group::load_aligned(self.table.meta(self.pos));
                    self.bitmask = self.group.match_modified().into_iter();
                } else {
                    return None;
                }
            }
        }
    }
}

/// Iterator over a sub-range of a table. Unlike `RawIter` this iterator does
/// not track an item count.
pub(crate) struct RawIterRange<T> {
    // Mask of full buckets in the current group. Bits are cleared from this
    // mask as each element is processed.
    current_group: BitMask,

    // Pointer to the buckets for the current group.
    data: Bucket<T>,

    // Pointer to the next group of control bytes,
    // Must be aligned to the group size.
    next_ctrl: *const u8,

    // Pointer one past the last control byte of this range.
    end: *const u8,
}

impl<T> RawIterRange<T> {
    /// Returns a `RawIterRange` covering a subset of a table.
    ///
    /// The control byte address must be aligned to the group size.
    #[inline]
    unsafe fn new(ctrl: *const u8, data: Bucket<T>, len: usize) -> Self {
        debug_assert_ne!(len, 0);
        debug_assert_eq!(ctrl as usize % Group::WIDTH, 0);
        let end = ctrl.add(len);

        // Load the first group and advance ctrl to point to the next group
        let current_group = Group::load_aligned(ctrl).match_full();
        let next_ctrl = ctrl.add(Group::WIDTH);

        Self {
            current_group,
            data,
            next_ctrl,
            end,
        }
    }
}

// We make raw iterators unconditionally Send and Sync, and let the PhantomData
// in the actual iterator implementations determine the real Send/Sync bounds.
unsafe impl<T> Send for RawIterRange<T> {}
unsafe impl<T> Sync for RawIterRange<T> {}

impl<T> Clone for RawIterRange<T> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            data: self.data.clone(),
            next_ctrl: self.next_ctrl,
            current_group: self.current_group,
            end: self.end,
        }
    }
}

impl<T> Iterator for RawIterRange<T> {
    type Item = Bucket<T>;

    #[inline]
    fn next(&mut self) -> Option<Bucket<T>> {
        unsafe {
            loop {
                if let Some(index) = self.current_group.lowest_set_bit() {
                    self.current_group = self.current_group.remove_lowest_bit();
                    return Some(self.data.next_n(index));
                }

                if self.next_ctrl >= self.end {
                    return None;
                }

                // We might read past self.end up to the next group boundary,
                // but this is fine because it only occurs on tables smaller
                // than the group size where the trailing control bytes are all
                // EMPTY. On larger tables self.end is guaranteed to be aligned
                // to the group size (since tables are power-of-two sized).
                self.current_group = Group::load_aligned(self.next_ctrl).match_full();
                self.data = self.data.next_n(Group::WIDTH);
                self.next_ctrl = self.next_ctrl.add(Group::WIDTH);
            }
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        // We don't have an item count, so just guess based on the range size.
        (
            0,
            Some(unsafe { offset_from(self.end, self.next_ctrl) + Group::WIDTH }),
        )
    }
}

impl<T> FusedIterator for RawIterRange<T> {}

/// Iterator which returns a raw pointer to every full bucket in the table.
pub struct RawIter<T> {
    pub(crate) iter: RawIterRange<T>,
    items: usize,
}

impl<T> Clone for RawIter<T> {
    #[inline]
    fn clone(&self) -> Self {
        Self {
            iter: self.iter.clone(),
            items: self.items,
        }
    }
}

impl<T> Iterator for RawIter<T> {
    type Item = Bucket<T>;

    #[inline]
    fn next(&mut self) -> Option<Bucket<T>> {
        if let Some(b) = self.iter.next() {
            self.items -= 1;
            Some(b)
        } else {
            // We don't check against items == 0 here to allow the
            // compiler to optimize away the item count entirely if the
            // iterator length is never queried.
            debug_assert_eq!(self.items, 0);
            None
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.items, Some(self.items))
    }
}

impl<T> ExactSizeIterator for RawIter<T> {}
impl<T> FusedIterator for RawIter<T> {}
