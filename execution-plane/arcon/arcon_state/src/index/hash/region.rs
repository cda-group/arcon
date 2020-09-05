use super::table::{h1, h2, Bucket};
use crate::{
    hint::likely,
    index::hash::{group::Group, table::EMPTY},
};
use core::{alloc::Layout, mem, ptr::NonNull};
use std::{
    alloc::{alloc, dealloc, handle_alloc_error},
    cell::UnsafeCell,
};

const GROUPS: usize = 8;
const GROUP_SHIFT: usize = 32;
const BUCKETS: usize = GROUPS * GROUP_SHIFT;

pub struct Region<T: Clone> {
    // 1 group is represents 32 buckets.
    // One region holds 8 groups, 256 buckets, which is 8 cache lines.
    groups: [Group; GROUPS],
    region_mask: usize,
    buckets: Box<[UnsafeCell<T>]>,
    buckets_left: usize,
}

impl<T: Clone> Region<T> {
    pub fn new() -> Self {
        let mut buckets: Vec<UnsafeCell<T>> = Vec::with_capacity(BUCKETS);
        unsafe { buckets.set_len(BUCKETS) };

        Self {
            groups: [Group::new(); GROUPS],
            region_mask: BUCKETS - 1,
            buckets: buckets.into_boxed_slice(),
            buckets_left: BUCKETS,
        }
    }

    #[inline]
    pub fn is_full(&self) -> bool {
        self.buckets_left == 0
    }

    /// Searches for an element in the region
    ///
    /// If found, it will only set its reference bit.
    #[inline]
    pub unsafe fn find(&mut self, hash: u64, mut eq: impl FnMut(&T) -> bool) -> Option<&T> {
        let target_byte = h2(hash);

        let mut group_max: usize = 0;
        for group in &mut self.groups {
            for bit in group.match_byte(target_byte) {
                let index = (bit + group_max) & self.region_mask;
                let bucket = &*self.buckets[index].get();
                if likely(eq(bucket)) {
                    group.set_referenced(bit);
                    return Some(bucket);
                }
            }

            group_max += GROUP_SHIFT;
        }

        return None;
    }

    /// Searches for an element in the region
    ///
    /// If found, it will set both its reference and modification bits.
    ///
    /// Note that we simply assume that the caller will modify the underlying element.
    #[inline]
    pub unsafe fn find_mut(&mut self, hash: u64, mut eq: impl FnMut(&T) -> bool) -> Option<&mut T> {
        let target_byte = h2(hash);

        let mut group_max: usize = 0;
        // In the worst case, scan over 8 groups.
        // Should still be fast due to the sequential
        // access of groups (prefetching + utilising full cache lines).
        for group in &mut self.groups {
            for bit in group.match_byte(target_byte) {
                let index = (bit + group_max) & self.region_mask;
                let bucket = &mut *self.buckets[index].get();
                if likely(eq(bucket)) {
                    group.set_modified(bit);
                    group.set_referenced(bit);
                    return Some(bucket);
                }
            }
            group_max += GROUP_SHIFT;
        }

        return None;
    }

    #[inline]
    pub unsafe fn insert(&mut self, hash: u64, record: T) {
        let mut group_max: usize = 0;
        for group in &mut self.groups {
            for bit in group.match_byte(EMPTY) {
                let index = (bit + group_max) & self.region_mask;
                group.set_ctrl(bit, h2(hash));
                group.set_modified(bit);
                self.buckets[index] = UnsafeCell::new(record);
                // perhaps saturating_sub
                self.buckets_left -= 1;
                return;
            }
            group_max += GROUP_SHIFT;
        }

        // should not be reached
        unreachable!();
    }

    #[inline]
    pub unsafe fn evict_mod_bucket(&mut self) -> &T {
        // LRU Approx algo
        let mut group_max: usize = 0;
        for group in &mut self.groups {
            // Match (0, 1), non-referenced bucket, but modified
            for bit in group.match_mod_only() {
                let index = (bit + group_max) & self.region_mask;
                // set ctrl byte back to EMPTY
                group.set_ctrl(bit, EMPTY);
                // clear mod bit
                group.clear_modified(bit);
                self.buckets_left += 1;
                let bucket = &*self.buckets[index].get();

                return bucket;
            }
            group_max += GROUP_SHIFT;
        }

        unreachable!();
    }
}

#[cfg(feature = "nightly")]
use core::intrinsics;

pub type BitMaskWord = u32;
pub const BITMASK_STRIDE: usize = 1;
pub const BITMASK_MASK: BitMaskWord = 0xffff;

#[derive(Copy, Clone)]
pub struct BitMask(pub BitMaskWord);

#[allow(clippy::use_self)]
impl BitMask {
    /// Returns a new `BitMask` with all bits inverted.
    #[inline]
    #[must_use]
    pub fn invert(self) -> Self {
        BitMask(self.0 ^ BITMASK_MASK)
    }

    /// Returns a new `BitMask` with the lowest bit removed.
    #[inline]
    #[must_use]
    pub fn remove_lowest_bit(self) -> Self {
        BitMask(self.0 & (self.0 - 1))
    }
    /// Returns whether the `BitMask` has at least one set bit.
    #[inline]
    #[allow(dead_code)]
    pub fn any_bit_set(self) -> bool {
        self.0 != 0
    }

    /// Returns the first set bit in the `BitMask`, if there is one.
    #[inline]
    pub fn lowest_set_bit(self) -> Option<usize> {
        if self.0 == 0 {
            None
        } else {
            Some(unsafe { self.lowest_set_bit_nonzero() })
        }
    }

    /// Returns the first set bit in the `BitMask`, if there is one. The
    /// bitmask must not be empty.
    #[inline]
    #[cfg(feature = "nightly")]
    pub unsafe fn lowest_set_bit_nonzero(self) -> usize {
        intrinsics::cttz_nonzero(self.0) as usize / BITMASK_STRIDE
    }
    #[inline]
    #[cfg(not(feature = "nightly"))]
    pub unsafe fn lowest_set_bit_nonzero(self) -> usize {
        self.trailing_zeros()
    }

    /// Returns the number of trailing zeroes in the `BitMask`.
    #[inline]
    pub fn trailing_zeros(self) -> usize {
        // ARM doesn't have a trailing_zeroes instruction, and instead uses
        // reverse_bits (RBIT) + leading_zeroes (CLZ). However older ARM
        // versions (pre-ARMv7) don't have RBIT and need to emulate it
        // instead. Since we only have 1 bit set in each byte on ARM, we can
        // use swap_bytes (REV) + leading_zeroes instead.
        if cfg!(target_arch = "arm") && BITMASK_STRIDE % 8 == 0 {
            self.0.swap_bytes().leading_zeros() as usize / BITMASK_STRIDE
        } else {
            self.0.trailing_zeros() as usize / BITMASK_STRIDE
        }
    }

    /// Returns the number of leading zeroes in the `BitMask`.
    #[inline]
    pub fn leading_zeros(self) -> usize {
        self.0.leading_zeros() as usize / BITMASK_STRIDE
    }
}

impl IntoIterator for BitMask {
    type Item = usize;
    type IntoIter = BitMaskIter;

    #[inline]
    fn into_iter(self) -> BitMaskIter {
        BitMaskIter(self)
    }
}

/// Iterator over the contents of a `BitMask`, returning the indicies of set
/// bits.
pub struct BitMaskIter(BitMask);

impl Iterator for BitMaskIter {
    type Item = usize;

    #[inline]
    fn next(&mut self) -> Option<usize> {
        let bit = self.0.lowest_set_bit()?;
        self.0 = self.0.remove_lowest_bit();
        Some(bit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn region_test() {
        let mask = BitMask(u32::max_value());
        let mut region = Region::<(u64, u64)>::new();
        let hash = 32381;
        let value = 20;
        for i in 0..256 {
            let key = i as u64;
            let bucket = unsafe { region.insert(i as u64, (key, value)) };
        }
        assert_eq!(region.is_full(), true);
        let read = unsafe { region.find(0 as u64, |x| (0, value).eq(&x)) };
        let read = unsafe { region.find(1 as u64, |x| (1, value).eq(&x)) };
        let evicted = unsafe { region.evict_mod_bucket() };
        //println!("{:?}", evicted);
        //region.print_modified();
    }
}
