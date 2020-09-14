// Copyright (c) 2016 Amanieu d'Antras
// SPDX-License-Identifier: MIT

use super::{
    bitmask::BitMask,
    table::{EMPTY, MODIFIED_TOUCHED, SAFE_TOUCHED},
};
use core::mem;

#[cfg(target_arch = "x86")]
use core::arch::x86;
#[cfg(target_arch = "x86_64")]
use core::arch::x86_64 as x86;

pub type BitMaskWord = u16;
pub const BITMASK_STRIDE: usize = 1;
pub const BITMASK_MASK: BitMaskWord = 0xffff;

/// Abstraction over a group of control bytes which can be scanned in
/// parallel.
///
/// This implementation uses a 128-bit SSE value.
#[derive(Copy, Clone)]
pub struct Group(x86::__m128i);

// FIXME: https://github.com/rust-lang/rust-clippy/issues/3859
#[allow(clippy::use_self)]
impl Group {
    /// Number of bytes in the group.
    pub const WIDTH: usize = mem::size_of::<Self>();

    /// Returns a full group of empty bytes, suitable for use as the initial
    /// value for an empty hash table. This value is explicitly declared as
    /// a static variable to ensure the address is consistent across dylibs.
    ///
    /// This is guaranteed to be aligned to the group size.
    pub fn static_empty() -> &'static [u8] {
        union AlignedBytes {
            _align: Group,
            bytes: [u8; Group::WIDTH],
        };
        static ALIGNED_BYTES: AlignedBytes = AlignedBytes {
            bytes: [EMPTY; Group::WIDTH],
        };
        unsafe { &ALIGNED_BYTES.bytes }
    }

    /// Loads a group of bytes starting at the given address.
    #[inline]
    #[allow(clippy::cast_ptr_alignment)] // unaligned load
    pub unsafe fn load(ptr: *const u8) -> Self {
        Group(x86::_mm_loadu_si128(ptr as *const _))
    }

    /// Loads a group of bytes starting at the given address, which must be
    /// aligned to `mem::align_of::<Group>()`.
    #[inline]
    #[allow(clippy::cast_ptr_alignment)]
    pub unsafe fn load_aligned(ptr: *const u8) -> Self {
        // FIXME: use align_offset once it stabilizes
        debug_assert_eq!(ptr as usize & (mem::align_of::<Self>() - 1), 0);
        Group(x86::_mm_load_si128(ptr as *const _))
    }

    /// Returns a `BitMask` indicating all bytes in the group which have
    /// the given value.
    #[inline]
    pub fn match_byte(self, byte: u8) -> BitMask {
        #[allow(
            clippy::cast_possible_wrap, // byte: u8 as i8
            // byte: i32 as u16
            //   note: _mm_movemask_epi8 returns a 16-bit mask in a i32, the
            //   upper 16-bits of the i32 are zeroed:
            clippy::cast_sign_loss,
            clippy::cast_possible_truncation
        )]
        unsafe {
            let cmp = x86::_mm_cmpeq_epi8(self.0, x86::_mm_set1_epi8(byte as i8));
            BitMask(x86::_mm_movemask_epi8(cmp) as u16)
        }
    }

    /// Returns a `BitMask` indicating all bytes in the group which are
    /// `EMPTY`.
    #[inline]
    pub fn match_empty(self) -> BitMask {
        self.match_byte(EMPTY)
    }

    /// Returns a `BitMask` indicating all bytes in the group which are
    /// `EMPTY` or `DELETED`.
    #[inline]
    pub fn match_empty_or_deleted(self) -> BitMask {
        #[allow(
            // byte: i32 as u16
            //   note: _mm_movemask_epi8 returns a 16-bit mask in a i32, the
            //   upper 16-bits of the i32 are zeroed:
            clippy::cast_sign_loss,
            clippy::cast_possible_truncation
        )]
        unsafe {
            // A byte is EMPTY or DELETED iff the high bit is set
            BitMask(x86::_mm_movemask_epi8(self.0) as u16)
        }
    }

    /// Returns a `BitMask` indicating all bytes in the group which are full.
    #[inline]
    pub fn match_full(&self) -> BitMask {
        self.match_empty_or_deleted().invert()
    }

    /// Returns a `BitMask` indicating all bytes in the group which are modified.
    #[inline]
    pub fn match_modified(&self) -> BitMask {
        // NOTE: A modified meta byte has the high bit set to 1
        //       thus we can simply run the same "match_empty_or_deleted" function
        //       as it tests for the same thing.
        self.match_empty_or_deleted()
    }

    /// Performs the following transformation on all bytes in the group:
    /// - SAFE => SAFE,
    /// - SAFE_TOUCHED => SAFE
    /// - MODIFIED => SAFE
    /// - MODIFIED_TOUCHED => SAFE_TOUCHED
    #[inline]
    pub fn convert_mod_to_safe(self, ptr: *mut u8) -> Group {
        // All bytes that match MODIFIED_TOUCHED are set to 1111_1111
        // in the resulting cmp_eq_epi8. The remainder will show as 0000_0000.
        //
        // Run bitwise AND operation using SAFE_TOUCHED to get new bytes
        // 0000_0000 & 0000_0010 => 0000_0000 // SAFE
        // 1111_1111 & 0000_0010 => 0000_0010 // SAFE_TOUCHED
        unsafe {
            let mod_touched_eq =
                x86::_mm_cmpeq_epi8(x86::_mm_set1_epi8(MODIFIED_TOUCHED as i8), self.0);
            let transformed_group = x86::_mm_and_si128(
                mod_touched_eq,
                x86::_mm_set1_epi8(SAFE_TOUCHED as i8), // SAFE: 0000_0010
            );
            // Store the resulting `_mm_and_si128` at `ptr` which is the current group.
            x86::_mm_storeu_si128(ptr as *mut _, transformed_group);
            Group(transformed_group)
        }
    }
}
