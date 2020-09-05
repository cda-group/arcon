// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

use super::{region::BitMask, table::EMPTY};

#[cfg(target_arch = "x86")]
use core::arch::x86;
#[cfg(target_arch = "x86_64")]
use core::arch::x86_64 as x86;

const EMPTY_META: u32 = 0;

/// A Group consists of a total of 32 ctrl bytes,
/// and `mod_bits` and `ref_bits` to maintain
/// information of every ctrl byte.
///
/// Groups are padded to a 64 bytes cache line
#[repr(align(64))]
#[derive(Debug, Copy, Clone)]
pub struct Group {
    mod_bits: u32,
    ref_bits: u32,
    ctrl: [u8; 32],
}

impl Group {
    pub fn new() -> Group {
        Group {
            mod_bits: EMPTY_META,
            ref_bits: EMPTY_META,
            ctrl: [EMPTY; 32],
        }
    }

    #[inline(always)]
    //#[cfg(target_feature = "avx2")]
    pub fn match_byte(&self, byte: u8) -> BitMask {
        unsafe {
            // TODO: make aligned load?
            let ctrl = x86::_mm256_loadu_si256(self.ctrl.as_ptr() as *const _);
            let cmp = x86::_mm256_cmpeq_epi8(ctrl, x86::_mm256_set1_epi8(byte as i8));
            BitMask(x86::_mm256_movemask_epi8(cmp) as u32)
        }
    }

    #[inline]
    pub fn reset_mod_bits(&mut self) {
        self.mod_bits = EMPTY_META;
    }

    #[inline]
    pub fn reset_ref_bits(&mut self) {
        self.ref_bits = EMPTY_META;
    }

    #[inline(always)]
    pub fn set_ctrl(&mut self, pos: usize, ctrl: u8) { 
        debug_assert!(pos <= 31);
        self.ctrl[pos] = ctrl;
    }

    #[inline(always)]
    pub fn set_modified(&mut self, bit: usize) { 
        debug_assert!(bit <= 31);
        let mask = 1 << bit;
        self.mod_bits |= mask;
    }

    #[inline(always)]
    pub fn clear_modified(&mut self, bit: usize) {
        debug_assert!(bit <= 31);
        let mask = !(1 << bit);
        self.mod_bits &= mask;
    }

    #[inline(always)]
    pub fn set_referenced(&mut self, bit: usize) {
        debug_assert!(bit <= 31);
        let mask = 1 << bit;
        self.ref_bits |= mask;
    }

    #[inline(always)]
    pub fn clear_referenced(&mut self, bit: usize) {
        debug_assert!(bit <= 31);
        let mask = !(1 << bit);
        self.ref_bits &= mask;
    }

    /// Returns a `BitMask` indicating whether any ctrl byte
    /// has both its reference and modification bit set.
    #[inline]
    pub fn match_ref_and_mod(&self, index: usize) -> BitMask {
        BitMask(self.ref_bits & self.mod_bits)
    }

    /// Returns a `BitMask` indicating whether any ctrl byte
    /// is modified.
    #[inline]
    pub fn match_modified(&self) -> BitMask {
        BitMask(self.mod_bits)
    }

    /// Returns a `BitMask` indicating whether any ctrl byte
    /// is modified but not referenced.
    #[inline]
    pub fn match_mod_only(&self) -> BitMask {
        BitMask(!(self.ref_bits) & self.mod_bits)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_modified() {
        let mut group = Group::new();
    }
}
