// Copyright (c) 2016 Amanieu d'Antras
// SPDX-License-Identifier: MIT

// Branch prediction hints.

#[cfg(feature = "nightly")]
pub(crate) use core::intrinsics::{likely, unlikely};
#[cfg(not(feature = "nightly"))]
#[inline]
pub(crate) fn likely(b: bool) -> bool {
    b
}
#[cfg(not(feature = "nightly"))]
#[inline]
pub(crate) fn unlikely(b: bool) -> bool {
    b
}
