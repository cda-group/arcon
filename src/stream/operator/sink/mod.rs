// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only

pub mod local_file;

#[cfg(feature = "socket")]
#[allow(dead_code)]
pub mod socket;

//#[cfg(feature = "kafka")]
//pub mod kafka;
