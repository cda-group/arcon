use kompact::prelude::SerId;

pub const UNIT_ID: SerId = 48;

pub const NEVER_ID: SerId = 49;

// Serialisation IDs for Arcon primitives
#[cfg(feature = "unsafe_flight")]
pub const UNSAFE_U32_ID: SerId = 50;
pub const RELIABLE_U32_ID: SerId = 51;

#[cfg(feature = "unsafe_flight")]
pub const UNSAFE_U64_ID: SerId = 52;
pub const RELIABLE_U64_ID: SerId = 53;

#[cfg(feature = "unsafe_flight")]
pub const UNSAFE_I32_ID: SerId = 54;
pub const RELIABLE_I32_ID: SerId = 55;

#[cfg(feature = "unsafe_flight")]
pub const UNSAFE_I64_ID: SerId = 56;
pub const RELIABLE_I64_ID: SerId = 57;

#[cfg(feature = "unsafe_flight")]
pub const UNSAFE_F32_ID: SerId = 58;
pub const RELIABLE_F32_ID: SerId = 59;

#[cfg(feature = "unsafe_flight")]
pub const UNSAFE_F64_ID: SerId = 60;
pub const RELIABLE_F64_ID: SerId = 61;

#[cfg(feature = "unsafe_flight")]
pub const UNSAFE_STRING_ID: SerId = 62;
pub const RELIABLE_STRING_ID: SerId = 63;

#[cfg(feature = "unsafe_flight")]
pub const UNSAFE_BOOLEAN_ID: SerId = 64;
pub const RELIABLE_BOOLEAN_ID: SerId = 65;
