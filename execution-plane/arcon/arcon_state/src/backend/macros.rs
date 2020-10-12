// Copyright (c) 2020, KTH Royal Institute of Technology.
// SPDX-License-Identifier: AGPL-3.0-only



// region helper macros for `with_backend_type!`
#[doc(hidden)]
#[cfg(feature = "rocks")]
#[macro_export]
macro_rules! cfg_if_rocks {
    (@pat $i: pat) => {
        $i
    };
    ($($body:tt)*) => {
        $($body)*
    };
}

#[doc(hidden)]
#[cfg(not(feature = "rocks"))]
#[macro_export]
macro_rules! cfg_if_rocks {
    (@pat $i: pat) => {
        _
    };
    ($($body:tt)*) => { unreachable!() };
}

#[doc(hidden)]
#[cfg(feature = "sled")]
#[macro_export]
macro_rules! cfg_if_sled {
    (@pat $i: pat) => {
        $i
    };
    ($($body:tt)*) => {
        $($body)*
    };
}

#[doc(hidden)]
#[cfg(not(feature = "sled"))]
#[macro_export]
macro_rules! cfg_if_sled {
    (@pat $i: pat) => {
        _
    };
    ($($body:tt)*) => { unreachable!() };
}

#[doc(hidden)]
#[cfg(all(feature = "faster", target_os = "linux"))]
#[macro_export]
macro_rules! cfg_if_faster {
    (@pat $i: pat) => {
        $i
    };
    ($($body:tt)*) => {
        $($body)*
    };
}

#[doc(hidden)]
#[cfg(not(all(feature = "faster", target_os = "linux")))]
#[macro_export]
macro_rules! cfg_if_faster {
    (@pat $i: pat) => {
        _
    };
    ($($body:tt)*) => { unreachable!() };
}
// endregion

/// Runs `$body` with `$type_ident` bound to a concrete state backend type based on the runtime
/// value of `$type_value` (which has to be of type [BackendType](crate::BackendType)). The caller
/// has to make sure that the return type of this expression is the same regardless of what
/// `$type_ident` happens to be bound to.
///
/// # Examples
///
/// ```no_run
/// # extern crate arcon_state;
/// # use arcon_state::backend::{BackendType, Backend};
/// # use arcon_state::with_backend_type;
/// # use std::any::Any;
/// let runtime_type = BackendType::Sled;
/// let boxed: Box<dyn Any> = with_backend_type!(runtime_type,
///     |SB| Box::new(SB::create("test_dir".as_ref()).unwrap()) as Box<dyn Any>
/// );
/// ```
#[macro_export]
macro_rules! with_backend_type {
    ($type_value:expr, |$type_ident:ident| $body:expr) => {{
        use $crate::backend::BackendType::*;
        #[allow(unreachable_patterns)]
        match $type_value {
            $crate::cfg_if_sled!(@pat Sled) => {
                $crate::cfg_if_sled! {
                    type $type_ident = $crate::backend::sled::Sled;
                    $body
                }
            }
            /*
            InMemory => {
                type $type_ident = $crate::backend::in_memory::InMemory;
                $body
            }
            MeteredInMemory => {
                type $type_ident = $crate::metered::Metered<$crate::in_memory::InMemory>;
                $body
            }
            */
            $crate::cfg_if_rocks!(@pat Rocks) => {
                $crate::cfg_if_rocks! {
                    type $type_ident = $crate::backend::rocks::Rocks;
                    $body
                }
            }
            /*
            $crate::cfg_if_rocks!(@pat MeteredRocks) => {
                $crate::cfg_if_rocks! {
                    type $type_ident = $crate::backend::metered::Metered<$crate::rocks::Rocks>;
                    $body
                }
            }
            */
            /*
            $crate::cfg_if_sled!(@pat MeteredSled) => {
                $crate::cfg_if_sled! {
                    type $type_ident = $crate::backend::metered::Metered<$crate::sled::Sled>;
                    $body
                }
            }
            */
            $crate::cfg_if_faster!(@pat Faster) => {
                $crate::cfg_if_faster! {
                    type $type_ident = $crate::backend::faster::Faster;
                    $body
                }
            }
            /*
            $crate::cfg_if_faster!(@pat MeteredFaster) => {
                $crate::cfg_if_faster! {
                    type $type_ident = $crate::backend::metered::Metered<$crate::faster::Faster>;
                    $body
                }
            }
            */
        }
    }};
}
