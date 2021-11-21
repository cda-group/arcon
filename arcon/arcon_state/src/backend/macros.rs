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
    ($($body:tt)*) => {
        unreachable!()
    };
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
    ($($body:tt)*) => {
        unreachable!()
    };
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
///     |SB| Box::new(SB::create("test_dir".as_ref(), "testDB".to_string()).unwrap()) as Box<dyn Any>
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
            $crate::cfg_if_rocks!(@pat Rocks) => {
                $crate::cfg_if_rocks! {
                    type $type_ident = $crate::backend::rocks::Rocks;
                    $body
                }
            }
        }
    }};
}
