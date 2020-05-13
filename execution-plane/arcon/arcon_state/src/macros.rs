/// Macro to implement [`Bundle`]s.
///
/// [`Bundle`]: trait.Bundle.html
///
/// # Examples
///
/// ```
/// # extern crate arcon_state;
/// # use arcon_state::*;
/// # use arcon_state::in_memory::*;
/// # let mut backend = InMemory::restore_or_create(&Default::default(), Default::default()).unwrap();
/// # let mut backend_session = backend.session();
/// arcon_state::bundle! {
///     /// My test bundle
///     pub struct MyTestBundle<T: Value> {
///         /// some value
///         value: Handle<ValueState<T>>,
///         /// some map
///         map: Handle<MapState<String, f64>, i32, u32>
///     }
/// }
///
/// impl<T: Value> MyTestBundle<T> {
///     fn new() -> Self {
///         MyTestBundle {
///             value: Handle::value("value"),
///             map: Handle::map("map").with_item_key(-1).with_namespace(0)
///         }       
///     }   
/// }
///
/// let mut bundle = MyTestBundle::<u32>::new();
/// // Usually a bundle should be registered by an arcon node, but we'll do it manually here
/// bundle.register_states(&mut unsafe { RegistrationToken::new(&mut backend_session) });
///
/// let mut active_bundle = bundle.activate(&mut backend_session);
/// let mut value_handle = active_bundle.value();
/// value_handle.set(3).unwrap();
/// println!("value is {:?}", value_handle.get().unwrap());
/// let mut map_handle = active_bundle.map();
/// map_handle.insert("foo".into(), 0.5).unwrap();
/// println!("map[foo] is {:?}", map_handle.get(&"foo".into()).unwrap())
/// ```
/// Note that if you want to have more bounds on your generic parameter, you'll have to use slightly
/// different syntax than what you're used to from regular Rust:
/// ```ignore
/// pub struct ParamWithManyBounds<T: Default: Clone> { // instead of T: Default + Clone
/// ```
/// Also, where clauses are unsupported.
// this might be nicer as a derive macro, but IntelliJ Rust can see through macro_rules!, so this
// version is nicer for people that use that
#[macro_export]
macro_rules! bundle {
    (
        $(#[$bundle_meta:meta])*
        $vis:vis struct $name:ident $(<
            $($generic_lifetime_param:lifetime),*$(,)?
            $($generic_param:ident $(: $first_bound:path $(: $other_bounds:path)*)?),*$(,)?
        >)? {$(
            $(#[$state_meta:meta])*
            $state_name:ident : Handle<$state_type:ty $(, $item_key_type:ty $(, $namespace_type:ty)?)?>
        ),*$(,)?}
    ) => {
        $(#[$bundle_meta])*
        $vis struct $name$(<
            $($generic_lifetime_param,)*
            $($generic_param $(: $first_bound $(+ $other_bounds)*)?,)*
        >)? {
            $(
                $(#[$state_meta])*
                $state_name : $crate::Handle<$state_type $(, $item_key_type $(, $namespace_type)?)?>,
            )*
        }

        const _: () = {
            #[allow(missing_debug_implementations)]
            pub struct Active<
                '__bundle, '__backend, $($($generic_lifetime_param,)*)?
                __B: $crate::Backend,
                $($($generic_param $(: $first_bound $(+ $other_bounds)*)?,)*)?
            > {
                backend: &'__backend mut __B,
                inner: &'__bundle mut $name$(<
                    $($generic_lifetime_param,)*
                    $($generic_param,)*
                >)?,
            }

            impl<
                '__bundle, '__backend, $($($generic_lifetime_param,)*)?
                __B: $crate::Backend,
                $($($generic_param $(: $first_bound $(+ $other_bounds)*)?,)*)?
            > Active<
                '__bundle, '__backend, $($($generic_lifetime_param,)*)?
                __B, $($($generic_param,)*)?
            > {$(
                $(#[$state_meta])*
                #[inline]
                pub fn $state_name(&mut self) -> $crate::handles::ActiveHandle<__B,
                    $state_type $(, $item_key_type $(, $namespace_type)?)?
                > {
                    self.inner.$state_name.activate(&mut self.backend)
                }
            )*}

            impl<
                '__this, '__backend, $($($generic_lifetime_param,)*)?
                __B: $crate::Backend + '__backend,
                $($($generic_param : '__this $(+ $first_bound $(+ $other_bounds)*)?,)*)?
            > $crate::Bundle<'__this, '__backend, __B> for $name$(<
                $($generic_lifetime_param,)* $($generic_param,)*
            >)? {
                type Active = Active<
                    '__this, '__backend, $($($generic_lifetime_param,)*)?
                    __B, $($($generic_param,)*)?
                >;

                fn register_states<'s>(
                    &mut self,
                    registration_token: &mut $crate::RegistrationToken<'s, '__backend, __B>
                ) {
                    $(self.$state_name.register(registration_token);)*
                }

                fn activate<'s>(
                    &'__this mut self,
                    session: &'__backend mut $crate::Session<'s, __B>,
                ) -> Self::Active {
                    Active {
                        backend: session.backend,
                        inner: self,
                    }
                }
            }
        };
    };
}
