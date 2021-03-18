/// Test basic functionality.
#[cfg(test)]
mod basic {

    #[arcon::proto]
    struct Point {
        x: i32,
        y: i32,
    }

    #[arcon::proto]
    enum Foo {
        Bar(i32),
        Baz(f32),
    }

    #[test]
    fn test() {
        let _p = Point { x: 0, y: 1 };
        let _f = Foo {
            this: Some(FooEnum::Bar(5)),
        };
    }
}

/// Test basic functionality with Arcon.
#[cfg(test)]
mod with_arcon {
    use arcon::prelude::Arcon;

    #[arcon::proto]
    #[cfg_attr(feature = "arcon_serde", derive(serde::Serialize, serde::Deserialize))]
    #[arcon(reliable_ser_id = 1, unsafe_ser_id = 2, version = 1)]
    #[derive(Arcon, Clone, abomonation_derive::Abomonation)]
    struct Point {
        x: i32,
        y: i32,
    }

    #[arcon::proto]
    enum Foo {
        Bar(i32),
        Baz(f32),
    }

    #[test]
    fn test() {
        let _p = Point { x: 0, y: 1 };
        let _f = Foo {
            this: Some(FooEnum::Bar(5)),
        };
    }
}

/// Test enum-wrapping shortcut.
#[cfg(test)]
mod wrap {

    #[arcon::proto]
    enum Foo {
        Bar(i32),
        Baz(f32),
    }

    #[test]
    fn test() {
        let _f: Foo = FooEnum::Bar(5).wrap();
    }
}

/// Test nested structs.
mod nested_structs {

    #[arcon::proto]
    struct A {
        b: B,
    }

    #[arcon::proto]
    struct B {
        c: i32,
    }

    #[test]
    fn test() {
        let _a = A { b: B { c: 3 } };
    }
}

/// Test nested datatypes.
mod nested_enums {

    #[arcon::proto]
    enum A {
        B(B),
        C(C),
    }

    #[arcon::proto]
    struct B {
        v: i32,
    }

    #[arcon::proto]
    struct C {}

    #[test]
    fn test() {
        let _a = A {
            this: Some(AEnum::C(C {})),
        };
    }
}

/// Test `arcon::prelude::ArconUnit` type.
#[cfg(test)]
mod unit {

    #[arcon::proto]
    struct Foo {
        u: (),
    }

    #[arcon::proto]
    enum Bar {
        U(()),
    }

    #[test]
    fn test() {
        use arcon::prelude::ArconUnit;
        let _f = Foo {
            u: ArconUnit::default(),
        };

        let _b = Bar {
            this: Some(BarEnum::U(ArconUnit::default())),
        };
    }
}

/// Test recursive datatypes.
#[cfg(test)]
mod list {
    #[arcon::proto]
    enum List {
        Cons(Cons),
        Nil(()),
    }

    #[arcon::proto]
    struct Cons {
        val: i32,
        tail: Box<List>,
    }

    #[test]
    fn test() {
        use arcon::prelude::ArconUnit;
        let _list = List {
            this: Some(ListEnum::Cons(Cons {
                val: 5,
                tail: Box::new(List {
                    this: Some(ListEnum::Nil(ArconUnit::default())),
                }),
            })),
        };
    }
}
