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

/// Test `()` type.
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
        let _f = Foo { u: () };

        let _b = Bar {
            this: Some(BarEnum::U(())),
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
        let _list = List {
            this: Some(ListEnum::Cons(Cons {
                val: 5,
                tail: Box::new(List {
                    this: Some(ListEnum::Nil(())),
                }),
            })),
        };
    }
}

/// Test to see that prost can correctly handle unit type.
#[cfg(test)]
mod prost_unit {
    use prost::{Message, Oneof};

    #[derive(Message, Clone, Eq, PartialEq)]
    struct Foo {
        #[prost(message, required)]
        u0: (),
        #[prost(int32)]
        i: i32,
        #[prost(message, required)]
        u1: (),
        #[prost(message, required)]
        u2: (),
        #[prost(oneof = "Bar", tags = "10, 11")]
        b: Option<Bar>,
    }

    #[derive(Oneof, Clone, Eq, PartialEq)]
    enum Bar {
        #[prost(message, tag = "10")]
        Baz(()),
        #[prost(message, tag = "11")]
        Qux(()),
    }

    #[test]
    fn test() {
        let f = Foo {
            u0: (),
            i: 0,
            u1: (),
            u2: (),
            b: Some(Bar::Qux(())),
        };

        let mut buf = Vec::new();

        let expected = f.clone();

        f.encode(&mut buf).unwrap();
        let found = Foo::decode(&buf[..]).unwrap();

        assert_eq!(expected, found);
    }
}
