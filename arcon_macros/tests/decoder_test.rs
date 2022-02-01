#[macro_use]
extern crate arcon_macros;

#[decoder(,)]
pub struct CommaItem {
    id: u64,
    price: u32,
}

#[decoder(;)]
pub struct SemicolonItem {
    id: u64,
    name: String,
    price: u32,
}

#[test]
fn comma_test() {
    use std::str::FromStr;
    let item: CommaItem = CommaItem::from_str("100, 250").unwrap();
    assert_eq!(item.id, 100);
    assert_eq!(item.price, 250);
}

#[test]
fn semicolon_test() {
    use std::str::FromStr;
    let item = SemicolonItem::from_str("100;test;250").unwrap();
    assert_eq!(item.id, 100);
    assert_eq!(item.name, String::from("test"));
    assert_eq!(item.price, 250);
}
