use crate::data::{ArconMessage, ArconType};
use kompact::prelude::{ActorPath, ActorRefStrong, Serialisable, Serialiser};
pub mod strategy;

/*
pub trait ArconSerde<A: ArconType>:
    Send + Sync + ArconSerdeClone<A> + Serialiser<ArconMessage<A>> + 'static
{
}

pub trait ArconSerdeClone<A>
where
    A: ArconType,
{
    fn clone_box(&self) -> Box<ArconSerde<A>>;
}
impl<A, B: 'static + ArconSerde<A> + Clone> ArconSerdeClone<A> for B
where
    A: ArconType,
{
    fn clone_box(&self) -> Box<ArconSerde<A>> {
        Box::new(self.clone())
    }
}

impl<A> Clone for Box<ArconSerde<A>>
where
    A: ArconType,
{
    fn clone(&self) -> Box<ArconSerde<A>> {
        self.clone_box()
    }
}
*/
/*
use std::cell::RefCell;

#[derive(Clone)]
pub struct ArconSerde<A: ArconType> {
    pub ser: Rc<RefCell<dyn Serialiser<ArconMessage<A>>>>,
}
unsafe impl<A: ArconType> Send for ArconSerde<A> {}
unsafe impl<A: ArconType> Sync for ArconSerde<A> {}
*/

/*
#[derive(Clone)]
pub struct ArconSerde<A: ArconType>(Rc<Box<Serialiser<A>>>);
unsafe impl<a: arcontype> send for arconserde<a> {}
unsafe impl<a: arcontype> sync for arconserde<a> {}
*/

#[derive(Clone)]
pub enum Channel<A: ArconType> {
    Local(ActorRefStrong<ArconMessage<A>>),
    Remote(ActorPath),
}

