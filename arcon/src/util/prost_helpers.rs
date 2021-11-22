use prost::Message;

#[cfg_attr(feature = "arcon_serde", derive(serde::Serialize, serde::Deserialize))]
#[derive(Message, Clone)]
pub struct ProstOption<A: Message + Default> {
    #[prost(message, tag = "1")]
    pub inner: Option<A>,
}

impl<A: Message + Default> From<Option<A>> for ProstOption<A> {
    fn from(inner: Option<A>) -> Self {
        ProstOption { inner }
    }
}

impl<A: Message + Default> From<ProstOption<A>> for Option<A> {
    fn from(opt: ProstOption<A>) -> Self {
        opt.inner
    }
}
