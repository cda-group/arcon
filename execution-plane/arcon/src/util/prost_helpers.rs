use prost::Message;

// It's so stupid that this is needed. std Option<T> doesn't implement prost::Message
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

impl<A: Message + Default> Into<Option<A>> for ProstOption<A> {
    fn into(self) -> Option<A> {
        self.inner
    }
}
