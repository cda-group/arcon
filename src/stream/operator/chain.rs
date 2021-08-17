//#![allow(non_snake_case)]
#![allow(unused)]
use super::{Operator, OperatorContext};
use crate::data::{ArconElement, ArconNever, ArconType};
use crate::error::ArconResult;

macro_rules! impl_chain {
    ( $head:ident, $( $tail:ident,)* ) => {
        impl<$head, $( $tail ),*> Operator for ($head, $( $tail ),*)
        where
        $head: Operator<TimerState = ArconNever>,
        $( $tail: Operator<TimerState = ArconNever>),*
    {
		type IN = $head::IN;
		type OUT= <last_type!($($tail,)*)>::OUT;
		type OperatorState = $head::OperatorState;
		type TimerState = $head::TimerState;
		type ElementIterator = <last_type!($($tail,)*)>::ElementIterator;

        #[allow(non_snake_case)]
		fn handle_element(
			&mut self,
			element: ArconElement<Self::IN>,
			ctx: OperatorContext<Self>,
		) -> ArconResult<Self::ElementIterator> {
			let ($head, $($tail,)*) = self;
            // Operator Chain: (https://github.com/cda-group/arcon/issues/246)
            //
            // TODO: figure out how to go through all operators to finally produce Self::ElementIterator
            //let head_iter = $head.handle_element(element, ctx)?;
            unimplemented!();
		}

        #[allow(non_snake_case)]
		crate::ignore_timeout!();
        #[allow(non_snake_case)]
		crate::ignore_persist!();
        #[allow(non_snake_case)]
		fn state(&mut self) -> &mut Self::OperatorState {
    		let ($head, $($tail,)* ) = self;
            $head.state()
		}
	}

	};
	() => {};
}

// helper macro to get the last operator in the chain
macro_rules! last_type {
        ($a:ident,) => { $a };
        ($a:ident, $($rest_a:ident,)+) => { last_type!($($rest_a,)+) };
}

impl_chain! { A, B, C, D, E, F, G, H, I, J, K, L, }

#[cfg(test)]
mod tests {
    use super::super::function::Map;
    use super::*;

    #[test]
    fn chain_test() {
        let map = Map::new(|f: u64| f + 10);
        let map_two = Map::new(|f: i32| f + 10);
        let chain = (map, map_two);
    }
}
