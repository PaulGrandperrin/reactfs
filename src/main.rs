#![allow(dead_code, unused)]
#![feature(entry_and_modify, proc_macro, conservative_impl_trait, generators,universal_impl_trait, generator_trait, nll, match_default_bindings,collection_placement,placement_in_syntax)]
#![cfg_attr(test, feature(plugin))]
#![cfg_attr(test, plugin(quickcheck_macros))]

extern crate futures_await as futures;
extern crate byteorder;
extern crate bytes;
extern crate itertools;
extern crate num_traits;
#[macro_use]
extern crate enum_primitive_derive;
#[macro_use] extern crate failure;

#[cfg(test)]
extern crate quickcheck;
#[cfg(test)]
#[macro_use]
extern crate proptest;

#[macro_use]
extern crate fuzztest;

use std::thread;
use std::sync::mpsc::channel;
use futures::prelude::*;
use futures::future::Future;
use byteorder::{ByteOrder};

use reactor::*;
use core::*;
use backend::mem::*;


pub mod reactor;
pub mod backend;
pub mod core;

fn main() {
	let vec = vec![(3,30), (1,10),(2,20), (4,40), (5,50),
				   (6,60), (7,70)];
	
	core::instrumentation::insert_checked(vec);
}