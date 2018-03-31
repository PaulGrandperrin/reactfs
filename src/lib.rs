#![allow(dead_code, unused)]
#![feature(entry_and_modify, proc_macro, generators, generator_trait, nll, match_default_bindings,collection_placement,placement_in_syntax, associated_type_defaults)]
#![cfg_attr(test, feature(plugin, test))]
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

#[cfg(test)]
extern crate test;

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

// we use by default the Error implementation from failure's crate
pub type Result<T> = std::result::Result<T, failure::Error>;
