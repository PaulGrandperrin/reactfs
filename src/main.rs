//#![allow(dead_code, unused)]
#![feature(proc_macro, conservative_impl_trait, generators,universal_impl_trait, generator_trait, nll)]
#![cfg_attr(test, feature(plugin))]
#![cfg_attr(test, plugin(quickcheck_macros))]

extern crate futures_await as futures;
extern crate byteorder;
extern crate itertools;
#[macro_use] extern crate failure;

#[cfg(test)]
extern crate quickcheck;
#[cfg(test)]
#[macro_use]
extern crate proptest;
/*
use std::thread;
use std::sync::mpsc::channel;
use futures::prelude::*;
use futures::future::Future;
use byteorder::{ByteOrder};

use reactor::*;
use core::*;
*/
pub mod reactor;
pub mod backend;
pub mod core;


fn main() {

    
    return;
}
