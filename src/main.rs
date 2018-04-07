#![allow(dead_code, unused)]
#![feature(entry_and_modify, proc_macro, generators, generator_trait, nll, match_default_bindings,collection_placement,placement_in_syntax, const_fn)]
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

fn main() {
    // launch start function and print potential errors
    match start() {
        Ok(_)  => {}
        Err(e) => {
            eprintln!("Execution failed with error: {}", e);

            let print_backtrace = std::env::var("RUST_BACKTRACE").unwrap_or("0".into()) != "0";

            if print_backtrace {
                eprintln!("{}", e.backtrace());
            }

            // print chain of causes
            for f in e.causes().skip(1) {
                eprintln!("caused by: {}", f);

                if print_backtrace {
                    if let Some(b) = f.backtrace() {
                        eprintln!("{}", b);
                    }
                }
            }

            if !print_backtrace {
                eprintln!("note: Run with `RUST_BACKTRACE=1` for a backtrace.");
            }

            std::process::exit(101);
        }
    }
}

fn start() -> Result<()> {
    let vec = vec![(3,30), (1,10),(2,20), (4,40), (5,50),
                   (6,60), (7,70)];
    
    core::instrumentation::insert_checked(vec);

    Ok(())
}