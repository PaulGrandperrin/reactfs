#![allow(dead_code, unused)]
#![feature(proc_macro, conservative_impl_trait, generators,universal_impl_trait, generator_trait, nll)]
extern crate futures_await as futures;
extern crate byteorder;
extern crate failure;
#[macro_use] extern crate enum_primitive_derive;
extern crate num_traits;

use std::thread;
use std::sync::mpsc::channel;
use futures::prelude::*;
use futures::future::Future;
use byteorder::{ByteOrder};

pub mod reactor;
pub mod fake_bd;
pub mod failures;
pub mod cow_btree;
pub mod core;

use reactor::*;

fn _write_u64(h: &Handle, n: u64, offset: u64) -> FutureWrite {
    let mut v = vec![0;8];
    byteorder::BigEndian::write_u64(&mut v, n);
    h.write(v, offset*8)
}

fn _read_u64(h: &Handle, offset: u64) -> impl Future<Item=u64, Error=failure::Error> {
    h.read(offset*8, 8).map(|v| {
        byteorder::BigEndian::read_u64(&v)
    })
}

#[async(boxed)]
fn _write_fibonacci_rec(h:Handle, n: u64) -> Result<(), failure::Error> {
    match n {
        0|1 => {
            let f = Future::join(
                _write_u64(&h, 0, 0),
                _write_u64(&h, 1, 1)
                );
            await!(f)?; 
        },
        _ => {
            await!(_write_fibonacci_rec(h.clone(), n-1))?;
            let f = Future::join(
                _read_u64(&h, n-1),
                _read_u64(&h, n-2)
            );
            let r = await!(f)?;
            await!(_write_u64(&h, r.0+r.1, n))?;
        }
    }
    Ok(())
}

#[async]
fn _write_fibonacci_seq(h:Handle, n: u64) -> Result<(), failure::Error> {
    let f = Future::join(
        _write_u64(&h, 0, 0),
        _write_u64(&h, 1, 1)
        );
    await!(f)?;

    for i in 2..n {
        let f = Future::join(
            _read_u64(&h, i-1),
            _read_u64(&h, i-2)
        );
        let r = await!(f)?;
        await!(_write_u64(&h, r.0+r.1, i))?;
    }
    Ok(())
}

#[async]
fn _read_error(h:Handle) -> Result<u64, failure::Error> {
    await!(_read_u64(&h, 1000000000))
}

#[derive(Debug)]
enum Test {
    A(Vec<u64>),
    B(Vec<u64>),
    C(Vec<u64>)
}

fn main() {

    let (bd_sender, bd_receiver) = channel::<BDRequest>();
    let (fs_sender, _fs_receiver) = channel::<FSResponse>();
    let (react_sender, react_receiver) = channel::<Event>();

    let react_sender_bd = react_sender.clone();
    let _bd_thread = thread::spawn(move || {
        fake_bd::fake_bd_loop(react_sender_bd, bd_receiver);
    });

    let mut core = Core::new(bd_sender, fs_sender, react_receiver);
    let handle = core.handle();
    
    //let f = write_fibonacci_rec(handle, 50);
    //let f = _write_fibonacci_rec(handle, 10);


    let f = async_block! {
        use cow_btree::*;
        await!(test(handle.clone()))?;        
        Ok::<(),failure::Error>(())
    };

    println!("starting reactor");
    let r = core.run(f);
    println!("final result: {:?}", r);
    
    return;
}
