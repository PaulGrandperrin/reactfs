//#![allow(dead_code, unused)]
#![feature(proc_macro, conservative_impl_trait, generators,universal_impl_trait, generator_trait, nll)]
extern crate futures_await as futures;
extern crate byteorder;
extern crate failure;

use std::thread;
use std::sync::mpsc::channel;
use futures::prelude::*;
use futures::future::Future;
use byteorder::{ByteOrder};

pub mod reactor;
pub mod fake_bd;
pub mod failures;

use reactor::*;


fn _read_u64<'a>(h: &'a Handle, offset: u64) -> impl Future<Item=u64, Error=failure::Error> +'a {
    h.read(offset*8, 8).map(|v| {
        byteorder::BigEndian::read_u64(&v)
    })
}



fn read_error<'a>(h: &'a Handle) -> impl Future<Item=u64, Error=failure::Error> + 'a {
    async_block!{
        await!(_read_u64(&h, 1000000000))
    }
}

fn main() {

    let (bd_sender, bd_receiver) = channel::<BDRequest>();
    let (fs_sender, _fs_receiver) = channel::<FSResponse>();
    let (react_sender, react_receiver) = channel::<Event>();

    let react_sender_bd = react_sender.clone();
    let _bd_thread = thread::spawn(move || {
        fake_bd::fake_bd_loop(react_sender_bd, bd_receiver);
    });

    let core = Core::new(bd_sender, fs_sender, react_receiver);
    let handle = core.handle();
    
    //let f = write_fibonacci_rec(handle, 50);
    let f = read_error(handle);
    
    println!("starting reactor");
    let r = core.run(f);
    println!("final result: {:?}", r);
    
    return;
}
