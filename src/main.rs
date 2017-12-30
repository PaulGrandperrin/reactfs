//#![allow(dead_code, unused)]
#![feature(proc_macro, conservative_impl_trait, generators,universal_impl_trait, generator_trait)]
extern crate futures_await as futures;
extern crate byteorder;

use std::io;
use std::thread;
use std::sync::mpsc::channel;
use futures::prelude::*;
use futures::future::Future;
use byteorder::{ByteOrder};

pub mod reactor;
pub mod fake_bd;
use reactor::*;

fn write_u64(h: &Handle, n: u64, offset: u64) -> FutureWrite {
    let mut v = vec![0;8];
    byteorder::BigEndian::write_u64(&mut v, n);
    h.write(v, offset*8)
}

fn read_u64(h: &Handle, offset: u64) -> impl Future<Item=u64, Error=io::Error> {
    h.read(offset*8, 8).map(|v| {
        byteorder::BigEndian::read_u64(&v)
    })
}

#[async(boxed)]
fn write_fibonacci_rec(h:Handle, n: u64) -> io::Result<(())>{
    match n {
        0|1 => {
            let f = Future::join(
                write_u64(&h, 0, 0),
                write_u64(&h, 1, 1)
                );
            await!(f)?;
        },
        _ => {
            await!(write_fibonacci_rec(h.clone(), n-1))?;
            let f = Future::join(
                read_u64(&h, n-1),
                read_u64(&h, n-2)
            );
            let r = await!(f)?;
            await!(write_u64(&h, r.0+r.1, n))?;
        }
    }
    Ok(())
}

#[async]
fn write_fibonacci_seq(h:Handle, n: u64) -> io::Result<(())>{
    let f = Future::join(
        write_u64(&h, 0, 0),
        write_u64(&h, 1, 1)
        );
    await!(f)?;

    for i in 2..n {
        let f = Future::join(
            read_u64(&h, i-1),
            read_u64(&h, i-2)
        );
        let r = await!(f)?;
        await!(write_u64(&h, r.0+r.1, i))?;
    }
    Ok(())
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
    
    let f = write_fibonacci_rec(handle, 50);

    println!("starting reactor");
    let r = core.run(f);
    println!("final result: {:?}", r);
    
    return;
}
