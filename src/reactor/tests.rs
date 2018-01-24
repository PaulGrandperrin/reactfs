use futures::Future;
use futures::prelude::*;
use std::thread;
use byteorder::{ByteOrder};
use std::sync::mpsc::channel;
use ::*;
use super::*;
use ::backend::unix_file::*;

/* TODO
 - use in memory fake block device
 - factorize code
*/

proptest! {
    #[test]
    fn fibonacci_seq(n in 0u64..20) {
        let (bd_sender, bd_receiver) = channel::<BDRequest>();
        let (fs_sender, _fs_receiver) = channel::<FSResponse>();
        let (react_sender, react_receiver) = channel::<Event>();

        let react_sender_bd = react_sender.clone();
        let _bd_thread = thread::spawn(move || {
            unix_file_backend_loop(react_sender_bd, bd_receiver);
        });

        let mut core = Core::new(bd_sender, fs_sender, react_receiver);
        let handle = core.handle();
        
        let f = write_fibonacci_seq(handle.clone(), n)
            .and_then(|_|{
                read_u64(&handle, n)
            })
            .and_then(|r|{
                assert!(r == fibonacci(n));
                Ok(())
            });

        let r = core.run(f);
        assert!(r.is_ok());
    }

    #[test]
    fn fibonacci_rec(n in 0u64..20) {
        let (bd_sender, bd_receiver) = channel::<BDRequest>();
        let (fs_sender, _fs_receiver) = channel::<FSResponse>();
        let (react_sender, react_receiver) = channel::<Event>();

        let react_sender_bd = react_sender.clone();
        let _bd_thread = thread::spawn(move || {
            unix_file_backend_loop(react_sender_bd, bd_receiver);
        });

        let mut core = Core::new(bd_sender, fs_sender, react_receiver);
        let handle = core.handle();
        
        let f = write_fibonacci_rec(handle.clone(), n)
            .and_then(|_|{
                read_u64(&handle, n)
            })
            .and_then(|r|{
                assert!(r == fibonacci(n));
                Ok(())
            });

        let r = core.run(f);
        assert!(r.is_ok());
    }
}


#[test]
fn error_propagation() {
    let (bd_sender, bd_receiver) = channel::<BDRequest>();
    let (fs_sender, _fs_receiver) = channel::<FSResponse>();
    let (react_sender, react_receiver) = channel::<Event>();

    let react_sender_bd = react_sender.clone();
    let _bd_thread = thread::spawn(move || {
        unix_file_backend_loop(react_sender_bd, bd_receiver);
    });

    let mut core = Core::new(bd_sender, fs_sender, react_receiver);
    let handle = core.handle();
    
    let f = read_error(handle);

    let r = core.run(f);
    assert!(r.is_err());
}

fn fibonacci(n: u64) -> u64 {
    match n {
        0 | 1 => n,
        _ => {
            let mut pred = 0;
            let mut curr = 1;
            for _ in 1..n {
                let tmp = curr;
                curr += pred;
                pred = tmp;
            }
            curr
        }
    }
}
    
fn write_u64(h: &Handle, n: u64, offset: u64) -> FutureWrite {
    let mut v = vec![0;8];
    byteorder::BigEndian::write_u64(&mut v, n);
    h.write(v, offset*8)
}

fn read_u64(h: &Handle, offset: u64) -> impl Future<Item=u64, Error=failure::Error> {
    h.read(offset*8, 8).map(|v| {
        byteorder::BigEndian::read_u64(&v)
    })
}

#[async(boxed)]
fn write_fibonacci_rec(h:Handle, n: u64) -> Result<(), failure::Error> {
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
fn write_fibonacci_seq(h:Handle, n: u64) -> Result<(), failure::Error> {
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

#[async]
fn read_error(h:Handle) -> Result<u64, failure::Error> {
    await!(read_u64(&h, 1000000000))
}
