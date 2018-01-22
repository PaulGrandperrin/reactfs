use futures::Future;
use futures::prelude::*;
use std::thread;
use byteorder::{ByteOrder};
use std::sync::mpsc::channel;
use ::*;
use super::*;


/* TODO
 - use in memory fake block device
 - factorize code
*/

proptest! {
	#[test]
	fn fibonacci_seq(size in 0u64..20) {
	    let (bd_sender, bd_receiver) = channel::<BDRequest>();
	    let (fs_sender, _fs_receiver) = channel::<FSResponse>();
	    let (react_sender, react_receiver) = channel::<Event>();

	    let react_sender_bd = react_sender.clone();
	    let _bd_thread = thread::spawn(move || {
	        fake_bd::fake_bd_loop(react_sender_bd, bd_receiver);
	    });

	    let mut core = Core::new(bd_sender, fs_sender, react_receiver);
	    let handle = core.handle();
	    
	    let f = write_fibonacci_seq(handle, size);

	    let r = core.run(f);
	    assert!(r.is_ok());
	}

	#[test]
	fn fibonacci_rec(size in 0u64..20) {
	    let (bd_sender, bd_receiver) = channel::<BDRequest>();
	    let (fs_sender, _fs_receiver) = channel::<FSResponse>();
	    let (react_sender, react_receiver) = channel::<Event>();

	    let react_sender_bd = react_sender.clone();
	    let _bd_thread = thread::spawn(move || {
	        fake_bd::fake_bd_loop(react_sender_bd, bd_receiver);
	    });

	    let mut core = Core::new(bd_sender, fs_sender, react_receiver);
	    let handle = core.handle();
	    
	    let f = write_fibonacci_rec(handle, size as u64);

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
        fake_bd::fake_bd_loop(react_sender_bd, bd_receiver);
    });

    let mut core = Core::new(bd_sender, fs_sender, react_receiver);
    let handle = core.handle();
    
    let f = read_error(handle);

    let r = core.run(f);
    assert!(r.is_err());
}
    
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
fn write_fibonacci_rec(h:Handle, n: u64) -> Result<(), failure::Error> {
    match n {
        0|1 => {
            let f = Future::join(
                _write_u64(&h, 0, 0),
                _write_u64(&h, 1, 1)
                );
            await!(f)?;
        },
        _ => {
            await!(write_fibonacci_rec(h.clone(), n-1))?;
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
fn write_fibonacci_seq(h:Handle, n: u64) -> Result<(), failure::Error> {
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
fn read_error(h:Handle) -> Result<u64, failure::Error> {
    await!(_read_u64(&h, 1000000000))
}
