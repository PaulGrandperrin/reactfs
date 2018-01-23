use futures::prelude::*;
use std::thread;
use std::sync::mpsc::channel;
use ::*;
use super::*;

/* TODO
 - use in memory fake block device
*/

#[test]
fn format_and_read_uberblock() {
    let (bd_sender, bd_receiver) = channel::<BDRequest>();
    let (fs_sender, _fs_receiver) = channel::<FSResponse>();
    let (react_sender, react_receiver) = channel::<Event>();

    let react_sender_bd = react_sender.clone();
    let _bd_thread = thread::spawn(move || {
        fake_bd::fake_bd_loop(react_sender_bd, bd_receiver);
    });

    let mut core = Core::new(bd_sender, fs_sender, react_receiver);
    let handle = core.handle();

    let f = format_and_read_uberblock_async(handle.clone());

    println!("starting reactor");
    let r = core.run(f);
    assert!(r.unwrap().tgx == 9);
}

#[async]
fn format_and_read_uberblock_async(handle: Handle) -> Result<Uberblock, failure::Error> {
    await!(format(handle.clone()))?;
    await!(read_latest_uberblock(handle.clone()))
}