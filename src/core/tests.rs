use futures::prelude::*;
use std::thread;
use std::sync::mpsc::channel;
use ::*;
use super::*;
use ::backend::mem::*;

proptest! {
    #[test]
    fn format_read_and_write_uberblock(n in 0usize..20) {
        let (bd_sender, bd_receiver) = channel::<BDRequest>();
        let (fs_sender, _fs_receiver) = channel::<FSResponse>();
        let (react_sender, react_receiver) = channel::<Event>();

        let react_sender_bd = react_sender.clone();
        let _bd_thread = thread::spawn(move || {
            mem_backend_loop(react_sender_bd, bd_receiver, 4096 * 1000);
        });

        let mut core = Core::new(bd_sender, fs_sender, react_receiver);
        let handle = core.handle();

        let f = format_read_and_write_uberblock_async(handle.clone(), n);

        println!("starting reactor");
        let r = core.run(f);
        assert!(r.unwrap().tgx == 9 + n as u64);
    }
}

#[async]
fn format_read_and_write_uberblock_async(handle: Handle, n: usize) -> Result<Uberblock, failure::Error> {
    await!(format(handle.clone()))?;

    for _ in 0..n {
        let mut u = await!(find_latest_uberblock(handle.clone()))?;
        u.tgx += 1;
        await!(write_new_uberblock(handle.clone(), u))?;
    }

    await!(find_latest_uberblock(handle.clone()))
}