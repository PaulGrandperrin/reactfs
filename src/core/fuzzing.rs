use futures::prelude::*;
use std::thread;
use std::sync::mpsc::channel;
use ::*;
use super::*;
use ::backend::mem::*;

pub fn fuzz_cow_btree(data: &[u8]) {
	// we don't want to explode the search space
	if data.len() > 50 * (8 + 8) {return}

    let (bd_sender, bd_receiver) = channel::<BDRequest>();
    let (fs_sender, _fs_receiver) = channel::<FSResponse>();
    let (react_sender, react_receiver) = channel::<Event>();

    let react_sender_bd = react_sender.clone();
    let _bd_thread = thread::spawn(move || {
        mem_backend_loop(react_sender_bd, bd_receiver, 4096 * 1000);
    });

    let mut core = Core::new(bd_sender, fs_sender, react_receiver);
    let handle = core.handle();

    // convert raw data to vector of tuple of (key, value)
    let vec_len = data.len() / (8 + 8);
    let mut data = Cursor::new(data);
    let mut vec = Vec::with_capacity(vec_len);
    for _ in 0..vec_len {
    	vec.push((data.get_u64::<LittleEndian>(), data.get_u64::<LittleEndian>()));
    }

    let f = fuzz_cow_btree_async(handle.clone(), vec);

    let r = core.run(f);
    r.unwrap();
}

#[async]
fn fuzz_cow_btree_async(handle: Handle, vec: Vec<(u64, u64)>) -> Result<(), failure::Error> {
    await!(format(handle.clone()))?;
    let uberblock = await!(find_latest_uberblock(handle.clone()))?;
    let (mut op, mut free_space_offset) = (uberblock.tree_root_pointer, uberblock.free_space_offset);

    // insert the vector in the btree
    for i in 0..vec.len() {
        let res = await!(insert_in_btree(
            handle.clone(),
            op.clone(),
            free_space_offset,
            LeafNodeEntry{key: vec[i].0 as u64, value: vec[i].1 as u64}
            ))?;
        op = res.0;
        free_space_offset = res.1;
    }

    // read the btree, the data should now be sorted
    let res = await!(read_tree(handle.clone(), op.clone()))?;
    
    // now sort the vector by key
    vec.sort_by_key(|e| {e.0});

    // check the btree data against the input vector
    for i in 0..vec.len() {
    	assert!(res[i].key == vec[i].0);
    	assert!(res[i].value == vec[i].1);
    }

    Ok(())
}
