use futures::prelude::*;
use std::thread;
use std::sync::mpsc::channel;

use ::backend::mem::*;

use super::*;
use super::util::*;
use super::uberblock::*;
use super::cow_btree::*;


pub fn insert_checked(vec: Vec<(u64, u64)>) {
    // insert the data in the cow btree
    let cow_btree = run_in_reactor_on_mem_backend(|handle| {
        Box::new(async_btree_insert_and_read(handle.clone(), &vec))
    }).unwrap();

    // do the same operation in an std btree
    use std::collections::BTreeMap;
    let mut std_btree = BTreeMap::<u64, u64>::new();
    for (k, v) in vec {
        std_btree.entry(k).and_modify(|e| {*e = e.wrapping_add(v).wrapping_mul(2)}).or_insert(v);
    }

    // check that both btrees are the same
    for ((std_k, std_v), cow) in std_btree.into_iter().zip(cow_btree) {
        //println!("key: {} == {}", std_k, cow.key);
        //println!("val: {} == {}", std_v, cow.value);
        assert!(std_k == cow.key);
        assert!(std_v == cow.value);
    }
}

pub fn raw_to_vec_of_tuple_u64(data: &[u8]) -> Vec<(u64, u64)> {
    let vec_len = data.len() / (8 + 8);
    let mut data = Cursor::new(data);
    let mut vec = Vec::with_capacity(vec_len);

    for _ in 0..vec_len {
        vec.push((data.get_u64::<LittleEndian>(), data.get_u64::<LittleEndian>()));
    }

    vec
}

pub fn raw_to_vec_of_tuple_u16(data: &[u8]) -> Vec<(u64, u64)> {
    let vec_len = data.len() / (2 + 2);
    let mut data = Cursor::new(data);
    let mut vec = Vec::with_capacity(vec_len);

    for _ in 0..vec_len {
        vec.push((data.get_u16::<LittleEndian>() as u64, data.get_u16::<LittleEndian>() as u64));
    }

    vec
}

pub fn run_in_reactor_on_mem_backend<'a, F, T, E>(closure: F) -> Result<T, E>
where F: Fn(Handle) -> Box<Future<Item=T, Error=E> + 'a>
{
    let (bd_sender, bd_receiver) = channel::<BDRequest>();
    let (fs_sender, _fs_receiver) = channel::<FSResponse>();
    let (react_sender, react_receiver) = channel::<Event>();

    let react_sender_bd = react_sender.clone();
    let _bd_thread = thread::spawn(move || {
        mem_backend_loop(react_sender_bd, bd_receiver, 4096 * 1000);
    });

    let mut core = Core::new(bd_sender, fs_sender, react_receiver);
    let handle = core.handle();

    let f = closure(handle);

    core.run(f)
}

fn async_btree_insert_and_read<'f>(handle: Handle, vec: &'f Vec<(u64, u64)>) -> impl Future<Item=Vec<LeafNodeEntry>, Error=failure::Error> + 'f {
    async_block!{
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
        await!(read_btree(handle.clone(), op.clone()))
    }
}
