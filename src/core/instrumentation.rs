use std::u8;
use futures::prelude::*;
use std::thread;
use std::sync::mpsc::channel;

use ::backend::mem::*;

use super::*;
use super::util::*;
use super::uberblock::*;
use super::cow_btree::*;

pub enum Operation {
    Insert(u64, u64),
    Remove(u64)
}

pub fn insert_checked(vec: Vec<(u64, u64)>) {
    // insert the data in the cow btree
    let cow_btree = run_in_reactor_on_mem_backend(|handle| {
        Box::new(async_btree_insert_and_read(handle.clone(), &vec))
    }).unwrap();

    // do the same operation in an std btree
    use std::collections::BTreeMap;
    let mut std_btree = BTreeMap::<u64, u64>::new();
    for (k, v) in vec {
        std_btree.insert(k, v);
    }

    // check that both btrees are the same
    for ((std_k, std_v), cow) in std_btree.into_iter().zip(cow_btree) {
        //println!("key: {} == {}", std_k, cow.key);
        //println!("val: {} == {}", std_v, cow.value);
        assert!(std_k == cow.key);
        assert!(std_v == cow.value);
    }
}

pub fn insert_and_remove_checked(vec: Vec<Operation>) {
    run_in_reactor_on_mem_backend(|handle| {
        Box::new(async_btree_insert_and_remove_checked(handle.clone(), &vec))
    }).unwrap();
}

pub fn raw_to_vec_of_operation(data: &[u8]) -> Option<Vec<Operation>> {
    let mut data = Cursor::new(data);
    let mut vec = Vec::new();

    for counter in 0.. {
        if data.remaining() < 1 {break}
        match data.get_u8() {
            0 => { // insert
                if data.remaining() < 2 {break}
                vec.push(Operation::Insert(data.get_u16::<LittleEndian>() as u64, counter));
            }
            1 => { // remove
                if data.remaining() < 2 {break;}
                vec.push(Operation::Remove(data.get_u16::<LittleEndian>() as u64));
            }
            _ => return None
        }
    }

    Some(vec)
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
                LeafNodeEntry::new(vec[i].0 as u64, vec[i].1 as u64)
                ))?;
            op = res.0;
            free_space_offset = res.1;

            // check that the key wasn't already there
            assert!(res.2 == None);
        }

        // read the btree, the data should now be sorted
        await!(read_btree(handle.clone(), op.clone()))
    }
}

fn async_btree_insert_and_remove_checked<'f>(handle: Handle, vec: &'f Vec<Operation>) -> impl Future<Item=(), Error=failure::Error> + 'f {
    async_block!{
        // create an identical std btree
        use std::collections::BTreeMap;
        let mut std_btree = BTreeMap::<u64, u64>::new();

        // format
        await!(format(handle.clone()))?;
        let uberblock = await!(find_latest_uberblock(handle.clone()))?;
        let (mut op, mut free_space_offset) = (uberblock.tree_root_pointer, uberblock.free_space_offset);

        // process operations
        for o in vec {
            match o {
                // insert in cow btree
                Operation::Insert(k, v) => {
                    let res = await!(insert_in_btree(
                        handle.clone(),
                        op.clone(),
                        free_space_offset,
                        LeafNodeEntry::new(*k, *v)
                        ))?;
                    op = res.0;
                    free_space_offset = res.1;

                    // insert in std btree
                    let std_old_value = std_btree.insert(*k, *v);

                    // check that the potential old value is the same
                    assert!(std_old_value == res.2);
                }
                Operation::Remove(k) => {
                    // remove in cow btree
                    let res = await!(remove(
                        handle.clone(),
                        op.clone(),
                        free_space_offset,
                        *k
                        ))?;
                    op = res.0;
                    free_space_offset = res.1;

                    // remove in std btree
                    let std_old_value = std_btree.remove(k);

                    // check that the potential old value is the same
                    assert!(std_old_value == res.2);
                }
            };

            // read the cow btree
            let cow_btree = await!(read_btree(handle.clone(), op.clone()))?;

            // check that both btrees are the same
            for ((std_k, std_v), cow) in std_btree.iter().zip(cow_btree) {
                //println!("key: {} == {}", std_k, cow.key);
                //println!("val: {} == {}", std_v, cow.value);
                assert!(*std_k == cow.key);
                assert!(*std_v == cow.value);
            }
        }

        Ok(())
    }
}