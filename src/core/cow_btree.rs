use std::mem;
use std::u64;
use std::fmt::Debug;
use super::*;
use super::util::*;

impl<K: Serializable + Ord + Copy, V: Serializable> NodeEntry<K, V> {
    pub fn new(key: K, value: V) -> Self {
        Self {
            key,
            value,
        }
    }
}

impl<K: Serializable + Ord + Copy, V: Serializable, B: ConstUsize, T: ConstObjectType> Node<K, V, B, T> {
    pub fn new() -> Self {
        Self {
            entries: vec![],
            _b: PhantomData,
            _t: PhantomData,
        }
    }

    pub fn with_entries(entries: Vec<NodeEntry<K, V>>) -> Self {
        Self {
            entries,
            _b: PhantomData,
            _t: PhantomData,
        }
    }

    pub fn from_bytes(bytes: &mut Cursor<&[u8]>) -> Result<Self, failure::Error> {
        let mut entries = vec![];

        while bytes.remaining() >= K::SIZE + V::SIZE {
            let key = K::from_bytes(bytes)?;
            let value = V::from_bytes(bytes)?;
            entries.push(NodeEntry::new(key, value));
        }

        debug_assert!(bytes.remaining() == 0);

        Ok(Self::with_entries(entries))
    }

    fn to_bytes(&self, bytes: &mut Cursor<&mut [u8]>) {
        debug_assert!(bytes.remaining_mut() >= self.entries.len() * (K::SIZE + V::SIZE));

        for NodeEntry{key, value} in &self.entries {
            key.to_bytes(bytes);
            value.to_bytes(bytes);
        }
    }

    pub fn to_mem(&self) -> Box<[u8]> {
        let size = self.entries.len()*(K::SIZE + V::SIZE);
        let mut mem = Vec::with_capacity(size);
        unsafe{mem.set_len(size)};
        self.to_bytes(&mut Cursor::new(&mut mem));
        return mem.into_boxed_slice();
    }

    pub fn async_write_at(&self, handle: Handle, offset: u64) -> Box<Future<Item=u64, Error=failure::Error>> { // box not really needed
        Box::new(handle.write(self.to_mem().to_vec(), offset))
    }

    fn cow<'f>(&'f self, handle: Handle, fso: &'f mut u64) -> Box<Future<Item=ObjectPointer, Error=failure::Error> + 'f> { // box not really needed
        Box::new(async_block! {
            let offset = *fso;
            let len = await!(self.async_write_at(handle.clone(), offset))?;
            *fso += len;
            let op = ObjectPointer {
                offset,
                len,
                object_type: T::OTYPE
            };
            Ok(op)
        })
    }

}

impl<K: Serializable + Ord + Copy, V: Serializable, B: ConstUsize> NodeTrait<K, V> for Node<K, V, B, Leaf> {
    fn insert(&mut self, mut entry: NodeEntry<K, V>) -> Option<V> {
        // algo invariant: the entries should be sorted
        debug_assert!(is_sorted(self.entries.iter().map(|l|{l.key})));

        let res = self.entries.binary_search_by_key(&entry.key, |e| e.key);
        match res {
            Ok(i)  => {
                mem::swap(&mut self.entries[i].value, &mut entry.value);
                Some(entry.value)
            },
            Err(i) => {
                self.entries.insert(i, entry);
                None
            },
        }
    }
}

impl<K: Serializable + Ord + Copy, V: Serializable, B: ConstUsize> NodeTrait<K, V> for Node<K, V, B, Internal> {
    fn insert(&mut self, mut entry: NodeEntry<K, V>) -> Option<V> {
        // algo invariant: the entries should be sorted
        debug_assert!(is_sorted(self.entries.iter().map(|l|{l.key})));

        let res = self.entries.binary_search_by_key(&entry.key, |e| e.key);
        match res {
            Ok(i)  => unreachable!("cow_btree: trying to insert in an InternalNode but key already exists"),
            Err(i) => {
                self.entries.insert(i, entry);
                None
            },
        }
    }
}

/// insert or go in entry then split 
#[async(boxed)]  // box not really needed
fn insert_in_leaf_node<K: Serializable + Ord + Copy + 'static, V: Serializable + 'static, B: ConstUsize + 'static>
(handle: Handle, node: Node<K, V, B, Leaf>, free_space_offset: u64, entry_to_insert: NodeEntry<K, V>)
-> Result<(NodeEntry<K, ObjectPointer>, u64, Option<V>), failure::Error> {
    
    // algo invariant: the entries should be sorted
    debug_assert!(is_sorted(node.entries.iter().map(|l|{l.key})));

    let old_value = node.insert(entry_to_insert);

    // COW node
    let op = await!(node.cow(handle.clone(), &mut free_space_offset))?;

    let entry = NodeEntry::<K, ObjectPointer>::new(node.entries[0].key, op);

    Ok((entry, free_space_offset, old_value))
}

/// insert or go in entry then split 
#[async(boxed)]
fn insert_in_internal_node<K: Serializable + Ord + Copy + 'static, V: Serializable + 'static, B: ConstUsize + 'static>
(handle: Handle, cur_node: Node<K, ObjectPointer, B, Internal>, free_space_offset: u64, entry_to_insert: NodeEntry<K, V>)
-> Result<(NodeEntry<K, ObjectPointer>, u64, Option<V>), failure::Error> {
    // algo invariant: the entries should be sorted
    debug_assert!(is_sorted(cur_node.entries.iter().map(|l|{l.key})));

    let res = cur_node.entries.binary_search_by_key(&entry_to_insert.key, |entry| entry.key);
    let index = match res {
        Ok(i) => i, // exact match
        Err(0) => 0, // key is smaller than first entry
        Err(i) => i - 1, // match first bigger entry
    };

    // object pointer of branch where to insert
    let op = cur_node.entries[index].value.clone();
    
    // read pointed object
    let any_object = await!(op.async_read_object::<K, V, B>(handle.clone()))?;

    match any_object {
        AnyObject::LeafNode(child_node) => {
            // algo invariant
            debug_assert!(child_node.entries.len() >= BTREE_B && child_node.entries.len() <= BTREE_DEGREE); // b <= len <= 2b+1 with b=2 except root
            let old_value = if child_node.entries.len() < BTREE_DEGREE { // pro-active splitting if the node has the maximum size
                let (child_entry, new_free_space_offset, old_value) = await!(insert_in_leaf_node(handle.clone(), *child_node, free_space_offset, entry_to_insert))?;
                free_space_offset = new_free_space_offset;

                // update current's node selected entry
                cur_node.entries[index] = child_entry;

                old_value
            } else { // split
                // split the node and insert in relevant child
                let (left_entry, right_entry, new_free_space_offset, old_value) = await!(leaf_split_and_insert(handle.clone(), *child_node, free_space_offset, entry_to_insert))?;
                free_space_offset = new_free_space_offset;

                // update current's node selected entry
                cur_node.entries[index] = left_entry;

                // push in current node new entry which points to right_node
                cur_node.insert(right_entry);

                old_value
            };

            // COW that new node
            let op = await!(cur_node.cow(handle.clone(), &mut free_space_offset))?;

            let entry = NodeEntry::<K, ObjectPointer>::new(cur_node.entries[0].key, op);

            // return
            Ok((
                entry,
                free_space_offset,
                old_value
            ))
        }
        AnyObject::InternalNode(child_node) => {
            // algo invariant
            debug_assert!(child_node.entries.len() >= BTREE_B && child_node.entries.len() <= BTREE_DEGREE); // b <= len <= 2b+1 with b=2 except root

            let old_value = if child_node.entries.len() < BTREE_DEGREE { // pro-active splitting if the node has the maximum size
                let (child_entry, new_free_space_offset, old_value) = await!(insert_in_internal_node(handle.clone(), *child_node, free_space_offset, entry_to_insert))?;
                free_space_offset = new_free_space_offset;

                // update current's node selected entry
                cur_node.entries[index] = child_entry;

                old_value
            } else { // split
                // split the node and insert in relevant child
                let (left_entry, right_entry, new_free_space_offset, old_value) = await!(internal_split_and_insert(handle.clone(), *child_node, free_space_offset, entry_to_insert))?;
                free_space_offset = new_free_space_offset;

                // update current's node selected entry
                cur_node.entries[index] = left_entry;

                // push in current node new entry which points to right_node
                cur_node.insert(right_entry);

                old_value
            };

            // COW that new node
            let op = await!(cur_node.cow(handle.clone(), &mut free_space_offset))?;

            let entry = NodeEntry::<K, ObjectPointer>::new(cur_node.entries[0].key, op);

            // return
            Ok((
                entry,
                free_space_offset,
                old_value
            ))
        }
    }
}

#[async(boxed)] // box not really needed
fn leaf_split_and_insert<K: Serializable + Ord + Copy + 'static, V: Serializable + 'static, B: ConstUsize + 'static>
(handle: Handle, node: Node<K, V, B, Leaf>, free_space_offset: u64, entry_to_insert: NodeEntry<K, V>)
-> Result<(NodeEntry<K, ObjectPointer>, NodeEntry<K, ObjectPointer>, u64, Option<V>), failure::Error> {
    // rename node to left_node ...
    let mut left_node = node;
    // ... and split off its right half to right_node
    let right_entries = left_node.entries.split_off(BTREE_SPLIT); // split at b+1
    let mut right_node = Node::<K, V, B, Leaf>::with_entries(right_entries);

    // insert entry in either node
    let (left_entry, right_entry, old_value) = if entry_to_insert.key < right_node.entries[0].key { // are we smaller than the first element of the right half
        // TODO maybe inlining insert_in_internal_node code would be simpler
        let (left_entry, new_free_space_offset, old_value) = await!(insert_in_leaf_node(handle.clone(), left_node, free_space_offset, entry_to_insert))?;
        free_space_offset = new_free_space_offset;
        let right_op = await!(right_node.cow(handle.clone(), &mut free_space_offset))?;
        let right_entry = NodeEntry::<K, ObjectPointer>::new(right_node.entries[0].key, right_op);
        (left_entry, right_entry, old_value)
    } else {
        // TODO maybe inlining insert_in_internal_node code would be simpler
        let (right_entry, new_free_space_offset, old_value) = await!(insert_in_leaf_node(handle.clone(), right_node, free_space_offset, entry_to_insert))?;
        free_space_offset = new_free_space_offset;
        let left_op = await!(left_node.cow(handle.clone(), &mut free_space_offset))?;
        let left_entry = NodeEntry::<K, ObjectPointer>::new(left_node.entries[0].key, left_op);
        (left_entry, right_entry, old_value)
    };

    Ok((left_entry, right_entry, free_space_offset, old_value))
}

#[async(boxed)] // box not really needed
fn internal_split_and_insert<K: Serializable + Ord + Copy + 'static, V: Serializable + 'static, B: ConstUsize + 'static>
(handle: Handle, node: Node<K, ObjectPointer, B, Internal>, free_space_offset: u64, entry_to_insert: NodeEntry<K, V>)
-> Result<(NodeEntry<K, ObjectPointer>, NodeEntry<K, ObjectPointer>, u64, Option<V>), failure::Error> {
    // rename node to left_node ...
    let mut left_node = node;
    // ... and split off its right half to right_node
    let right_entries = left_node.entries.split_off(BTREE_SPLIT); // split at b+1
    let mut right_node = Node::<K, ObjectPointer, B, Internal>::with_entries(right_entries);

    // insert entry in either node
    let (left_entry, right_entry, old_value) = if entry_to_insert.key < right_node.entries[0].key { // are we smaller than the first element of the right half
        // TODO maybe inlining insert_in_internal_node code would be simpler
        let (left_entry, new_free_space_offset, old_value) = await!(insert_in_internal_node(handle.clone(), left_node, free_space_offset, entry_to_insert))?;
        free_space_offset = new_free_space_offset;
        let right_op = await!(right_node.cow(handle.clone(), &mut free_space_offset))?;
        let right_entry = NodeEntry::<K, ObjectPointer>::new(right_node.entries[0].key, right_op);
        (left_entry, right_entry, old_value)
    } else {
        // TODO maybe inlining insert_in_internal_node code would be simpler
        let (right_entry, new_free_space_offset, old_value) = await!(insert_in_internal_node(handle.clone(), right_node, free_space_offset, entry_to_insert))?;
        free_space_offset = new_free_space_offset;
        let left_op = await!(left_node.cow(handle.clone(), &mut free_space_offset))?;
        let left_entry = NodeEntry::<K, ObjectPointer>::new(left_node.entries[0].key, left_op);
        (left_entry, right_entry, old_value)
    };

    Ok((left_entry, right_entry, free_space_offset, old_value))
}

#[async(boxed)] // box not really needed
pub fn insert_in_btree(handle: Handle, op: ObjectPointer, free_space_offset: u64, entry_to_insert: LeafNodeEntry) -> Result<(ObjectPointer, u64, Option<u64>), failure::Error> {
    // read pointed object
    let any_object = await!(op.async_read_object::<u64, u64, ConstUsize2>(handle.clone()))?;

    let (op, new_free_space_offset, old_value) = match any_object {
        AnyObject::LeafNode(node) => {
            // algo invariant
            debug_assert!(node.entries.len() <= BTREE_DEGREE); // b <= len <= 2b+1 with b=2 except root

            if node.entries.len() >= BTREE_DEGREE { // pro-active splitting if the node has the maximum size
                // split the node and insert in relevant child
                let (left_entry, right_entry, new_free_space_offset, old_value) = await!(leaf_split_and_insert(handle.clone(), *node, free_space_offset, entry_to_insert))?;
                free_space_offset = new_free_space_offset;

                // create new root
                let mut new_root = InternalNode::new();
                new_root.entries.push(left_entry);
                new_root.entries.push(right_entry);
                // no need to sort

                // COW new root
                let new_op = await!(new_root.cow(handle.clone(), &mut free_space_offset))?;
                (new_op, free_space_offset, old_value)
            } else {
                let (entry, free_space_offset, old_value) = await!(insert_in_leaf_node(handle, *node, free_space_offset, entry_to_insert))?;
                (entry.value, free_space_offset, old_value)
            }
        }
        AnyObject::InternalNode(node) => {
            // algo invariant
            debug_assert!(node.entries.len() <= BTREE_DEGREE); // b <= len <= 2b+1 with b=2 except root

            if node.entries.len() >= BTREE_DEGREE { // pro-active splitting if the node has the maximum size
                // split the node and insert in relevant child
                let (left_entry, right_entry, new_free_space_offset, old_value) = await!(internal_split_and_insert(handle.clone(), *node, free_space_offset, entry_to_insert))?;
                free_space_offset = new_free_space_offset;

                // create new root
                let mut new_root = InternalNode::new();
                new_root.entries.push(left_entry);
                new_root.entries.push(right_entry);
                // no need to sort

                // COW new root
                let new_op = await!(new_root.cow(handle.clone(), &mut free_space_offset))?;
                (new_op, free_space_offset, old_value)
            } else {
                let (entry, free_space_offset, old_value) = await!(insert_in_internal_node(handle, *node, free_space_offset, entry_to_insert))?;
                (entry.value, free_space_offset, old_value)
            }
        }
    };
    Ok((op, new_free_space_offset, old_value))
}

#[async(boxed)] // box not really needed
pub fn get<K: Serializable + Ord + Copy + 'static, V: Serializable + Copy, B: ConstUsize>(handle: Handle, op: ObjectPointer, key: K) -> Result<Option<V>, failure::Error> {
    // read root node
    let any_object = await!(op.async_read_object::<K, V, B>(handle.clone()))?;

    match any_object {
        AnyObject::LeafNode(node) => {
            // algo invariant
            debug_assert!(node.entries.len() <= BTREE_DEGREE); // b <= len <= 2b+1 with b=2 except root

            // algo invariant: the entries should be sorted
            debug_assert!(is_sorted(node.entries.iter().map(|l|{l.key})));

            let res = node.entries.binary_search_by_key(&key, |entry| entry.key);
            if let Ok(i) = res {
                return Ok(Some(node.entries[i].value));
            } else {
                return Ok(None);
            }
        }
        AnyObject::InternalNode(node) => {
            // algo invariant
            debug_assert!(node.entries.len() <= BTREE_DEGREE); // b <= len <= 2b+1 with b=2 except root

            // algo invariant: the entries should be sorted
            debug_assert!(is_sorted(node.entries.iter().map(|l|{l.key})));

            let res = node.entries.binary_search_by_key(&key, |entry| entry.key);
            let index = match res {
                Ok(i) => i, // exact match
                Err(0) => unreachable!("cow_btree: key should not be smaller than current's node smallest entry"),
                Err(i) => i - 1, // match first bigger key
            };

            await!(get::<K, V, B>(handle.clone(), node.entries[index].value.clone(), key))
        }
    }
}

#[async(boxed)] // box not really needed
fn remove_in_leaf<K: Serializable + Ord + Copy + 'static, V: Serializable + 'static, B: ConstUsize + 'static>
(handle: Handle, node: Node<K, V, B, Leaf>, free_space_offset: u64, key: K)
-> Result<(ObjectPointer, u64, Option<V>), failure::Error> {
    /*
        Here, we have the following garanties:
        - All nodes have a least one neighbor to fully or partially merge with.
        - If the root node is a leaf node, it has between 0 and 2B+1 entries
        - If the root node is an internal node, it has between 2 and 2B+1 entries
        - All non-root nodes have between B and 2B+1 entries  
    */

    // algo invariant: the entries should be sorted
    debug_assert!(is_sorted(node.entries.iter().map(|l|{l.key})));

    let res = node.entries.binary_search_by_key(&key, |entry| entry.key);

    let removed = if let Ok(i) = res {
        Some(node.entries.remove(i).value)
    } else {
        None
    };
    
    // COW node
    let op = await!(node.cow(handle.clone(), &mut free_space_offset))?;

    Ok((op, free_space_offset, removed))
}

#[async(boxed)]
fn remove_in_internal<K: Serializable + Ord + Copy + 'static, V: Serializable + 'static, B: ConstUsize + 'static>
(handle: Handle, node: Node<K, ObjectPointer, B, Internal>, mut free_space_offset: u64, key: K)
-> Result<(ObjectPointer, u64, Option<V>), failure::Error> {
    /*
        Here, we have the following garanties:
        - All nodes have a least one neighbor to fully or partially merge with.
        - If the root node is a leaf node, it has between 0 and 2B+1 entries
        - If the root node is an internal node, it has between 2 and 2B+1 entries
        - All non-root nodes have between B and 2B+1 entries  
    */

    // algo invariant: the entries should be sorted
    debug_assert!(is_sorted(node.entries.iter().map(|l|{l.key})));

    let res = node.entries.binary_search_by_key(&key, |entry| entry.key);
    let index = match res {
        Ok(i) => i, // exact match
        Err(0) => unreachable!("cow_btree: key should not be smaller than current's node smallest entry"),
        Err(i) => i - 1, // match first bigger key
    };

    // read the child on the way to the key to delete
    let child = await!(node.entries[index].value.async_read_object(handle.clone()))?;

    match child {
        AnyObject::LeafNode(mut child) => {
            // TODO: add asserts
            if child.entries.len() <= BTREE_B { // pro-active merging if the node has the minimum size

                let neighbor_index = if index > 0 { // if we have a left neighbor
                    index - 1
                } else if index < (node.entries.len() - 1) { // if we have a right neighbor
                    index + 1
                } else {
                    unreachable!("cow_btree: all node should have at least one neighbor at any time!");
                };

                let mut neighbor = match await!(node.entries[neighbor_index].value.async_read_object(handle.clone()))? { // TODO: use trait
                    AnyObject::LeafNode(n) => *n,
                    AnyObject::InternalNode(_) => unreachable!("cow_btree: all sibling should be of the same kind")
                };

                // TODO: add assert
                let removed_value = if child.entries.len() + neighbor.entries.len() <= BTREE_DEGREE { // if there is enough space to do a full merge
                    fuzz_marker!("cow_btree_remove_full_merge_leaf");

                    // figure out the direction of which node will be merge into which
                    let (mut src_node, src_index, mut dst_node, dst_index) = match neighbor_index as isize - index as isize {
                        -1 => (*child, index, neighbor, neighbor_index), // we are merging with left neighbor
                         1 => (neighbor, neighbor_index, *child, index), // we are merging with right neighbor
                         _ => unreachable!("cow_btree: invalid relative index")
                    };

                    // we drain the source node into the destination node
                    dst_node.entries.append(&mut src_node.entries);

                    // the entries should still be sorted
                    debug_assert!(is_sorted(dst_node.entries.iter().map(|l|{l.key})));

                    // recursion
                    let (op, new_free_space_offset, removed_value) = await!(remove_in_leaf(handle.clone(), dst_node, free_space_offset, key))?;
                    free_space_offset = new_free_space_offset;

                    // update child entry to point to the new node
                    node.entries[dst_index].value = op;

                    // TODO: try to swap the above and below lines and see if the fuzzer catches the bug

                    // remove the source node as it is empty now
                    node.entries.remove(src_index); // all entries to the right are shifted left

                    removed_value
                } else { // partial merge
                    // figure out how many entries we want to move
                    let nb_entries_to_move = neighbor.entries.len() / 2; // TODO: maybe there is a better value

                    match neighbor_index as isize - index as isize {
                        -1 => { // we are merging with left neighbor
                            fuzz_marker!("cow_btree_remove_partial_merge_left_leaf");

                            let mut entries = Vec::new();
                            mem::swap(&mut child.entries, &mut entries);

                            // move the entries from neighbor
                            for i in neighbor.entries.drain(nb_entries_to_move..) {
                                child.entries.place_back() <- i;
                            }

                            // move the entries from original child
                            child.entries.append(&mut entries);

                            // in node.entries, update key to child
                            node.entries[index].key = child.entries[0].key;
                        }
                         1 => { // we are merging with right neighbor
                            fuzz_marker!("cow_btree_remove_partial_merge_right_leaf");

                            // move the entries
                            for i in neighbor.entries.drain(..nb_entries_to_move) {
                                child.entries.place_back() <- i;
                            }

                            // in node.entries, update key to neighbor
                            node.entries[neighbor_index].key = neighbor.entries[0].key;
                         }
                         _ => unreachable!("cow_btree: invalid relative index")
                    };

                    // check that now the child has enough entries to substain one remove, and is not too big
                    debug_assert!(child.entries.len() >= BTREE_B + 1 && child.entries.len() <= BTREE_DEGREE); // b + 1 <= len <= 2b+1

                    // the entries should still be sorted
                    debug_assert!(is_sorted(child.entries.iter().map(|l|{l.key})));

                    // recursion
                    let (child_op, new_free_space_offset, removed_value) = await!(remove_in_leaf(handle.clone(), *child, free_space_offset, key))?;
                    free_space_offset = new_free_space_offset;

                    // update child entry to point to the new node
                    node.entries[index].value = child_op;

                    // COW the neighbor
                    let neighbor_op = await!(neighbor.cow(handle.clone(), &mut free_space_offset))?;
                    node.entries[neighbor_index].value = neighbor_op;

                    removed_value
                };
                
                if node.entries.len() > 1 { // common case
                    // COW the node
                    let op = await!(node.cow(handle.clone(), &mut free_space_offset))?;
                    return Ok((op, free_space_offset, removed_value));
                } else { // we are the root node and we can pop the head
                    fuzz_marker!("cow_btree_remove_pop_head_leaf");
                    return Ok((node.entries.remove(0).value, free_space_offset, removed_value));
                }
            } else { // there is enough entries in the node: no need to merge
                // TODO: find a way to factorize this code with the merge code
                // recursion
                let (op, new_free_space_offset, removed_value) = await!(remove_in_leaf(handle.clone(), *child, free_space_offset, key))?;
                free_space_offset = new_free_space_offset;

                // update child entry to point to the new node
                node.entries[index].value = op;

                // COW the node
                let op = await!(node.cow(handle.clone(), &mut free_space_offset))?;
                return Ok((op, free_space_offset, removed_value));
            }
        }
        AnyObject::InternalNode(mut child) => {
            // TODO: add asserts
            if child.entries.len() <= BTREE_B { // pro-active merging if the node has the minimum size

                let neighbor_index = if index > 0 { // if we have a left neighbor
                    index - 1
                } else if index < (node.entries.len() - 1) { // if we have a right neighbor
                    index + 1
                } else {
                    unreachable!("cow_btree: all node should have at least one neighbor at any time!");
                };

                let mut neighbor = match await!(node.entries[neighbor_index].value.async_read_object::<K, V, B>(handle.clone()))? { // TODO: use trait
                    AnyObject::InternalNode(n) => *n,
                    AnyObject::LeafNode(_) => unreachable!("cow_btree: all sibling should be of the same kind")
                };

                // TODO: add assert
                let removed_value = if child.entries.len() + neighbor.entries.len() <= BTREE_DEGREE { // if there is enough space to do a full merge
                    fuzz_marker!("cow_btree_remove_full_merge_internal");
                    // figure out the direction of which node will be merge into which
                    let (mut src_node, src_index, mut dst_node, dst_index) = match neighbor_index as isize - index as isize {
                        -1 => (*child, index, neighbor, neighbor_index), // we are merging with left neighbor
                         1 => (neighbor, neighbor_index, *child, index), // we are merging with right neighbor
                         _ => unreachable!("cow_btree: invalid relative index")
                    };

                    // we drain the source node into the destination node
                    dst_node.entries.append(&mut src_node.entries);

                    // the entries should still be sorted
                    debug_assert!(is_sorted(dst_node.entries.iter().map(|l|{l.key})));

                    // recursion
                    let (op, new_free_space_offset, removed_value) = await!(remove_in_internal(handle.clone(), dst_node, free_space_offset, key))?;
                    free_space_offset = new_free_space_offset;

                    // update child entry to point to the new node
                    node.entries[dst_index].value = op;

                    // TODO: try to swap the above and below lines and see if the fuzzer catches the bug

                    // remove the source node as it is empty now
                    node.entries.remove(src_index); // all entries to the right are shifted left

                    removed_value
                } else { // partial merge
                    // figure out how many entries we want to move
                    let nb_entries_to_move = neighbor.entries.len() / 2; // TODO: maybe there is a better value

                    match neighbor_index as isize - index as isize {
                        -1 => { // we are merging with left neighbor
                            fuzz_marker!("cow_btree_remove_partial_merge_left_internal");

                            let mut entries = Vec::new();
                            mem::swap(&mut child.entries, &mut entries);

                            // move the entries from neighbor
                            for i in neighbor.entries.drain(nb_entries_to_move..) {
                                child.entries.place_back() <- i;
                            }

                            // move the entries from original child
                            child.entries.append(&mut entries);

                            // in node.entries, update key to child
                            node.entries[index].key = child.entries[0].key;
                        }
                         1 => { // we are merging with right neighbor
                            fuzz_marker!("cow_btree_remove_partial_merge_right_internal");

                            // move the entries
                            for i in neighbor.entries.drain(..nb_entries_to_move) {
                                child.entries.place_back() <- i;
                            }

                            // in node.entries, update key to neighbor
                            node.entries[neighbor_index].key = neighbor.entries[0].key;
                         }
                         _ => unreachable!("cow_btree: invalid relative index")
                    };

                    // check that now the child has enough entries to substain one remove, and is not too big
                    debug_assert!(child.entries.len() >= BTREE_B + 1 && child.entries.len() <= BTREE_DEGREE); // b + 1 <= len <= 2b+1

                    // the entries should still be sorted
                    debug_assert!(is_sorted(child.entries.iter().map(|l|{l.key})));

                    // recursion
                    let (child_op, new_free_space_offset, removed_value) = await!(remove_in_internal(handle.clone(), *child, free_space_offset, key))?;
                    free_space_offset = new_free_space_offset;

                    // update child entry to point to the new node
                    node.entries[index].value = child_op;

                    // COW the neighbor
                    let neighbor_op = await!(neighbor.cow(handle.clone(), &mut free_space_offset))?;
                    node.entries[neighbor_index].value = neighbor_op;

                    removed_value
                };
                
                if node.entries.len() > 1 { // common case
                    // COW the node
                    let op = await!(node.cow(handle.clone(), &mut free_space_offset))?;
                    return Ok((op, free_space_offset, removed_value));
                } else { // we are the root node and we can pop the head
                    fuzz_marker!("cow_btree_remove_pop_head_internal");
                    return Ok((node.entries.remove(0).value, free_space_offset, removed_value));
                }
            } else { // there is enough entries in the node: no need to merge
                // TODO: find a way to factorize this code with the merge code
                // recursion
                let (op, new_free_space_offset, removed_value) = await!(remove_in_internal(handle.clone(), *child, free_space_offset, key))?;
                free_space_offset = new_free_space_offset;

                // update child entry to point to the new node
                node.entries[index].value = op;

                // COW the node
                let op = await!(node.cow(handle.clone(), &mut free_space_offset))?;
                return Ok((op, free_space_offset, removed_value));
            }
        }
    }

    unreachable!()
}


// TODO: write a version that does not do any modifications if the entry to remove doesn't exist
#[async(boxed)] // box not really needed
pub fn remove<K: Serializable + Ord + Copy + 'static, V: Serializable, B: ConstUsize>
(handle: Handle, op: ObjectPointer, free_space_offset: u64, key: K)
-> Result<(ObjectPointer, u64, Option<V>), failure::Error> {
    /*
        Here, we have the following garanties:
        - All nodes have a least one neighbor to fully or partially merge with.
        - If the root node is a leaf node, it has between 0 and 2B+1 entries
        - If the root node is an internal node, it has between 2 and 2B+1 entries
        - All non-root nodes have between B and 2B+1 entries  
    */

    // read pointed object
    let any_object = await!(op.async_read_object::<K, V, B>(handle.clone()))?;

    let (op, new_free_space_offset, removed_value) = match any_object {
        AnyObject::LeafNode(node) => {
            match node.entries.len() > 0 && key < node.entries[0].key { // if the key is smaller than the smallest key
                true  => (op, free_space_offset, None), // the key cannot be in the btree
                false => await!(remove_in_leaf(handle.clone(), *node, free_space_offset, key))?
            }
        }
        AnyObject::InternalNode(node) => {
            match node.entries.len() > 0 && key < node.entries[0].key { // if the key is smaller than the smallest key
                true  => (op, free_space_offset, None), // the key cannot be in the btree
                false => await!(remove_in_internal(handle.clone(), *node, free_space_offset, key))?
            }
        }
    };

    Ok((op, new_free_space_offset, removed_value))
}

#[async(boxed)]
pub fn print_btree<K: Serializable + Ord + Copy + Debug, V: Serializable + Debug, B: ConstUsize>(handle: Handle, op: ObjectPointer, indentation: usize) -> Result<(), failure::Error> {
    let any_object = await!(op.async_read_object::<K, V, B>(handle.clone()))?;

    match any_object {
        AnyObject::LeafNode(node) => {
            // algo invariant
            debug_assert!(node.entries.len() <= BTREE_DEGREE); // b <= len <= 2b+1 with b=2 except root

            println!("{} {:?}", "  ".repeat(indentation), node.entries);
        }
        AnyObject::InternalNode(node) => {
            // algo invariant
            debug_assert!(node.entries.len() <= BTREE_DEGREE); // b <= len <= 2b+1 with b=2 except root

            println!("{} {:?}", "  ".repeat(indentation), node.entries);
            for n in node.entries {
                await!(print_btree::<K, V, B>(handle.clone(), n.value, indentation + 1))?;
            }
        }
    }

    if indentation == 0 {
        println!();
    }

    Ok(())
}

#[async(boxed)]
pub fn read_btree<K: Serializable + Ord + Copy, V: Serializable, B: ConstUsize>(handle: Handle, op: ObjectPointer) -> Result<Vec<NodeEntry<K, V>>, failure::Error> {
    let mut v = vec![];
    let any_object = await!(op.async_read_object::<K, V, B>(handle.clone()))?;

    match any_object {
        AnyObject::LeafNode(mut node) => {
            // algo invariant
            debug_assert!(node.entries.len() <= BTREE_DEGREE); // b <= len <= 2b+1 with b=2 except root

            v.append(&mut node.entries);
        }
        AnyObject::InternalNode(node) => {
            // algo invariant
            debug_assert!(node.entries.len() <= BTREE_DEGREE); // b <= len <= 2b+1 with b=2 except root

            for n in node.entries {
                let mut res = await!(read_btree::<K, V, B>(handle.clone(), n.value))?;
                v.append(&mut res);
            }
        }
    }

    Ok(v)
}
