use std::u64;
use super::*;

impl LeafNode {
    pub fn new() -> LeafNode {
        LeafNode {
            entries: vec![],
        }
    }

    pub fn from_bytes(bytes: &mut Cursor<&[u8]>) -> Result<LeafNode, failure::Error> {
        let mut entries = vec![];

        while bytes.remaining() >= 8 + 8 {
            let key = bytes.get_u64::<LittleEndian>();
            let value = bytes.get_u64::<LittleEndian>();
            entries.push(LeafNodeEntry{key, value});
        }

        assert!(bytes.remaining() == 0);

        Ok(
            LeafNode {
                entries
            }
        )
    }

    pub fn to_mem(&self) -> Box<[u8]> {
        let size = self.entries.len()*(8*2);
        let mut mem = Vec::with_capacity(size);
        unsafe{mem.set_len(size)};
        self.to_bytes(&mut Cursor::new(&mut mem));
        return mem.into_boxed_slice();
    }

    pub fn to_bytes(&self, bytes: &mut Cursor<&mut [u8]>) {
        assert!(bytes.remaining_mut() >= self.entries.len() * (8 + 8));

        for LeafNodeEntry{key, value} in &self.entries {
            bytes.put_u64::<LittleEndian>(*key);
            bytes.put_u64::<LittleEndian>(*value);
        }
    }

    pub fn async_write_at(&self, handle: Handle, offset: u64) -> impl Future<Item=u64, Error=failure::Error> {
            handle.write(self.to_mem().to_vec(), offset)
    }

    // --

    fn insert(&mut self, entry: LeafNodeEntry) {
        let res = self.entries.binary_search_by_key(&entry.key, |e| e.key);
        if let Ok(i) = res {
            self.entries[i].value = self.entries[i].value.wrapping_add(entry.value).wrapping_mul(2); // non linear function
        } else {
            self.entries.push(entry);
            self.entries.sort_unstable_by_key(|entry| entry.key); // TODO be smart
        }
    }

    fn cow<'f>(&'f self, handle: Handle, fso: &'f mut u64) -> impl Future<Item=ObjectPointer, Error=failure::Error> + 'f {
        async_block! {
            let offset = *fso;
            let len = await!(self.async_write_at(handle.clone(), offset))?;
            *fso += len;
            let op = ObjectPointer {
                offset,
                len,
                object_type: ObjectType::LeafNode 
            };
            Ok(op)
        }
    }
}

impl InternalNode {

    pub fn new() -> InternalNode {
        InternalNode {
            entries: vec![],
        }
    }

    pub fn from_bytes(bytes: &mut Cursor<&[u8]>) -> Result<InternalNode, failure::Error> {
        let mut entries = vec![];

        while bytes.remaining() >= 8 + (8 + 8 + 1) {
            let key = bytes.get_u64::<LittleEndian>();
            let object_pointer = ObjectPointer::from_bytes(bytes)?;
            entries.push(InternalNodeEntry{key, object_pointer});
        }

        assert!(bytes.remaining() == 0);

        Ok(
            InternalNode {
                entries
            }
        )
    }

    pub fn to_mem(&self) -> Box<[u8]> {
        let size = self.entries.len()*(8 + (8 + 8 + 1));
        let mut mem = Vec::with_capacity(size);
        unsafe{mem.set_len(size)};
        self.to_bytes(&mut Cursor::new(&mut mem));
        return mem.into_boxed_slice();
    }

    fn to_bytes(&self, bytes: &mut Cursor<&mut [u8]>) {
        assert!(bytes.remaining_mut() >= self.entries.len() * (8 + (8 + 8 + 1)));

        for InternalNodeEntry{key, object_pointer} in &self.entries {
            bytes.put_u64::<LittleEndian>(*key);
            object_pointer.to_bytes(bytes);
        }
    }

    fn async_write_at(&self, handle: Handle, offset: u64) -> impl Future<Item=u64, Error=failure::Error> {
        handle.write(self.to_mem().to_vec(), offset)
    }

    // --

    fn cow<'f>(&'f self, handle: Handle, fso: &'f mut u64) -> impl Future<Item=ObjectPointer, Error=failure::Error> + 'f {
        async_block! {
            let offset = *fso;
            let len = await!(self.async_write_at(handle.clone(), offset))?;
            *fso += len;
            let op = ObjectPointer {
                offset,
                len,
                object_type: ObjectType::InternalNode 
            };
            Ok(op)
        }
    }
}

/// splits the given node if needed, then insert or recurse in entry
#[async(boxed)]
fn insert_in_btree_rec(handle: Handle, op: ObjectPointer, free_space_offset: u64, entry_to_insert: LeafNodeEntry) -> Result<(ObjectPointer, Option<InternalNodeEntry>, u64), failure::Error> {
    // read pointed object
    let mut any_object = await!(op.async_read_object(handle.clone()))?;

    match any_object {
        AnyObject::LeafNode(mut node) => {
            // algo invariant
            assert!(node.entries.len() <= BTREE_DEGREE); // b <= len <= 2b+1 with b=2 except root 
            if node.entries.len() < BTREE_DEGREE { // if there is enough space to insert
                node.insert(entry_to_insert);

                // COW node
                let op = await!(node.cow(handle.clone(), &mut free_space_offset))?;

                // return
                Ok((op, None, free_space_offset))
            } else { // split
                // rename node to left_node ...
                let mut left_node = node;
                // ... and split off its right half to right_node
                let right_entries = left_node.entries.split_off(BTREE_SPLIT); // split at b+1
                let mut right_node = LeafNode {
                    entries: right_entries
                };

                // insert entry in either node
                if entry_to_insert.key < right_node.entries[0].key { // are we smaller than the first element of the right half
                    left_node.insert(entry_to_insert);
                } else {
                    right_node.insert(entry_to_insert);
                }

                // COW left node
                let op = await!(left_node.cow(handle.clone(), &mut free_space_offset))?;

                // COW right node
                let new_op = await!(right_node.cow(handle.clone(), &mut free_space_offset))?;

                // return
                Ok((
                    op,
                    Some(InternalNodeEntry{key: right_node.entries[0].key, object_pointer: new_op}),
                    free_space_offset
                ))
            }

        }
        AnyObject::InternalNode(mut node) => {
            // algo invariant
            assert!(node.entries.len() <= BTREE_DEGREE); // b <= len <= 2b+1 with b=2 except root

            if node.entries.len() < BTREE_DEGREE { // no need to split
                // invariant: the array is sorted
                let res = node.entries.binary_search_by_key(&entry_to_insert.key, |entry| entry.key);
                let index = match res {
                    Ok(i) => { // exact match
                        i
                    }
                    Err(0) => 0, // smallest key is inserted in first child
                    Err(i) => { // match first bigger key
                        i - 1
                    }
                };

                // object pointer of branch where to insert
                let op = node.entries[index].object_pointer.clone();
                
                // recursion in child
                let (new_op, maybe_new_entry, mut free_space_offset) = 
                    await!(insert_in_btree_rec(handle.clone(), op, free_space_offset, entry_to_insert))?;

                // update new_op with COWed new child
                node.entries[index].object_pointer = new_op;

                // maybe add new entry from a potentially split child
                if let Some(new_entry) = maybe_new_entry {
                    node.entries.push(new_entry);
                    node.entries.sort_unstable_by_key(|entry| entry.key); // TODO be smart
                }

                // COW node
                let op = await!(node.cow(handle.clone(), &mut free_space_offset))?;

                // return
                Ok((op, None, free_space_offset))
            } else { // node is full: split
                // rename node to left_node ...
                let mut left_node = node;
                // ... and split off its right half to right_node
                let right_entries = left_node.entries.split_off(BTREE_SPLIT); // split at b+1
                let mut right_node = InternalNode {
                    entries: right_entries
                };

                // find in which branch to follow
                if entry_to_insert.key < right_node.entries[0].key {
                    // invariant: the array is sorted
                    let res = left_node.entries.binary_search_by_key(&entry_to_insert.key, |entry| entry.key);
                    let index = match res {
                        Ok(i) => { // exact match
                            i
                        }
                        Err(0) => 0, // smallest key is inserted in first child
                        Err(i) => { // match first bigger key
                            i - 1
                        }
                    };

                    // object pointer of branch where to insert
                    let op = left_node.entries[index].object_pointer.clone();

                    // recursion in child
                    let (new_op, maybe_new_entry, new_free_space_offset) = 
                        await!(insert_in_btree_rec(handle.clone(), op, free_space_offset, entry_to_insert))?;
                    free_space_offset = new_free_space_offset;

                    // update new_op with COWed new child
                    left_node.entries[index].object_pointer = new_op;

                    // maybe add new entry from a potentially split child
                    if let Some(new_entry) = maybe_new_entry {
                        left_node.entries.push(new_entry);
                        left_node.entries.sort_unstable_by_key(|entry| entry.key); // TODO be smart
                    }
                } else {
                    let res = right_node.entries.binary_search_by_key(&entry_to_insert.key, |entry| entry.key);
                    let index = match res {
                        Ok(i) => { // exact match
                            i
                        }
                        Err(0) => unreachable!("new key cannot be smaller than first key of right split"),
                        Err(i) => { // match first bigger key
                            i - 1
                        }
                    };

                    // object pointer of branch where to insert
                    let op = right_node.entries[index].object_pointer.clone();
                    
                    // recursion in child
                    let (new_op, maybe_new_entry, new_free_space_offset) = 
                        await!(insert_in_btree_rec(handle.clone(), op, free_space_offset, entry_to_insert))?;
                    free_space_offset = new_free_space_offset;

                    // update new_op with COWed new child
                    right_node.entries[index].object_pointer = new_op;

                    // maybe add new entry from a potentially split child
                    if let Some(new_entry) = maybe_new_entry {
                        right_node.entries.push(new_entry);
                        right_node.entries.sort_unstable_by_key(|entry| entry.key); // TODO be smart
                    }
                }

                // COW left node
                let op = await!(left_node.cow(handle.clone(), &mut free_space_offset))?;

                // COW right node
                let new_op = await!(right_node.cow(handle.clone(), &mut free_space_offset))?;

                // return
                Ok((
                    op,
                    Some(InternalNodeEntry{key: right_node.entries[0].key, object_pointer: new_op}),
                    free_space_offset
                ))
            }
        }
    }
}

#[async]
pub fn insert_in_btree(handle: Handle, op: ObjectPointer, free_space_offset: u64, entry: LeafNodeEntry) -> Result<(ObjectPointer, u64), failure::Error> {
    // use recursive function to insert
    let (new_op, maybe_new_entry, mut free_space_offset) = await!(insert_in_btree_rec(handle.clone(), op, free_space_offset, entry))?;
    
    // we might get back two object pointers...
    if let Some(new_entry) = maybe_new_entry { // ... if so, create a new root pointing to both 
        let mut new_root = InternalNode::new();
        new_root.entries.push(InternalNodeEntry{key: u64::MIN, object_pointer: new_op});
        new_root.entries.push(new_entry);
        new_root.entries.sort_unstable_by_key(|entry| entry.key); // TODO be smart

        let op_root = await!(new_root.cow(handle.clone(), &mut free_space_offset))?;

        // return
        Ok((op_root, free_space_offset))
    }
    else {
        Ok((new_op, free_space_offset))
    }
}

/// insert or go in entry then split 
#[async]
fn insert_in_leaf_node(handle: Handle, node: LeafNode, free_space_offset: u64, entry_to_insert: LeafNodeEntry) -> Result<(ObjectPointer, u64), failure::Error> {
    // algo invariant
    assert!(node.entries.len() < BTREE_DEGREE); // b <= len <= 2b+1 with b=2 except root. we need one free slot to insert
    node.insert(entry_to_insert);

    // COW node
    let op = await!(node.cow(handle.clone(), &mut free_space_offset))?;

    Ok((op, free_space_offset))
}

/// insert or go in entry then split 
#[async(boxed)]
fn insert_in_internal_node(handle: Handle, cur_node: InternalNode, free_space_offset: u64, entry_to_insert: LeafNodeEntry) -> Result<(ObjectPointer, u64), failure::Error> {
    // algo invariant
    assert!(cur_node.entries.len() < BTREE_DEGREE); // b <= len <= 2b+1 with b=2 except root. we need one free slot to insert

    // invariant: the array is sorted
    let res = cur_node.entries.binary_search_by_key(&entry_to_insert.key, |entry| entry.key);
    let index = match res {
        Ok(i) => { // exact match
            i
        }
        Err(0) => 0, // smallest key is inserted in first child
        Err(i) => { // match first bigger key
            i - 1
        }
    };

    // object pointer of branch where to insert
    let op = cur_node.entries[index].object_pointer.clone();
    
    // read pointed object
    let mut any_object = await!(op.async_read_object(handle.clone()))?;

    match any_object {
        AnyObject::LeafNode(child_node) => {
            if child_node.entries.len() < BTREE_DEGREE { // if there is enough space to insert
                let (child_op, new_free_space_offset) = await!(insert_in_leaf_node(handle.clone(), *child_node, free_space_offset, entry_to_insert))?;
                free_space_offset = new_free_space_offset;

                // in current's node selected entry, update object pointer to point to new child node
                cur_node.entries[index].object_pointer = child_op;
            } else { // split
                // rename node to left_node ...
                let mut left_node = child_node;
                // ... and split off its right half to right_node
                let right_entries = left_node.entries.split_off(BTREE_SPLIT); // split at b+1
                let mut right_node = LeafNode {
                    entries: right_entries
                };

                let median = right_node.entries[0].key;

                // insert entry in either node
                let (left_op, right_op) = if entry_to_insert.key < right_node.entries[0].key { // are we smaller than the first element of the right half
                    // TODO maybe inlining insert_in_leaf_node code would be simpler
                    let (left_op, new_free_space_offset) = await!(insert_in_leaf_node(handle.clone(), *left_node, free_space_offset, entry_to_insert))?;
                    free_space_offset = new_free_space_offset;
                    let right_op = await!(right_node.cow(handle.clone(), &mut free_space_offset))?;
                    (left_op, right_op)
                } else {
                    // TODO maybe inlining insert_in_leaf_node code would be simpler
                    let (right_op, new_free_space_offset) = await!(insert_in_leaf_node(handle.clone(), right_node, free_space_offset, entry_to_insert))?;
                    free_space_offset = new_free_space_offset;
                    let left_op = await!(left_node.cow(handle.clone(), &mut free_space_offset))?;
                    (left_op, right_op)
                };

                // in current's node selected entry, update object pointer to point to left node
                cur_node.entries[index].object_pointer = left_op;

                // push in current node new entry which points to right_node
                cur_node.entries.push(InternalNodeEntry{key: median, object_pointer: right_op});
                cur_node.entries.sort_unstable_by_key(|entry| entry.key); // TODO be smart
            }

            // COW that new node
            let op = await!(cur_node.cow(handle.clone(), &mut free_space_offset))?;

            // return
            Ok((
                op,
                free_space_offset
            ))
        }
        AnyObject::InternalNode(mut child_node) => {
            if child_node.entries.len() < BTREE_DEGREE { // if there is enough space to insert
                let (child_op, new_free_space_offset) = await!(insert_in_internal_node(handle.clone(), *child_node, free_space_offset, entry_to_insert))?;
                free_space_offset = new_free_space_offset;

                // in current's node selected entry, update object pointer to point to new child node
                cur_node.entries[index].object_pointer = child_op;
            } else { // split
                // rename node to left_node ...
                let mut left_node = child_node;
                // ... and split off its right half to right_node
                let right_entries = left_node.entries.split_off(BTREE_SPLIT); // split at b+1
                let mut right_node = InternalNode {
                    entries: right_entries
                };

                let median = right_node.entries[0].key;

                // insert entry in either node
                let (left_op, right_op) = if entry_to_insert.key < right_node.entries[0].key { // are we smaller than the first element of the right half
                    // TODO maybe inlining insert_in_internal_node code would be simpler
                    let (left_op, new_free_space_offset) = await!(insert_in_internal_node(handle.clone(), *left_node, free_space_offset, entry_to_insert))?;
                    free_space_offset = new_free_space_offset;
                    let right_op = await!(right_node.cow(handle.clone(), &mut free_space_offset))?;
                    (left_op, right_op)
                } else {
                    // TODO maybe inlining insert_in_internal_node code would be simpler
                    let (right_op, new_free_space_offset) = await!(insert_in_internal_node(handle.clone(), right_node, free_space_offset, entry_to_insert))?;
                    free_space_offset = new_free_space_offset;
                    let left_op = await!(left_node.cow(handle.clone(), &mut free_space_offset))?;
                    (left_op, right_op)
                };

                // in current's node selected entry, update object pointer to point to left node
                cur_node.entries[index].object_pointer = left_op;

                // push in current node new entry which points to right_node
                cur_node.entries.push(InternalNodeEntry{key: median, object_pointer: right_op});
                cur_node.entries.sort_unstable_by_key(|entry| entry.key); // TODO be smart
            }

            // COW that new node
            let op = await!(cur_node.cow(handle.clone(), &mut free_space_offset))?;

            // return
            Ok((
                op,
                free_space_offset
            ))
        }
    }
}

#[async]
pub fn insert_in_btree_2(handle: Handle, op: ObjectPointer, free_space_offset: u64, entry_to_insert: LeafNodeEntry) -> Result<(ObjectPointer, u64), failure::Error> {
    // read pointed object
    let mut any_object = await!(op.async_read_object(handle.clone()))?;

    let (op, new_free_space_offset) = match any_object {
        AnyObject::LeafNode(node) => {
            if node.entries.len() >= BTREE_DEGREE { // if there is not enough space to insert
                // rename node to left_node ...
                let mut left_node = node;
                // ... and split off its right half to right_node
                let right_entries = left_node.entries.split_off(BTREE_SPLIT); // split at b+1
                let mut right_node = LeafNode {
                    entries: right_entries
                };

                let median = right_node.entries[0].key;

                // insert entry in either node
                let (left_op, right_op) = if entry_to_insert.key < right_node.entries[0].key { // are we smaller than the first element of the right half
                    // TODO maybe inlining insert_in_internal_node code would be simpler
                    let (left_op, new_free_space_offset) = await!(insert_in_leaf_node(handle.clone(), *left_node, free_space_offset, entry_to_insert))?;
                    free_space_offset = new_free_space_offset;
                    let right_op = await!(right_node.cow(handle.clone(), &mut free_space_offset))?;
                    (left_op, right_op)
                } else {
                    // TODO maybe inlining insert_in_internal_node code would be simpler
                    let (right_op, new_free_space_offset) = await!(insert_in_leaf_node(handle.clone(), right_node, free_space_offset, entry_to_insert))?;
                    free_space_offset = new_free_space_offset;
                    let left_op = await!(left_node.cow(handle.clone(), &mut free_space_offset))?;
                    (left_op, right_op)
                };

                // create new root
                let mut new_root = InternalNode::new();
                new_root.entries.push(InternalNodeEntry{key: u64::MIN, object_pointer: left_op});
                new_root.entries.push(InternalNodeEntry{key: median, object_pointer: right_op});
                // no need to sort

                let new_op = await!(new_root.cow(handle.clone(), &mut free_space_offset))?;
                (new_op, free_space_offset)
            } else {
                await!(insert_in_leaf_node(handle, *node, free_space_offset, entry_to_insert))?
            }
        }
        AnyObject::InternalNode(mut node) => {
            if node.entries.len() >= BTREE_DEGREE { // if there is not enough space to insert
                // rename node to left_node ...
                let mut left_node = node;
                // ... and split off its right half to right_node
                let right_entries = left_node.entries.split_off(BTREE_SPLIT); // split at b+1
                let mut right_node = InternalNode {
                    entries: right_entries
                };

                let median = right_node.entries[0].key;

                // insert entry in either node
                let (left_op, right_op) = if entry_to_insert.key < right_node.entries[0].key { // are we smaller than the first element of the right half
                    // TODO maybe inlining insert_in_internal_node code would be simpler
                    let (left_op, new_free_space_offset) = await!(insert_in_internal_node(handle.clone(), *left_node, free_space_offset, entry_to_insert))?;
                    free_space_offset = new_free_space_offset;
                    let right_op = await!(right_node.cow(handle.clone(), &mut free_space_offset))?;
                    (left_op, right_op)
                } else {
                    // TODO maybe inlining insert_in_internal_node code would be simpler
                    let (right_op, new_free_space_offset) = await!(insert_in_internal_node(handle.clone(), right_node, free_space_offset, entry_to_insert))?;
                    free_space_offset = new_free_space_offset;
                    let left_op = await!(left_node.cow(handle.clone(), &mut free_space_offset))?;
                    (left_op, right_op)
                };

                // create new root
                let mut new_root = InternalNode::new();
                new_root.entries.push(InternalNodeEntry{key: u64::MIN, object_pointer: left_op});
                new_root.entries.push(InternalNodeEntry{key: median, object_pointer: right_op});
                // no need to sort

                let new_op = await!(new_root.cow(handle.clone(), &mut free_space_offset))?;
                (new_op, free_space_offset)
            } else {
                await!(insert_in_internal_node(handle, *node, free_space_offset, entry_to_insert))?
            }
        }
    };
    Ok((op, new_free_space_offset))
}

#[async(boxed)]
pub fn print_btree(handle: Handle, op: ObjectPointer, indentation: usize) -> Result<(), failure::Error> {
    let mut any_object = await!(op.async_read_object(handle.clone()))?;

    match any_object {
        AnyObject::LeafNode(node) => {
            assert!(node.entries.len() <= BTREE_DEGREE);
            println!("{} {:?}", "  ".repeat(indentation), node.entries);
        }
        AnyObject::InternalNode(node) => {
            assert!(node.entries.len() <= BTREE_DEGREE);
            println!("{} {:?}", "  ".repeat(indentation), node.entries);
            for n in node.entries {
                await!(print_btree(handle.clone(), n.object_pointer, indentation + 1))?;
            }
        }
    }

    if indentation == 0 {
        println!();
    }

    Ok(())
}

#[async(boxed)]
pub fn read_btree(handle: Handle, op: ObjectPointer) -> Result<Vec<LeafNodeEntry>, failure::Error> {
    let mut v = vec![];
    let mut any_object = await!(op.async_read_object(handle.clone()))?;

    match any_object {
        AnyObject::LeafNode(mut node) => {
            assert!(node.entries.len() <= BTREE_DEGREE);
            v.append(&mut node.entries);
        }
        AnyObject::InternalNode(node) => {
            assert!(node.entries.len() <= BTREE_DEGREE);
            for n in node.entries {
                let mut res = await!(read_btree(handle.clone(), n.object_pointer))?;
                v.append(&mut res);
            }
        }
    }

    Ok(v)
}
