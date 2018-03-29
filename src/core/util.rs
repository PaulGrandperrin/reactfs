use super::*;

#[async]
pub fn format(handle: Handle) -> Result<(), failure::Error> {
    let mut free_space_offset = 10 * BLOCK_SIZE as u64;
    
    // write tree
    let mut tree = LeafNode::new();
    let tree_offset = free_space_offset;
    let tree_len = await!(tree.async_write_at(handle.clone(), free_space_offset))?;
    free_space_offset += tree_len;

    // create pointer to tree
    let op = ObjectPointer::new(tree_offset, tree_len, ObjectType::LeafNode);

    // create all uberblocks
    let writes: Vec<_> = (0..10)
        .map(|i| {
            let s: Box<[u8]> = Uberblock::new(i, op.clone(), free_space_offset).to_mem();
            handle.write(s.into_vec(), i*BLOCK_SIZE as u64)
        })
        .collect();

    // write all uberblocks
    await!(future::join_all(writes))?;
    Ok(())
}

#[inline]
pub fn is_sorted<I: Iterator<Item=T>, T: Ord>(mut it: I) -> bool {
    let last: T = match it.next() {
        Some(i) => i,
        None => return true
    };

    for i in it {
        if i <= last {
            return false;
        }
    }
    true
}
