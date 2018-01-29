use std::mem;
use std::fmt;
use std::io::Write;

use failure;
use futures::future;
use reactor::*;
use futures::prelude::*;
use itertools::Itertools;
use num_traits::{FromPrimitive, ToPrimitive};
use bytes::{Buf, BufMut, LittleEndian};
use std::io::Cursor;

#[cfg(test)]
mod tests;

const MAGIC_NUMBER: &[u8] = b"ReactFS0";
const BLOCK_SIZE: usize = 4096;

#[derive(Debug, Clone, Primitive)]
enum ObjectType {
    InternalNode = 0,
    LeafNode = 1
}

#[derive(Debug)]
enum AnyObject {
    LeafNode(Box<LeafNode>),
    InternalNode(Box<InternalNode>),
}

#[derive(Debug)]
struct InternalNodeEntry {
    key: u64,
    object_pointer: ObjectPointer
}

#[derive(Debug)]
struct InternalNode {
    entries: Vec<InternalNodeEntry>,
}

#[derive(Debug)]
struct LeafNodeEntry {
    key: u64,
    value: u64
}

#[derive(Debug)]
struct LeafNode {
    entries: Vec<LeafNodeEntry>,
}

#[derive(Debug, Clone)]
struct ObjectPointer {
    offset: u64,
    len: u64,
    object_type: ObjectType
    // checksum
}

#[derive(Debug)]
struct Uberblock {
    tgx: u64,
    free_space_offset: u64,
    tree_root_pointer: ObjectPointer,
}

impl ObjectPointer {
    fn new(offset: u64, len: u64, object_type: ObjectType) -> ObjectPointer {
        ObjectPointer {
            offset,
            len,
            object_type,
        }
    }

    fn from_bytes(bytes: &mut Cursor<&[u8]>) -> Result<ObjectPointer, failure::Error> {
        assert!(bytes.remaining() >= 8 + 8 + 1);
        
        let offset = bytes.get_u64::<LittleEndian>();
        let len = bytes.get_u64::<LittleEndian>();
        let object_type = ObjectType::from_u8(bytes.get_u8()).
            ok_or(format_err!("Unknown ObjectType"))?;

        Ok(
            ObjectPointer {
                offset,
                len,
                object_type,
            }
        )
    }

    fn to_bytes(&self, bytes: &mut Cursor<&mut [u8]>) {
        assert!(bytes.remaining_mut() >= 8 + 8 + 1);
        
        bytes.put_u64::<LittleEndian>(self.offset);
        bytes.put_u64::<LittleEndian>(self.len);
        bytes.put_u8(self.object_type.to_u8().unwrap()); // there is less than 2^8 types
    }

    fn async_read_object(&self, handle: Handle) -> impl Future<Item=AnyObject, Error=failure::Error> {
        let object_type = self.object_type.clone();

        handle.read(self.offset, self.len).and_then(move |mem|{
            match object_type {
                ObjectType::LeafNode => {
                    Ok(AnyObject::LeafNode(Box::new(
                        LeafNode::from_bytes(&mut Cursor::new(&mem))?
                    )))
                }
                ObjectType::InternalNode => {
                    Ok(AnyObject::InternalNode(Box::new(
                        InternalNode::from_bytes(&mut Cursor::new(&mem))?
                    )))
                }
                _ => unimplemented!()
            }
        })
    }

    /*
    fn async_read_object<'f>(&'f self, handle: Handle) -> impl Future<Item=AnyObject, Error=failure::Error> + 'f {
        async_block!{
            let mem = await!(handle.read(self.offset, self.len))?;
            match self.object_type {
                ObjectType::LeafNode => {
                    Ok(AnyObject::LeafNode(Box::new(
                        LeafNode::from_bytes(&mut Cursor::new(&mem))?
                    )))
                }
                ObjectType::InternalNode => {
                    Ok(AnyObject::InternalNode(Box::new(
                        InternalNode::from_bytes(&mut Cursor::new(&mem))?
                    )))
                }
                _ => unimplemented!()
            }
        }
    }
    */
}

impl Uberblock {

    fn new(tgx: u64, tree_root_pointer: ObjectPointer, free_space_offset: u64) -> Uberblock {
        Uberblock {
            tgx,
            free_space_offset,
            tree_root_pointer,
        }
    }

    fn from_bytes(bytes: &mut Cursor<&[u8]>) -> Result<Uberblock, failure::Error> {
        assert!(bytes.remaining() >= 8 + 8 + 8 + (8 + 8 + 1));

        let mut magic= [0;8];
        bytes.copy_to_slice(&mut magic);
        if magic != MAGIC_NUMBER {
            return Err(format_err!("Incorrect magic number. found: {:?}, expected: {:?}", magic, MAGIC_NUMBER));
        }
        let tgx = bytes.get_u64::<LittleEndian>();
        let free_space_offset = bytes.get_u64::<LittleEndian>();
        let tree_root_pointer = ObjectPointer::from_bytes(bytes)?;

        assert!(bytes.remaining() == 0);

        Ok(
            Uberblock {
                tgx,
                tree_root_pointer,
                free_space_offset,
            }
        )
    }

    fn to_bytes(&self, bytes: &mut Cursor<&mut [u8]>) {
        assert!(bytes.remaining_mut() >= 8 + 8 + 8);
        
        bytes.put_slice(MAGIC_NUMBER);
        bytes.put_u64::<LittleEndian>(self.tgx);
        bytes.put_u64::<LittleEndian>(self.free_space_offset);
        self.tree_root_pointer.to_bytes(bytes);
    }

    fn to_mem(&self) -> Box<[u8]> {
        let mut mem: Box<[u8;41]> = Box::new(unsafe{mem::uninitialized()});
        self.to_bytes(&mut Cursor::new(&mut *mem));
        return mem;
    }

    fn async_write_at(&self, handle: Handle, offset: u64) -> impl Future<Item=u64, Error=failure::Error> {
        handle.write(self.to_mem().to_vec(), offset)
    }
}

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

    fn to_mem(&self) -> Box<[u8]> {
        let size = self.entries.len()*(8*2);
        let mut mem = Vec::with_capacity(size);
        unsafe{mem.set_len(size)};
        self.to_bytes(&mut Cursor::new(&mut mem));
        return mem.into_boxed_slice();
    }

    fn to_bytes(&self, bytes: &mut Cursor<&mut [u8]>) {
        assert!(bytes.remaining_mut() >= self.entries.len() * (8 + 8));

        for LeafNodeEntry{key, value} in &self.entries {
            bytes.put_u64::<LittleEndian>(*key);
            bytes.put_u64::<LittleEndian>(*value);
        }
    }

    fn async_write_at(&self, handle: Handle, offset: u64) -> impl Future<Item=u64, Error=failure::Error> {
            handle.write(self.to_mem().to_vec(), offset)
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

    fn to_mem(&self) -> Box<[u8]> {
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
}

#[async]
fn format(handle: Handle) -> Result<(), failure::Error> {
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

#[async]
fn find_latest_uberblock(handle: Handle) -> Result<Uberblock, failure::Error> {
    let uberblocks = await!(handle.read(0, BLOCK_SIZE as u64 *10))?;
    let uberblock = uberblocks.chunks(BLOCK_SIZE)
        .map(|chunk| {
            Uberblock::from_bytes(&mut Cursor::new(&chunk[0..(8 + 8 + 8 + (8 + 8 + 1))]))
        })
        .fold_results(None::<Uberblock>, |acc, u| { // compute max if no error
            if let Some(acc) = acc {
                if u.tgx <= acc.tgx {
                    return Some(acc)
                }
            }
            Some(u)
        }).map(|o| {
            o.unwrap() // guaranted to succeed
        });

    uberblock
}

#[async]
fn write_new_uberblock(handle: Handle, uberblock: Uberblock) -> Result<(), failure::Error> {
    // first we find the oldest uberblock offset
    let data = await!(handle.read(0, BLOCK_SIZE as u64 *10))?;
    let (offset, _tgx) = data.chunks(BLOCK_SIZE).enumerate()
        .map(|(i, chunk)| {
            Uberblock::from_bytes(&mut Cursor::new(&chunk[0..(8 + 8 + 8 + (8 + 8 + 1))])).map(|u|{
                (i, u)
            })
        })
        .fold_results(None::<(usize, u64)>, |acc, u| {
            if let Some(acc) = acc {
                if u.1.tgx <= acc.1 {
                    return Some(acc)
                }
            }
            Some((u.0, u.1.tgx))
        }).map(|o| {
            o.unwrap() // guaranted to succeed
        })?;

    // now write the new uberblock in place of the oldest
    await!(handle.write(uberblock.to_mem().into_vec(), (offset*BLOCK_SIZE) as u64))?;

    Ok(())
}
