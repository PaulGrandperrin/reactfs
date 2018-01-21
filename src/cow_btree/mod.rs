use std::mem;
use std::fmt;
use std::io::Write;

use failure;
use futures::future;
use futures::Future;
use reactor::*;
use byteorder;
use byteorder::{ByteOrder};
use futures::prelude::*;
use num_traits::FromPrimitive;

const MAGIC_NUMBER: &[u8] = b"ReactFS0";
pub type SectorBuffer = [u8; 4096];
pub type SectorOffset = u64;

pub struct InternalNode {
	mem: Vec<u8>,
	len: u64
}

pub struct LeafNode {
	mem: Vec<u8>,
	len: u64
}

#[derive(Primitive)]
pub enum Kind {
	InternalNode = 0,
	LeafNode = 1
}

pub enum TreeNode {
	Internal(InternalNode),
	Leaf(LeafNode)
}

impl TreeNode {
	fn from_mem(mem: Vec<u8>) -> TreeNode {
		assert_eq!(mem.len(), 4096);
		let kind = Kind::from_u8(mem[0]);
		let len = byteorder::LittleEndian::read_u64(&mem[1..8+1]);
		match kind {
			Some(Kind::InternalNode) => {
				TreeNode::Internal(InternalNode {
					mem,
					len
				})
			},
			Some(Kind::LeafNode) => {
				TreeNode::Leaf(LeafNode {
					mem,
					len
				})
			}
			None => {
				panic!("unexpected node kind");
			}
		}
	}

}

impl LeafNode {
	fn into_mem(mut self) -> Vec<u8> {
		// save state
		self.mem[0] = Kind::LeafNode as u8;
		byteorder::LittleEndian::write_u64(&mut self.mem[1..], self.len);

		self.mem
	}

	fn insert(&mut self, key: u64) -> bool {
		if self.len == 3
			{return false;}

		byteorder::LittleEndian::write_u64(&mut self.mem[(8+self.len*8) as usize..], key);
		self.len += 1;

		true
	}
}

impl InternalNode {

	pub fn new_empty() -> InternalNode {
		InternalNode {
			mem: vec![0u8; 4096],
			len: 0
		}
	}

	fn into_mem(mut self) -> Vec<u8> {
		// save state
		self.mem[0] = Kind::InternalNode as u8;
		byteorder::LittleEndian::write_u64(&mut self.mem[1..], self.len);

		self.mem
	}

	pub fn print_keys(&self) {
		
		for i in 0..self.len {
			let key = byteorder::LittleEndian::read_u64(&self.mem[(8+8*i) as usize .. (8+8*i+8) as usize]);
			println!("key {}: {}", i, key);
		}
	}

	pub fn insert(&mut self, key: u64) -> bool {
		if self.len == 3
			{return false;}

		byteorder::LittleEndian::write_u64(&mut self.mem[(8+self.len*8) as usize..], key);
		self.len += 1;

		true
	}
}

fn _write_u64(h: &Handle, n: u64, offset: u64) -> FutureWrite {
    let mut v = vec![0;8];
    byteorder::LittleEndian::write_u64(&mut v, n);
    h.write(v, offset*8)
}

fn _read_u64(h: &Handle, offset: u64) -> impl Future<Item=u64, Error=failure::Error> {
    h.read(offset*8, 8).map(|v| {
        byteorder::LittleEndian::read_u64(&v)
    })
}

fn read_internal_node(h: Handle, offset: u64) -> impl Future<Item=TreeNode, Error=failure::Error> {
	async_block!{
		let mem = await!(h.read(offset*4096, 4096))?;
		let node = TreeNode::from_mem(mem);
		//intern_node.print_keys();
		Ok(node)
	}
}

fn write_internal_node(h: Handle, offset: u64, intern_node: InternalNode) -> impl Future<Item=(), Error=failure::Error> {
	async_block!{
		await!(h.write(intern_node.into_mem(), offset*4096))?;
		Ok(())
	}
}

struct Uberblock {
	mem: Box<SectorBuffer>,
	tgx: u64,
	tree_root_offset: SectorOffset,
	free_space_offset: SectorOffset
}


impl fmt::Debug for Uberblock {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Uberblock {{ tgx: {}, tree_root_offset: {}, free_space_offset: {} }}", self.tgx, self.tree_root_offset, self.free_space_offset)
    }
}

impl Uberblock {

	fn new(tgx: u64, tree_root_offset: SectorOffset, free_space_offset: SectorOffset) -> Uberblock {
		Uberblock {
			mem: Box::new([0u8; 4096]),
			tgx,
			tree_root_offset,
			free_space_offset
		}
	}

	fn from_slice(slice: &[u8]) -> Uberblock {
		assert_eq!(slice.len(), 4096);
		let mut mem: Box<SectorBuffer> = Box::new(unsafe{mem::uninitialized()});
		mem.copy_from_slice(slice);
		
		assert_eq!(&mem[0..8], MAGIC_NUMBER, "Incorrect magic number. found: {:?}, expected: {:?}", &mem[0..8], MAGIC_NUMBER);
		let tgx = byteorder::LittleEndian::read_u64(&mem[8..16]);
		let tree_root_offset = byteorder::LittleEndian::read_u64(&mem[16..24]);
		let free_space_offset = byteorder::LittleEndian::read_u64(&mem[24..32]);

		Uberblock {
			mem,
			tgx,
			tree_root_offset,
			free_space_offset,
		}
	}

	fn into_sectorbuf(mut self) -> Box<SectorBuffer> {
		(&mut self.mem[0..]).write(MAGIC_NUMBER);
		byteorder::LittleEndian::write_u64(&mut self.mem[8..], self.tgx);
		byteorder::LittleEndian::write_u64(&mut self.mem[16..], self.tree_root_offset);
		byteorder::LittleEndian::write_u64(&mut self.mem[24..], self.free_space_offset);

		self.mem//.clone() // FIXME remove clone and drop 
	}
}

//impl Drop for Uberblock {
//	fn drop(&mut self) {
//		println!("dropping uberblock {}", self.tgx);
//	}
//}

fn create_tree(handle: Handle, uberblock: Box<Uberblock>) -> impl Future<Item=(), Error=failure::Error> {
    async_block!{
	    let intern_node = InternalNode::new_empty();
	    await!(write_internal_node(handle.clone(), uberblock.tree_root_offset, intern_node))?;

	    Ok(())
	}
}

fn format_fs(handle: Handle) -> impl Future<Item=(), Error=failure::Error> {
	async_block!{
		let writes: Vec<_> = (0..10)
			.map(|i| {
				let s: Box<[u8]> = Uberblock::new(i, 10, 11).into_sectorbuf();
				handle.write(s.into_vec(), i*4096)
			})
			.collect();

		await!(future::join_all(writes))?;

		let mut u = Uberblock::new(9, 10, 11);
		await!(create_tree(handle.clone(), Box::new(u)))?;

		Ok(())
	}
}

fn read_latest_uberblock(handle: Handle) -> impl Future<Item=Uberblock, Error=failure::Error> {
	async_block!{
		let uberblocks = await!(handle.read(0, 4096*10))?;
		let uberblock = uberblocks.chunks(4096)
			.map(|chunk| {Uberblock::from_slice(chunk)})
			.max_by(|x, y| {
				x.tgx.cmp(&y.tgx)
			}).unwrap();

		Ok(uberblock)
	}
}


pub fn test(handle: Handle) -> impl Future<Item=(), Error=failure::Error> {
	async_block!{
		await!(format_fs(handle.clone()))?;
	    let a= vec![1];
	    //let b = &a;
	    let u = await!(read_latest_uberblock(handle.clone()))?;
	    //println!("{:?}", b);


	    insert_in_tree(handle.clone(), &u);

	    println!("{:?}", u);
	    Ok(())
	}
}


fn insert_in_tree(handle: Handle, uberblock: *const Uberblock) -> Box<impl Future<Item=(), Error=failure::Error>> {
    Box::new(async_block!{
	    let uberblock: &Uberblock = unsafe{&*uberblock};
	    let mut intern_node = await!(read_internal_node(handle.clone(), uberblock.tree_root_offset))?;
	    match intern_node {
	    	TreeNode::Internal(n) => {
	    		
	    	},
	    	TreeNode::Leaf(n) => {
	    		
	    	}
	    };
	    //await!(write_internal_node(handle.clone(), 0, intern_node))?;
	    
	    //let intern_node = await!(read_internal_node(handle.clone(), 0))?;
	    //intern_node.print_keys();
	    
	    Ok::<(),failure::Error>(())
	})
}


pub fn read_tree(handle: Handle) -> impl Future<Item=(), Error=failure::Error> {
	async_block!{
	    let intern_node = await!(read_internal_node(handle.clone(), 0))?;
	    
	    
	    Ok::<(),failure::Error>(())
	}
}







































struct Key {
	object_id: u64,
	kind: u8,
	offset: u64
}

struct Item {
	key: Key,
	offset: u32,
	size: u32
}

// fixed size
// checksum
// flags
// FS IDs
// gen number
struct BlockHeader {

}

// Internal tree node: [key, blockpointer] pairs
// 
// Leaf nodes: [item, data] pairs
// ->  [[blockHeader][Item][Item].......[data][data]]
