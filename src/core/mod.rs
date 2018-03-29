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

mod object_pointer;
mod uberblock;
mod cow_btree;
mod util;

#[cfg(any(feature="instrumentation", test, fuzzing))]
pub mod instrumentation;

#[cfg(test)]
mod tests;

const MAGIC_NUMBER: &[u8] = b"ReactFS0";
const BLOCK_SIZE: usize = 4096;
const BTREE_B: usize = 2;
const BTREE_DEGREE: usize = BTREE_B*2+1;
const BTREE_SPLIT: usize = BTREE_B+1;

#[derive(Debug)]
pub struct Uberblock {
    tgx: u64,
    free_space_offset: u64,
    tree_root_pointer: ObjectPointer,
}

#[derive(Debug, Clone, Primitive)]
pub enum ObjectType {
    InternalNode = 0,
    LeafNode = 1
}

#[derive(Debug)]
pub enum AnyObject {
    LeafNode(Box<LeafNode>),
    InternalNode(Box<InternalNode>),
}

#[derive(Debug, Clone)]
pub struct ObjectPointer {
    offset: u64,
    len: u64,
    object_type: ObjectType
    // checksum
}

#[derive(Debug)]
pub struct NodeEntry<K, V> {
    key: K,
    value: V
}

impl<K, V> NodeEntry<K, V> {
    fn new(key: K, value: V) -> Self {
        Self {key, value}
    }
}

#[derive(Debug)]
pub struct Node<T> {
    entries: Vec<T>,
}

pub type LeafNodeEntry = NodeEntry<u64, u64>;
pub type InternalNodeEntry = NodeEntry<u64, ObjectPointer>;

pub type InternalNode = Node<InternalNodeEntry>;
pub type LeafNode = Node<LeafNodeEntry>;
