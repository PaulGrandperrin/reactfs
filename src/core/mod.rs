use std::mem;
use std::fmt;
use std::io::Write;
use std::marker::PhantomData;

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

// serialization trait

pub trait Serializable: Sized {
    fn to_bytes(&self, bytes: &mut Cursor<&mut [u8]>);
    fn from_bytes(bytes: &mut Cursor<&[u8]>) -> Result<Self, failure::Error>;
}

// poor man's const generic

pub trait ConstUsize {
    const USIZE: usize;
}

#[derive(Debug)]
pub struct ConstUsize2;
impl ConstUsize for ConstUsize2 {
    const USIZE: usize = 2;
}

#[derive(Debug)]
pub struct ConstUsize3;
impl ConstUsize for ConstUsize3 {
    const USIZE: usize = 3;
}

// generic btree types

#[derive(Debug)]
pub struct NodeEntry<K, V> {
    key: K,
    value: V
}

#[derive(Debug)]
pub struct Node<K, V, B: ConstUsize> {
    entries: Vec<NodeEntry<K, V>>,
    b: PhantomData<B>,
}

// concrete btree types

pub type LeafNodeEntry = NodeEntry<u64, u64>;
pub type InternalNodeEntry = NodeEntry<u64, ObjectPointer>;

pub type LeafNode = Node<u64, u64, ConstUsize2>;
pub type InternalNode = Node<u64, ObjectPointer, ConstUsize2>;

// Serializable implementations

impl Serializable for u64 {
    fn to_bytes(&self, bytes: &mut Cursor<&mut [u8]>) {
        unimplemented!()
    }
    fn from_bytes(bytes: &mut Cursor<&[u8]>) -> Result<Self, failure::Error> {
        unimplemented!()
    }
}

impl Serializable for ObjectPointer {
    fn to_bytes(&self, bytes: &mut Cursor<&mut [u8]>) {
        unimplemented!()
    }
    fn from_bytes(bytes: &mut Cursor<&[u8]>) -> Result<Self, failure::Error> {
        unimplemented!()
    }
}