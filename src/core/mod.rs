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

const fn btree_degree(b: usize) -> usize {b * 2 + 1}
const fn btree_split(b: usize) -> usize {b + 1}

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
pub enum AnyObject<K: Serializable + Ord + Copy, V: Serializable, B: ConstUsize> {
    LeafNode(Box<Node<K, V, B, Leaf>>),
    InternalNode(Box<Node<K, ObjectPointer, B, Internal>>),
}

#[derive(Debug, Clone)]
pub struct ObjectPointer {
    offset: u64,
    len: u64,
    object_type: ObjectType
    // checksum
}

// traits

trait Index {

}

trait NodeTrait<K: Serializable + Ord + Copy, V: Serializable> {
    fn insert(&mut self, entry: NodeEntry<K, V>) -> Option<V>;
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

pub trait ConstObjectType {
    const OTYPE: ObjectType;
}

#[derive(Debug)]
pub struct Leaf;
impl ConstObjectType for Leaf {
    const OTYPE: ObjectType = ObjectType::LeafNode;
}

#[derive(Debug)]
pub struct Internal;
impl ConstObjectType for Internal {
    const OTYPE: ObjectType = ObjectType::InternalNode;
}

// generic btree types

#[derive(Debug)]
pub struct NodeEntry<K: Serializable + Ord + Copy, V: Serializable> {
    key: K,
    value: V,
}

#[derive(Debug)]
pub struct Node<K: Serializable + Ord + Copy, V: Serializable, B: ConstUsize, T: ConstObjectType> {
    entries: Vec<NodeEntry<K, V>>,
    _b: PhantomData<B>,
    _t: PhantomData<T>,
}

// concrete btree types

pub type LeafNodeEntry = NodeEntry<u64, u64>;
pub type InternalNodeEntry = NodeEntry<u64, ObjectPointer>;

pub type LeafNode = Node<u64, u64, ConstUsize2, Leaf>;
pub type InternalNode = Node<u64, ObjectPointer, ConstUsize2, Internal>;

// serialization trait

pub trait Serializable: Sized {
    const SIZE: usize;

    fn to_bytes(&self, bytes: &mut Cursor<&mut [u8]>);
    fn from_bytes(bytes: &mut Cursor<&[u8]>) -> Result<Self, failure::Error>;
}

impl Serializable for u64 {
    const SIZE: usize = 8;

    fn to_bytes(&self, bytes: &mut Cursor<&mut [u8]>) {
        bytes.put_u64::<LittleEndian>(*self);
    }
    fn from_bytes(bytes: &mut Cursor<&[u8]>) -> Result<Self, failure::Error> {
        Ok(bytes.get_u64::<LittleEndian>())
    }
}

impl Serializable for ObjectPointer {
    const SIZE: usize = (8 + 8 + 1);

    fn to_bytes(&self, bytes: &mut Cursor<&mut [u8]>) {
        self.to_bytes(bytes);
    }
    fn from_bytes(bytes: &mut Cursor<&[u8]>) -> Result<Self, failure::Error> {
        ObjectPointer::from_bytes(bytes)
    }
}