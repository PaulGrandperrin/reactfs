use super::*;

impl ObjectPointer {
    pub fn new(offset: u64, len: u64, object_type: ObjectType) -> ObjectPointer {
        ObjectPointer {
            offset,
            len,
            object_type,
        }
    }

    pub fn from_bytes(bytes: &mut Cursor<&[u8]>) -> Result<ObjectPointer, failure::Error> {
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

    pub fn to_bytes(&self, bytes: &mut Cursor<&mut [u8]>) {
        assert!(bytes.remaining_mut() >= 8 + 8 + 1);
        
        bytes.put_u64::<LittleEndian>(self.offset);
        bytes.put_u64::<LittleEndian>(self.len);
        bytes.put_u8(self.object_type.to_u8().unwrap()); // there is less than 2^8 types
    }

    pub fn async_read_object<K: Serializable + Ord + Copy, V: Serializable, B: ConstUsize>(&self, handle: Handle) -> impl Future<Item=AnyObject<K, V, B>, Error=failure::Error> {
        let object_type = self.object_type.clone();

        handle.read(self.offset, self.len).and_then(move |mem|{
            match object_type {
                ObjectType::LeafNode => {
                    Ok(AnyObject::LeafNode(Box::new(
                        Node::<K, V, B, Leaf>::from_bytes(&mut Cursor::new(&mem))?
                    )))
                }
                ObjectType::InternalNode => {
                    Ok(AnyObject::InternalNode(Box::new(
                        Node::<K, ObjectPointer, B, Internal>::from_bytes(&mut Cursor::new(&mem))?
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