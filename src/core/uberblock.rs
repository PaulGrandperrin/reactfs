use super::*;

impl Uberblock {

    pub fn new(tgx: u64, tree_root_pointer: ObjectPointer, free_space_offset: u64) -> Uberblock {
        Uberblock {
            tgx,
            free_space_offset,
            tree_root_pointer,
        }
    }

    pub fn from_bytes(bytes: &mut Cursor<&[u8]>) -> Result<Uberblock, failure::Error> {
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

    pub fn to_bytes(&self, bytes: &mut Cursor<&mut [u8]>) {
        assert!(bytes.remaining_mut() >= 8 + 8 + 8);
        
        bytes.put_slice(MAGIC_NUMBER);
        bytes.put_u64::<LittleEndian>(self.tgx);
        bytes.put_u64::<LittleEndian>(self.free_space_offset);
        self.tree_root_pointer.to_bytes(bytes);
    }

    pub fn to_mem(&self) -> Box<[u8]> {
        let mut mem: Box<[u8;41]> = Box::new(unsafe{mem::uninitialized()});
        self.to_bytes(&mut Cursor::new(&mut *mem));
        return mem;
    }

    pub fn async_write_at(&self, handle: Handle, offset: u64) -> impl Future<Item=u64, Error=failure::Error> {
        handle.write(self.to_mem().to_vec(), offset)
    }
}

#[async]
pub fn find_latest_uberblock(handle: Handle) -> Result<Uberblock, failure::Error> {
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
pub fn write_new_uberblock(handle: Handle, uberblock: Uberblock) -> Result<(), failure::Error> {
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
