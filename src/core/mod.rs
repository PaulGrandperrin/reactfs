use std::mem;
use std::fmt;
use std::io::Write;

use failure;
use futures::future;
use reactor::*;
use byteorder;
use byteorder::{ByteOrder};
use futures::prelude::*;
use itertools::Itertools;

#[cfg(test)]
mod tests;

const MAGIC_NUMBER: &[u8] = b"ReactFS0";
const BLOCK_SIZE: usize = 4096;

struct Uberblock {
    tgx: u64,
    tree_root_offset: u64,
    free_space_offset: u64
}

impl fmt::Debug for Uberblock {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Uberblock {{ tgx: {}, tree_root_offset: {}, free_space_offset: {} }}", self.tgx, self.tree_root_offset, self.free_space_offset)
    }
}

impl Uberblock {

    fn new(tgx: u64, tree_root_offset: u64, free_space_offset: u64) -> Uberblock {
        Uberblock {
            tgx,
            tree_root_offset,
            free_space_offset
        }
    }

    fn from_mem(mem: &[u8]) -> Result<Uberblock, failure::Error> {
        debug_assert!(mem.len() >= 32);
        if &mem[0..8] != MAGIC_NUMBER {
            return Err(format_err!("Incorrect magic number. found: {:?}, expected: {:?}", &mem[0..8], MAGIC_NUMBER));
        }
        let tgx = byteorder::LittleEndian::read_u64(&mem[8..16]);
        let tree_root_offset = byteorder::LittleEndian::read_u64(&mem[16..24]);
        let free_space_offset = byteorder::LittleEndian::read_u64(&mem[24..32]);

        Ok(
            Uberblock {
                tgx,
                tree_root_offset,
                free_space_offset,
            }
        )
    }

    fn write_in_mem(&self, mem: &mut [u8]) {
        debug_assert!(mem.len() >= 32);
        (&mut mem[0..]).write(MAGIC_NUMBER).unwrap();
        byteorder::LittleEndian::write_u64(&mut mem[8..], self.tgx);
        byteorder::LittleEndian::write_u64(&mut mem[16..], self.tree_root_offset);
        byteorder::LittleEndian::write_u64(&mut mem[24..], self.free_space_offset);
    }

    fn to_mem(&self) -> Box<[u8]> {
        let mut mem: Box<[u8;32]> = Box::new(unsafe{mem::uninitialized()});
        self.write_in_mem(&mut *mem);
        return mem;
    }
}

#[async]
fn format(handle: Handle) -> Result<(), failure::Error> {
    let writes: Vec<_> = (0..10)
        .map(|i| {
            let s: Box<[u8]> = Uberblock::new(i, 10, 11).to_mem();
            handle.write(s.into_vec(), i*BLOCK_SIZE as u64)
        })
        .collect();

    await!(future::join_all(writes))?;
    Ok(())
}

#[async]
fn find_latest_uberblock(handle: Handle) -> Result<Uberblock, failure::Error> {
    let uberblocks = await!(handle.read(0, BLOCK_SIZE as u64 *10))?;
    let uberblock = uberblocks.chunks(BLOCK_SIZE)
        .map(|chunk| {
            Uberblock::from_mem(chunk)
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
            Uberblock::from_mem(chunk).map(|u|{
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