#![no_main]
#[macro_use] extern crate libfuzzer_sys;
extern crate reactfs;

use reactfs::core::instrumentation::*;

fuzz_target!(|data: &[u8]| {
   fuzz_btree(data);
});
