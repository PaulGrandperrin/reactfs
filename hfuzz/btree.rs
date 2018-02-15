extern crate reactfs;
#[macro_use] extern crate honggfuzz;

#[cfg(all(fuzzing))]
use reactfs::core::instrumentation::*;

fn main() {
    loop {
        fuzz!(|data: &[u8]| {
        	// we don't want to explode the search space
    		if data.len() > 100 * (8 + 8) {return}

    		let vec = raw_to_vec_of_tuple_u64(data);

            insert_checked(vec);
        });
    }
}
