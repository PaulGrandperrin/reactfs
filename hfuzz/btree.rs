extern crate reactfs;
#[macro_use] extern crate honggfuzz;

use reactfs::core::instrumentation::*;

fn main() {
    loop {
        fuzz!(|data: &[u8]| {
            let vec = match raw_to_vec_of_operation(data) {
                Some(v) => v,
                None => return
            };

            // we don't want to explode the search space
            if vec.len() > 1000 {return}

            insert_and_remove_checked(vec);
        });
    }
}
