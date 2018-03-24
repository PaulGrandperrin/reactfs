extern crate fuzztest;
use fuzztest::*;

#[test]
#[ignore]
fn check_fuzz_cow_btree_remove_pop_head_leaf() {
    check_target_with_marker("hfuzz-btree", "cow_btree_remove_pop_head_leaf");
}

#[test]
#[ignore]
fn check_fuzz_cow_btree_remove_pop_head_internal() {
    check_target_with_marker("hfuzz-btree", "cow_btree_remove_pop_head_internal");
}

#[test]
#[ignore]
fn check_fuzz_cow_btree_remove_full_merge_leaf() {
    check_target_with_marker("hfuzz-btree", "cow_btree_remove_full_merge_leaf");
}

#[test]
#[ignore]
fn check_fuzz_cow_btree_remove_full_merge_internal() {
    check_target_with_marker("hfuzz-btree", "cow_btree_remove_full_merge_internal");
}

#[test]
#[ignore]
fn check_fuzz_cow_btree_remove_partial_merge_left_leaf() {
    check_target_with_marker("hfuzz-btree", "cow_btree_remove_partial_merge_left_leaf");
}

#[test]
#[ignore]
fn check_fuzz_cow_btree_remove_partial_merge_right_leaf() {
    check_target_with_marker("hfuzz-btree", "cow_btree_remove_partial_merge_right_leaf");
}

#[test]
#[ignore]
fn check_fuzz_cow_btree_remove_partial_merge_left_internal() {
    check_target_with_marker("hfuzz-btree", "cow_btree_remove_partial_merge_left_internal");
}

#[test]
#[ignore]
fn check_fuzz_cow_btree_remove_partial_merge_right_internal() {
    check_target_with_marker("hfuzz-btree", "cow_btree_remove_partial_merge_right_internal");
}

#[test]
#[ignore]
#[should_panic]
fn check_fuzz_invalid_marker() {
    check_target_with_marker("hfuzz-btree", "invalid");
}

#[test]
#[ignore]
#[should_panic]
fn check_fuzz_invalid_target() {
    check_target_with_marker("invalid", "dont_care");
}