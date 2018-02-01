use futures::prelude::*;
use std::thread;
use std::sync::mpsc::channel;
use ::*;
use super::*;
use ::backend::mem::*;

proptest! {
    #[test]
    fn format_read_and_write_uberblock(n in 0usize..20) {
        let (bd_sender, bd_receiver) = channel::<BDRequest>();
        let (fs_sender, _fs_receiver) = channel::<FSResponse>();
        let (react_sender, react_receiver) = channel::<Event>();

        let react_sender_bd = react_sender.clone();
        let _bd_thread = thread::spawn(move || {
            mem_backend_loop(react_sender_bd, bd_receiver, 4096 * 1000);
        });

        let mut core = Core::new(bd_sender, fs_sender, react_receiver);
        let handle = core.handle();

        let f = format_read_and_write_uberblock_async(handle.clone(), n);

        println!("starting reactor");
        let r = core.run(f);
        assert!(r.unwrap().tgx == 9 + n as u64);
    }

    #[test]
    fn cow_btree_increasing(n in 0usize..100) {
        let (bd_sender, bd_receiver) = channel::<BDRequest>();
        let (fs_sender, _fs_receiver) = channel::<FSResponse>();
        let (react_sender, react_receiver) = channel::<Event>();

        let react_sender_bd = react_sender.clone();
        let _bd_thread = thread::spawn(move || {
            mem_backend_loop(react_sender_bd, bd_receiver, 4096 * 1000);
        });

        let mut core = Core::new(bd_sender, fs_sender, react_receiver);
        let handle = core.handle();

        let f = cow_btree_increasing_async(handle.clone(), n);

        println!("starting reactor");
        let r = core.run(f);
        r.unwrap();
    }
}

#[test]
fn cow_btree_random() {
    let (bd_sender, bd_receiver) = channel::<BDRequest>();
    let (fs_sender, _fs_receiver) = channel::<FSResponse>();
    let (react_sender, react_receiver) = channel::<Event>();

    let react_sender_bd = react_sender.clone();
    let _bd_thread = thread::spawn(move || {
        mem_backend_loop(react_sender_bd, bd_receiver, 4096 * 1000);
    });

    let mut core = Core::new(bd_sender, fs_sender, react_receiver);
    let handle = core.handle();

    let f = cow_btree_random_async(handle.clone());

    println!("starting reactor");
    let r = core.run(f);
    r.unwrap();
}

#[async]
fn format_read_and_write_uberblock_async(handle: Handle, n: usize) -> Result<Uberblock, failure::Error> {
    await!(format(handle.clone()))?;

    for _ in 0..n {
        let mut u = await!(find_latest_uberblock(handle.clone()))?;
        u.tgx += 1;
        await!(write_new_uberblock(handle.clone(), u))?;
    }

    await!(find_latest_uberblock(handle.clone()))
}

#[async]
fn cow_btree_increasing_async(handle: Handle, n: usize) -> Result<(), failure::Error> {
    await!(format(handle.clone()))?;
    let uberblock = await!(find_latest_uberblock(handle.clone()))?;
    let (mut op, mut free_space_offset) = (uberblock.tree_root_pointer, uberblock.free_space_offset);

    for i in (0..n) {
        let res = await!(insert_in_btree(
            handle.clone(),
            op.clone(),
            free_space_offset,
            LeafNodeEntry{key: i as u64, value: 1000+i as u64}
            ))?;
        op = res.0;
        free_space_offset = res.1;
    }

    let res = await!(read_tree(handle.clone(), op.clone()))?;
    
    for i in res {
        assert!(i.key == i.value - 1000);
    }

    Ok(())
}

#[async]
fn cow_btree_random_async(handle: Handle) -> Result<(), failure::Error> {
    await!(format(handle.clone()))?;
    let uberblock = await!(find_latest_uberblock(handle.clone()))?;
    let (mut op, mut free_space_offset) = (uberblock.tree_root_pointer, uberblock.free_space_offset);

    // 0 to 999 shuffled
    let v:Vec<u64> = vec![
        295, 471, 303, 266, 981, 599, 979, 488, 219, 316, 666, 941, 350, 007, 617, 390, 178, 917, 881, 467,
        181, 270, 679, 127, 242, 285, 114, 375, 030, 520, 035, 564, 098, 261, 208, 031, 025, 109, 960, 280,
        421, 096, 704, 550, 358, 611, 227, 378, 524, 111, 554, 943, 429, 751, 426, 221, 572, 745, 502, 405,
        724, 536, 290, 928, 717, 436, 656, 248, 756, 693, 236, 585, 087, 855, 268, 999, 722, 188, 153, 677,
        560, 675, 657, 224, 404, 092, 164, 707, 809, 207, 843, 601, 783, 619, 841, 714, 410, 659, 446, 046,
        702, 957, 939, 468, 583, 244, 150, 924, 610, 020, 141, 660, 811, 641, 526, 056, 220, 846, 419, 321,
        059, 329, 461, 428, 310, 875, 102, 130, 091, 131, 764, 923, 320, 956, 618, 009, 865, 647, 101, 607,
        805, 706, 234, 938, 146, 148, 672, 237, 793, 169, 710, 203, 144, 649, 306, 177, 678, 151, 589, 670,
        460, 133, 612, 631, 944, 179, 716, 625, 006, 383, 232, 967, 185, 458, 897, 962, 491, 205, 945, 166,
        643, 505, 019, 616, 422, 539, 334, 729, 929, 856, 776, 260, 703, 259, 384, 576, 848, 691, 900, 871,
        252, 389, 143, 667, 005, 974, 532, 189, 204, 535, 869, 415, 125, 609, 727, 084, 190, 698, 281, 077,
        774, 769, 665, 157, 766, 269, 892, 602, 066, 978, 034, 828, 797, 728, 985, 940, 969, 937, 311, 045,
        299, 655, 365, 065, 435, 354, 905, 368, 773, 658, 644, 976, 202, 586, 958, 163, 654, 597, 029, 708,
        788, 332, 319, 711, 214, 682, 931, 348, 750, 137, 302, 694, 838, 024, 067, 226, 840, 195, 089, 578,
        827, 833, 173, 003, 676, 646, 930, 124, 054, 413, 473, 487, 562, 834, 336, 540, 043, 417, 191, 341,
        359, 047, 823, 072, 246, 921, 338, 140, 480, 071, 651, 346, 632, 927, 476, 175, 591, 475, 427, 444,
        568, 792, 987, 284, 229, 099, 914, 360, 543, 747, 847, 946, 567, 249, 835, 254, 692, 775, 057, 010,
        307, 123, 225, 126, 757, 482, 073, 212, 187, 028, 377, 325, 669, 494, 135, 795, 211, 552, 088, 142,
        859, 948, 149, 309, 760, 870, 241, 623, 218, 988, 791, 908, 890, 604, 650, 250, 826, 885, 916, 322,
        741, 407, 335, 159, 860, 821, 200, 998, 037, 277, 686, 090, 162, 086, 174, 459, 251, 557, 624, 081,
        434, 198, 023, 803, 400, 845, 964, 243, 438, 363, 147, 472, 511, 274, 264, 652, 595, 022, 154, 351,
        443, 731, 516, 911, 755, 997, 050, 640, 993, 517, 683, 228, 271, 324, 761, 015, 507, 420, 381, 862,
        786, 279, 815, 555, 762, 430, 355, 538, 463, 376, 036, 474, 594, 699, 531, 317, 965, 027, 932, 913,
        920, 397, 529, 286, 884, 782, 106, 498, 831, 504, 738, 743, 287, 781, 484, 097, 026, 016, 961, 754,
        886, 918, 830, 852, 518, 863, 085, 451, 331, 356, 230, 217, 315, 288, 896, 874, 816, 197, 638, 276,
        645, 399, 132, 742, 637, 380, 152, 739, 462, 829, 272, 158, 570, 080, 100, 579, 408, 534, 633, 485,
        888, 935, 720, 017, 565, 777, 898, 139, 508, 934, 873, 014, 790, 515, 915, 514, 369, 167, 732, 519,
        995, 902, 684, 004, 868, 314, 798, 592, 374, 367, 353, 122, 772, 483, 466, 705, 464, 955, 525, 479,
        161, 822, 723, 305, 062, 291, 371, 060, 257, 063, 345, 563, 110, 255, 661, 853, 301, 718, 265, 983,
        453, 882, 687, 455, 818, 910, 542, 813, 439, 500, 104, 968, 136, 379, 820, 263, 894, 103, 982, 447,
        055, 209, 587, 767, 577, 239, 052, 533, 182, 598, 749, 170, 975, 437, 506, 561, 545, 912, 342, 812,
        972, 887, 156, 493, 051, 449, 298, 608, 951, 403, 176, 966, 787, 642, 440, 746, 622, 262, 386, 842,
        503, 629, 904, 796, 041, 926, 323, 688, 297, 313, 223, 328, 222, 527, 801, 759, 990, 537, 366, 877,
        387, 971, 450, 481, 456, 748, 183, 442, 094, 970, 370, 752, 196, 497, 357, 636, 893, 193, 398, 785,
        807, 064, 674, 907, 867, 621, 880, 973, 825, 489, 042, 457, 337, 018, 117, 040, 401, 980, 495, 733,
        630, 780, 947, 441, 184, 569, 172, 231, 293, 530, 566, 613, 582, 996, 513, 953, 522, 889, 352, 872,
        876, 730, 409, 603, 395, 278, 806, 396, 861, 581, 778, 994, 735, 165, 627, 012, 878, 330, 701, 082,
        445, 394, 155, 273, 817, 575, 620, 879, 541, 312, 883, 199, 075, 373, 819, 523, 238, 606, 448, 734,
        186, 216, 626, 909, 712, 713, 989, 068, 418, 180, 078, 866, 593, 431, 800, 634, 709, 478, 120, 333,
        469, 771, 653, 083, 857, 385, 235, 412, 992, 721, 726, 891, 129, 574, 160, 119, 933, 201, 247, 033,
        851, 580, 546, 794, 008, 061, 588, 850, 113, 984, 549, 614, 509, 763, 049, 349, 327, 308, 922, 689,
        433, 470, 424, 116, 048, 765, 615, 256, 544, 547, 740, 808, 512, 736, 300, 115, 496, 240, 696, 079,
        326, 392, 245, 044, 138, 145, 362, 600, 673, 810, 528, 105, 571, 648, 364, 680, 039, 213, 919, 903,
        977, 406, 832, 844, 784, 836, 118, 391, 093, 663, 134, 664, 194, 954, 452, 799, 340, 267, 695, 258,
        949, 986, 058, 108, 292, 681, 901, 011, 112, 553, 744, 584, 814, 662, 753, 697, 294, 719, 725, 737,
        925, 432, 510, 070, 668, 411, 779, 556, 388, 770, 192, 490, 492, 804, 465, 361, 283, 991, 499, 715,
        477, 416, 414, 171, 942, 402, 168, 628, 858, 548, 347, 959, 393, 559, 573, 032, 700, 551, 635, 074,
        206, 128, 758, 906, 950, 899, 952, 558, 095, 021, 013, 789, 339, 002, 854, 069, 837, 382, 802, 233,
        343, 425, 344, 000, 685, 001, 318, 590, 864, 521, 304, 671, 596, 107, 501, 895, 038, 486, 639, 076,
        275, 690, 053, 282, 215, 963, 372, 253, 423, 121, 936, 605, 839, 210, 849, 296, 768, 454, 289, 824,
        ];

    for i in v {
        let res = await!(insert_in_btree(
            handle.clone(),
            op.clone(),
            free_space_offset,
            LeafNodeEntry{key: i as u64, value: 1000+i as u64}
            ))?;
        op = res.0;
        free_space_offset = res.1;
    }

    let res = await!(read_tree(handle.clone(), op.clone()))?;
    
    for i in 0..1000 {
        assert!(res[i].key == res[i].value - 1000);
        assert!(res[i].key == i as u64);
    }

    Ok(())
}