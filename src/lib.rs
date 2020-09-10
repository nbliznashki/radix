mod bitmap;
mod bucketcolumn;
mod column;
mod columnflatten;
mod columnpartition;
mod columnrepartition;
mod columnu8;
mod expressions;
mod hashcolumn;
mod hashjoin;
mod helpers;

pub use bucketcolumn::*;
pub use column::*;
pub use columnflatten::*;
pub use columnpartition::*;
pub use columnrepartition::*;
pub use columnu8::*;
pub use expressions::*;
pub use hashjoin::*;
//TO-DO: Make library safe (e.g.) safe rust code outside of it can't cause UB

#[cfg(test)]
mod tests {
    use crate::bitmap::*;
    use crate::bucketcolumn::*;
    use crate::columnu8::*;
    //use crate::expressions::operations::init_dict;
    use crate::hashcolumn::*;
    use crate::*;
    use std::{any::Any, collections::HashMap, ops::DerefMut, sync::Arc};

    use std::collections::hash_map::RandomState;
    use std::rc::*;

    use rayon::prelude::*;

    //Validate that the function behaves the same way when given an index and when not given an index
    #[test]
    fn works_for_rc() {
        struct Dummy {
            col: Rc<Vec<usize>>,
        }

        impl ColumnPartition<Vec<usize>, usize> for Dummy {
            fn get_col(&self) -> &Vec<usize> {
                &self.col
            }
            fn get_col_mut(&mut self) -> &mut Vec<usize> {
                Rc::get_mut(&mut self.col).unwrap()
            }
        }
    }
    #[test]
    fn compare_hash_with_and_without_index() {
        let s = RandomState::new();
        let v: Vec<usize> = vec![1, 1, 2, 3];

        let r1 = v.hash_column(&None, &None, &s);
        let index: Vec<usize> = vec![0, 1, 2, 3];
        let r2 = v.hash_column(&Some(index), &None, &s);
        assert_eq!(r1.data, r2.data);
    }

    //Validate that the function behaves the same way when given an index and when not given an index
    #[test]
    fn compare_hash_append_with_and_without_index() {
        let s = RandomState::new();
        let v: Vec<usize> = vec![1, 1, 2, 3];

        let mut r1 = v.hash_column(&None, &None, &s);
        v.hash_column_append(&None, &None, &s, &mut r1);

        let _index: Vec<Option<usize>> = vec![Some(0), Some(1), Some(2), Some(3)];
        let mut r2 = v.hash_column(&None, &None, &s);
        v.hash_column_append(&None, &None, &s, &mut r2);
        assert_eq!(r1, r2);
    }
    //Validate that the function behaves the same way when given an index and when not given an index
    #[test]
    fn compare_hash_with_null() {
        let s = RandomState::new();
        let v: Vec<usize> = vec![1, 1, 2, 3];

        let mut r1 = v.hash_column(&None, &None, &s);
        v.hash_column_append(&None, &None, &s, &mut r1);

        let bitmap = Some(Bitmap {
            bits: vec![1, 1, 0, 1],
        });

        let mut r2 = v.hash_column(&None, &None, &s);

        r2.bitmap = bitmap;
        v.hash_column_append(&None, &None, &s, &mut r2);
        assert_eq!(r1.data[0], r2.data[0]);
        assert_eq!(r1.data[1], r2.data[1]);
        assert_eq!(r1.data[2], r2.data[2]);
        assert_eq!(r1.data[3], r2.data[3]);

        assert!(r1.bitmap.is_none());
        assert!(r2.bitmap.is_some());
        assert_eq!(r2.bitmap.unwrap().bits, vec![1, 1, 0, 1]);
    }

    //Validate that the function behaves the same way when given an index and when not given an index
    #[test]
    fn first_bit_hash_add_with_null_equal() {
        let s = RandomState::new();
        let v1: Vec<usize> = vec![1, 2, 3, 4, 5];
        let v2: Vec<usize> = vec![5, 4, 3, 2, 1];

        let bitmap = Some(Bitmap {
            bits: vec![1, 1, 0, 1, 1],
        });

        let index: Vec<usize> = vec![0, 1, 2, 3, 4];
        let index = Some(index);
        let mut r1 = v1.hash_column(&index, &bitmap, &s);
        v2.hash_column_append(&None, &None, &s, &mut r1);

        let mut r2 = v1.hash_column(&None, &None, &s);
        v2.hash_column_append(&index, &bitmap, &s, &mut r2);

        assert_eq!(r1.data, r2.data);
    }

    #[test]
    fn first_bit_hash_add_with_null_diff() {
        let s = RandomState::new();
        let v1: Vec<usize> = vec![1, 2, 3, 4, 5];
        let v2: Vec<usize> = vec![5, 4, 3, 2, 1];

        let bitmap = Some(Bitmap {
            bits: vec![1, 1, 1, 0, 1],
        });

        let index: Vec<usize> = vec![0, 1, 2, 3, 4];
        let index = Some(index);
        let mut r1 = v1.hash_column(&index, &bitmap, &s);
        v2.hash_column_append(&None, &None, &s, &mut r1);

        let mut r2 = v1.hash_column(&None, &None, &s);
        v2.hash_column_append(&index, &bitmap, &s, &mut r2);

        assert_eq!(r1.data[0], r2.data[0]);
        assert_eq!(r1.data[1], r2.data[1]);
        assert_eq!(r1.data[2], r2.data[2]);
        assert!(r1.data[3] != r2.data[3]);
        assert_eq!(r1.data[4], r2.data[4]);
    }

    //Should not be possible to add a column of different length
    #[test]
    #[should_panic]
    fn different_len_hash_append() {
        let s = RandomState::new();
        let v1: Vec<usize> = vec![1, 1, 2, 3, 5];
        let v2: Vec<usize> = vec![1, 1, 2, 3];

        let index: Vec<usize> = vec![0, 1, 2, 3, 4];
        let mut r1 = v1.hash_column(&Some(index), &None, &s);
        v2.hash_column_append(&None, &None, &s, &mut r1);
    }

    #[test]
    fn bucket_init() {
        let hash: HashColumn = HashColumn {
            data: vec![1, 2, 4, 6, 8, 10],
            bitmap: Some(Bitmap {
                bits: vec![0, 1, 1, 1, 1, 1],
            }),
        };
        let b = BucketColumn::from_hash(hash, 2);
        println!("{:?}", &b.data);
        assert_eq!(*b, vec![1, 2, 0, 2, 0, 2]);
    }

    #[test]
    #[should_panic]
    fn bucket_init_overflow() {
        let hash: HashColumn = HashColumn {
            data: vec![1, 2, 4, 6, 8, 10],
            bitmap: None,
        };
        let _b = BucketColumn::from_hash(hash, 63);
    }

    #[test]
    fn bucket_map_init() {
        let hash: HashColumn = HashColumn {
            data: vec![1, 2, 4, 6, 8, 10],
            bitmap: None,
        };
        let b = BucketColumn::from_hash(hash, 2);
        assert_eq!(*b, vec![1, 2, 0, 2, 0, 2]);
        let bmap = BucketsSizeMap::from_bucket_column(b, 2);
        assert_eq!(*bmap, vec![vec![1, 1, 1, 0], vec![1, 0, 2, 0]]);
        //println!("{:?}", *bmap)
    }

    #[test]
    fn bucket_map_init_serial() {
        let hash: HashColumn = HashColumn {
            data: vec![1, 2, 4, 6, 8, 10],
            bitmap: None,
        };
        let b = BucketColumn::from_hash(hash, 2);
        assert_eq!(*b, vec![1, 2, 0, 2, 0, 2]);
        let bmap = BucketsSizeMap::from_bucket_column(b, 1);
        assert_eq!(*bmap, vec![vec![2, 1, 3, 0]]);
        //println!("{:?}", *bmap)
    }

    #[test]
    fn bucket_size_map_init() {
        let hash: HashColumn = HashColumn {
            data: vec![1, 2, 4, 6, 8, 10],
            bitmap: None,
        };
        let b = BucketColumn::from_hash(hash, 2);
        assert_eq!(*b, vec![1, 2, 0, 2, 0, 2]);
        let bmap = BucketsSizeMap::from_bucket_column(b, 2);
        assert_eq!(*bmap, vec![vec![1, 1, 1, 0], vec![1, 0, 2, 0]]);
        assert_eq!(bmap.offsets, vec![vec![0, 0, 0, 0], vec![1, 1, 1, 0]]);
        assert_eq!(bmap.bucket_sizes, vec![2, 1, 3, 0]);
        //println!("{:?}", *bmap)
    }
    #[test]
    fn partition_column_test_a() {
        let hash: HashColumn = HashColumn {
            data: vec![1, 2, 4, 6, 8, 10],
            bitmap: None,
        };
        let data = vec![1, 2, 4, 6, 8, 10];
        let b = BucketColumn::from_hash(hash, 2);
        assert_eq!(*b, vec![1, 2, 0, 2, 0, 2]);
        let bmap = BucketsSizeMap::from_bucket_column(b, 2);

        let part = data.partition_column(&None, &None, &bmap);
        assert_eq!(
            part,
            PartitionedColumn::FixedLenType(
                vec![vec![4, 8], vec![1], vec![2, 6, 10], vec![]],
                vec![None, None, None, None],
                vec![None, None, None, None]
            )
        );
    }

    #[test]
    fn partition_column_test_string_a() {
        let hash: HashColumn = HashColumn {
            data: vec![1, 1, 2, 3, 4, 5],
            bitmap: Some(Bitmap {
                bits: vec![0, 1, 1, 1, 1, 1],
            }),
        };
        let strvec: Vec<String> = vec![
            "aa".to_string(),
            "bb".to_string(),
            "cc".to_string(),
            "dd".to_string(),
            "ee".to_string(),
            "fff".to_string(),
        ];
        let strvec = StringVec { strvec };
        let b = BucketColumn::from_hash(hash, 2);

        let bmap = BucketsSizeMap::from_bucket_column(b, 2);
        let bitmap = Some(Bitmap {
            bits: vec![0, 1, 1, 1, 1, 1],
        });
        let part = strvec.partition_column(&None, &bitmap, &bmap);

        let expected_result = PartitionedColumn::<String>::VariableLenType(
            vec![
                ColumnU8 {
                    data: vec![101, 101],
                    start_pos: vec![0],
                    len: vec![2],
                },
                ColumnU8 {
                    data: vec![97, 97, 98, 98, 102, 102, 102],
                    start_pos: vec![0, 2, 4],
                    len: vec![2, 2, 3],
                },
                ColumnU8 {
                    data: vec![99, 99],
                    start_pos: vec![0],
                    len: vec![2],
                },
                ColumnU8 {
                    data: vec![100, 100],
                    start_pos: vec![0],
                    len: vec![2],
                },
            ],
            vec![None, None, None, None],
            vec![
                Some(Bitmap { bits: vec![1] }),
                Some(Bitmap {
                    bits: vec![0, 1, 1],
                }),
                Some(Bitmap { bits: vec![1] }),
                Some(Bitmap { bits: vec![1] }),
            ],
        );
        assert_eq!(part, expected_result);
    }

    #[test]
    fn repartition_column_test_a() {
        let data: Vec<u64> = vec![1, 2, 4, 6, 8, 10];
        let hash: HashColumn = HashColumn {
            data: vec![4, 1, 1, 1, 1, 3],
            bitmap: Some(Bitmap {
                bits: vec![1, 1, 1, 1, 1, 1],
            }),
        };

        let b = BucketColumn::from_hash(hash, 2);
        let bmap = BucketsSizeMap::from_bucket_column(b, 2);
        let part = data.partition_column(&None, &None, &bmap);

        let hash = HashColumn { data, bitmap: None };

        let hash = hash.data.partition_column(&None, &None, &bmap);

        let hash = if let PartitionedColumn::FixedLenType(hash_inner, index, bitmap) = hash {
            hash_inner
        } else {
            panic!()
        };

        assert_eq!(hash, vec![vec![1], vec![2, 4, 6, 8], vec![], vec![10]]);

        let index = hash.iter().map(|_| None).collect();
        let bitmap = hash.iter().map(|_| None).collect();
        let hash: HashColumnPartitioned = HashColumnPartitioned {
            data: hash,
            index,
            bitmap,
        };

        let b = BucketColumnPartitioned::from_hash(hash, 2);

        let mut bmap = BucketsSizeMapPartitioned::from_bucket_column(b);

        let new_index = bmap
            .bucket_column
            .iter()
            .enumerate()
            .map(|(i, v)| {
                if i & 2 == 0 {
                    Some((0..v.len()).collect())
                } else {
                    None
                }
            })
            .collect();

        bmap.hash.index = new_index;

        let bitmap = vec![
            None,
            Some(Bitmap {
                bits: vec![1, 1, 0, 1],
            }),
            None,
            None,
        ];

        let part = match part {
            PartitionedColumn::FixedLenType(column_data, index, _) => {
                PartitionedColumn::FixedLenType(column_data, index, bitmap)
            }
            _ => panic!(),
        };

        println!("{:?}", part);

        let part = part.partition_column(&bmap);
        println!("{:?}", part);

        //assert_eq!(hash, vec![vec![1], vec![2, 4, 6, 8], vec![], vec![10]]);
        //FixedLenType([[1], [2, 4, 6, 8], [], [10]], [None, None, None, None], [None, None, None, None])
        assert_eq!(
            part,
            PartitionedColumn::FixedLenType(
                vec![vec![4, 8], vec![1], vec![2, 6, 10], vec![]],
                vec![None, None, None, None],
                vec![
                    Some(Bitmap { bits: vec![1, 1] }),
                    Some(Bitmap { bits: vec![1] }),
                    Some(Bitmap {
                        bits: vec![1, 0, 1]
                    }),
                    Some(Bitmap { bits: vec![] }),
                ]
            )
        );
    }
    #[test]
    fn repartition_column_test_string() {
        let strvec: Vec<String> = vec![
            "aa".to_string(),
            "bb".to_string(),
            "cc".to_string(),
            "dd".to_string(),
            "ee".to_string(),
            "fff".to_string(),
        ];
        let strvec = StringVec { strvec };

        let hash: HashColumn = HashColumn {
            data: vec![4, 1, 1, 1, 1, 3],
            bitmap: Some(Bitmap {
                bits: vec![1, 1, 1, 1, 1, 1],
            }),
        };

        let b = BucketColumn::from_hash(hash, 2);
        let bmap = BucketsSizeMap::from_bucket_column(b, 2);
        let part = strvec.partition_column(&None, &None, &bmap);

        let data: Vec<u64> = vec![1, 2, 4, 6, 8, 10];
        let hash = HashColumn { data, bitmap: None };

        let hash = hash.data.partition_column(&None, &None, &bmap);
        let hash = if let PartitionedColumn::FixedLenType(hash_inner, index, bitmap) = hash {
            hash_inner
        } else {
            panic!()
        };

        let index = hash.iter().map(|_| None).collect();
        let bitmap = hash.iter().map(|_| None).collect();
        let hash: HashColumnPartitioned = HashColumnPartitioned {
            data: hash,
            index,
            bitmap,
        };

        let b = BucketColumnPartitioned::from_hash(hash, 2);
        let mut bmap = BucketsSizeMapPartitioned::from_bucket_column(b);

        let part = if let PartitionedColumn::VariableLenType(part_inner, index, bitmap) = part {
            part_inner
        } else {
            panic!()
        };

        let new_index = bmap
            .bucket_column
            .iter()
            .enumerate()
            .map(|(i, v)| {
                if i & 2 == 0 {
                    Some((0..v.len()).collect())
                } else {
                    None
                }
            })
            .collect();

        bmap.hash.index = new_index;
        let bitmap = part.par_iter().map(|_| None).collect();
        let index = part.par_iter().map(|_| None).collect();
        let part: PartitionedColumn<String> =
            PartitionedColumn::VariableLenType(part, index, bitmap);

        let bitmap = vec![
            None,
            Some(Bitmap {
                bits: vec![1, 1, 0, 1],
            }),
            None,
            None,
        ];

        let part = match part {
            PartitionedColumn::VariableLenType(columnu8_data, index, _) => {
                PartitionedColumn::VariableLenType(columnu8_data, index, bitmap)
            }
            _ => panic!(),
        };

        let part = part.partition_column(&bmap);

        let expected_result = PartitionedColumn::<String>::VariableLenType(
            vec![
                ColumnU8 {
                    data: vec![99, 99, 101, 101],
                    start_pos: vec![0, 2],
                    len: vec![2, 2],
                },
                ColumnU8 {
                    data: vec![97, 97],
                    start_pos: vec![0],
                    len: vec![2],
                },
                ColumnU8 {
                    data: vec![98, 98, 100, 100, 102, 102, 102],
                    start_pos: vec![0, 2, 4],
                    len: vec![2, 2, 3],
                },
                ColumnU8 {
                    data: vec![],
                    start_pos: vec![],
                    len: vec![],
                },
            ],
            vec![None, None, None, None],
            vec![
                Some(Bitmap { bits: vec![1, 1] }),
                Some(Bitmap { bits: vec![1] }),
                Some(Bitmap {
                    bits: vec![1, 0, 1],
                }),
                Some(Bitmap { bits: vec![] }),
            ],
        );
        assert_eq!(part, expected_result);
    }

    #[test]
    fn index_flatten() {
        let strvec: Vec<String> = vec![
            "aa".to_string(),
            "bb".to_string(),
            "cc".to_string(),
            "dd".to_string(),
            "ee".to_string(),
            "fff".to_string(),
        ];
        let strvec = StringVec { strvec };

        let hash: HashColumn = HashColumn {
            data: vec![4, 1, 1, 1, 1, 3],
            bitmap: Some(Bitmap {
                bits: vec![1, 1, 1, 1, 1, 1],
            }),
        };

        let b = BucketColumn::from_hash(hash, 2);
        let bmap = BucketsSizeMap::from_bucket_column(b, 2);

        //This gives column with elements per chunk 1 4 0 1
        let part = strvec.partition_column(&None, &None, &bmap);

        let index = vec![None, Some(vec![0, 0]), None, Some(vec![0])];

        let flattened_index = part.flatten_index(&index);

        assert_eq!(flattened_index.index_flattened, Some(vec![0, 1, 1, 2]),);
    }

    #[test]
    fn column_flatten_fixedlen() {
        let data: Vec<u64> = vec![1, 2, 3, 4, 5, 6];

        let hash: HashColumn = HashColumn {
            data: vec![4, 1, 1, 1, 1, 3],
            bitmap: Some(Bitmap {
                bits: vec![1, 1, 1, 1, 1, 1],
            }),
        };

        let b = BucketColumn::from_hash(hash, 2);
        let bmap = BucketsSizeMap::from_bucket_column(b, 2);

        //This gives column with elements per chunk 1 4 0 1
        let part = data.partition_column(&None, &None, &bmap);

        let index = vec![None, Some(vec![0, 0]), None, Some(vec![0])];

        let flattened_index = part.flatten_index(&index);

        let flattened_column = part.flatten(&flattened_index);

        assert_eq!(
            flattened_column,
            FlattenedColumn::FixedLenType(vec![1, 2, 6], None)
        );
    }

    #[test]
    fn column_flatten_variable() {
        let strvec: Vec<String> = vec![
            "aa".to_string(),
            "bbb".to_string(),
            "cc".to_string(),
            "dd".to_string(),
            "ee".to_string(),
            "ff".to_string(),
        ];
        let strvec = StringVec { strvec };

        let hash: HashColumn = HashColumn {
            data: vec![4, 1, 1, 1, 1, 3],
            bitmap: Some(Bitmap {
                bits: vec![1, 1, 1, 1, 1, 1],
            }),
        };

        let b = BucketColumn::from_hash(hash, 2);
        let bmap = BucketsSizeMap::from_bucket_column(b, 2);

        //This gives column with elements per chunk 1 4 0 1
        let part = strvec.partition_column(&None, &None, &bmap);

        let index = vec![None, Some(vec![0, 0]), None, Some(vec![0])];

        let flattened_index = part.flatten_index(&index);

        let flattened_column = part.flatten(&flattened_index);

        assert_eq!(
            flattened_column,
            FlattenedColumn::VariableLenTypeU8(
                ColumnU8 {
                    data: vec![97, 97, 98, 98, 98, 102, 102],
                    start_pos: vec![0, 2, 5],
                    len: vec![2, 3, 2]
                },
                None
            )
        )
    }

    #[test]
    fn partion_and_flatten() {
        use rand::distributions::Alphanumeric;
        use rand::prelude::*;
        use rayon::prelude::*;

        let strvec: Vec<String> = (0..1000usize)
            .into_par_iter()
            .map(|i| {
                let s: String = thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(i & 7)
                    .collect();
                s
            })
            .collect();
        let mut strvec_orig = strvec.clone();
        let strvec = StringVec { strvec };

        let s = RandomState::new();

        let hash = strvec.hash_column(&None, &None, &s);

        let b = BucketColumn::from_hash(hash, 4);
        let bmap = BucketsSizeMap::from_bucket_column(b, 2);

        let part = strvec.partition_column(&None, &None, &bmap);
        let part_index = match &part {
            PartitionedColumn::VariableLenType(columnu8, index, bitmap) => {
                let v: ColumnIndexPartitioned = columnu8.par_iter().map(|_| None).collect();
                v
            }

            _ => panic!(),
        };

        //println!("{:?}", part);

        let flattened_index = part.flatten_index(&part_index);

        let flattened_column = part.flatten(&flattened_index);

        let (data, start_pos, len) = match flattened_column {
            FlattenedColumn::VariableLenTypeU8(columnu8, None) => {
                (columnu8.data, columnu8.start_pos, columnu8.len)
            }
            _ => panic!(),
        };

        let mut strvec: Vec<String> = start_pos
            .par_iter()
            .zip_eq(len.par_iter())
            .map(|(start_pos, len)| {
                String::from_utf8(data[*start_pos..*start_pos + *len].to_vec()).unwrap()
            })
            .collect();
        strvec.sort();
        strvec_orig.sort();

        assert_eq!(strvec, strvec_orig);
    }
    #[test]
    fn partion_and_flatten_many_buckets_variable() {
        use rand::distributions::Alphanumeric;
        use rand::prelude::*;
        use rayon::prelude::*;

        let strvec: Vec<String> = (0..1000usize)
            .into_par_iter()
            .map(|i| {
                let s: String = thread_rng()
                    .sample_iter(&Alphanumeric)
                    .take(i & 7)
                    .collect();
                s
            })
            .collect();
        let mut strvec_orig = strvec.clone();
        let strvec = StringVec { strvec };

        let s = RandomState::new();

        let hash = strvec.hash_column(&None, &None, &s);

        let b = BucketColumn::from_hash(hash, 10);
        let bmap = BucketsSizeMap::from_bucket_column(b, 2);

        let part = strvec.partition_column(&None, &None, &bmap);
        let part_index = match &part {
            PartitionedColumn::VariableLenType(columnu8, index, bitmap) => {
                let v: ColumnIndexPartitioned = columnu8.par_iter().map(|_| None).collect();
                v
            }

            _ => panic!(),
        };

        //println!("{:?}", part);

        let flattened_index = part.flatten_index(&part_index);

        let flattened_column = part.flatten(&flattened_index);

        let (data, start_pos, len) = match flattened_column {
            FlattenedColumn::VariableLenTypeU8(columnu8, None) => {
                (columnu8.data, columnu8.start_pos, columnu8.len)
            }
            _ => panic!(),
        };

        let mut strvec: Vec<String> = start_pos
            .par_iter()
            .zip_eq(len.par_iter())
            .map(|(start_pos, len)| {
                String::from_utf8(data[*start_pos..*start_pos + *len].to_vec()).unwrap()
            })
            .collect();
        strvec.sort();
        strvec_orig.sort();

        assert_eq!(strvec, strvec_orig);
    }

    #[test]
    fn partion_and_flatten_fixed() {
        use rand::distributions::Standard;
        use rand::prelude::*;
        use rayon::prelude::*;

        let data: Vec<u64> = (0..1000usize)
            .into_par_iter()
            .map(|_| {
                let mut s: Vec<u64> = thread_rng().sample_iter(&Standard).take(1).collect();
                s.pop().unwrap()
            })
            .collect();

        let mut data_orig = data.clone();

        let s = RandomState::new();

        let hash = data.hash_column(&None, &None, &s);

        let b = BucketColumn::from_hash(hash, 4);
        let bmap = BucketsSizeMap::from_bucket_column(b, 2);

        let part = data.partition_column(&None, &None, &bmap);
        let part_index = match &part {
            PartitionedColumn::FixedLenType(column, index, bitmap) => {
                let v: ColumnIndexPartitioned = column.par_iter().map(|_| None).collect();
                v
            }

            _ => panic!(),
        };

        //println!("{:?}", part);

        let flattened_index = part.flatten_index(&part_index);

        let flattened_column = part.flatten(&flattened_index);

        let mut data = match flattened_column {
            FlattenedColumn::FixedLenType(data, None) => data,
            _ => panic!(),
        };

        data_orig.sort();
        data.sort();

        assert_eq!(data_orig, data);
    }

    #[test]
    fn partial_sum_serial_test() {
        use crate::helpers::*;
        let data: Vec<u64> = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let result = partial_sum_serial(&data, 0);
        assert_eq!(result, vec![1, 3, 6, 10, 15, 21, 28, 36]);

        let data: Vec<u64> = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let result = partial_sum_serial(&data, 1);
        assert_eq!(result, vec![2, 4, 7, 11, 16, 22, 29, 37]);
    }

    #[test]
    fn partial_sum_serial_assign_test() {
        use crate::helpers::*;
        let mut data: Vec<u64> = vec![1, 2, 3, 4, 5, 6, 7, 8];
        partial_sum_serial_assign(&mut data, 0);
        assert_eq!(data, vec![1, 3, 6, 10, 15, 21, 28, 36]);

        let mut data: Vec<u64> = vec![1, 2, 3, 4, 5, 6, 7, 8];
        partial_sum_serial_assign(&mut data, 1);
        assert_eq!(data, vec![2, 4, 7, 11, 16, 22, 29, 37]);
    }

    #[test]
    fn partial_sum_serial_with_buffer_test() {
        use crate::helpers::*;
        let data: Vec<u64> = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let mut result: Vec<u64> = vec![0; data.len()];
        partial_sum_serial_with_buffer(&data, &mut result, 0);
        assert_eq!(result, vec![1, 3, 6, 10, 15, 21, 28, 36]);

        partial_sum_serial_with_buffer(&data, &mut result, 1);
        assert_eq!(result, vec![2, 4, 7, 11, 16, 22, 29, 37]);
    }

    #[test]
    fn partial_sum_parallel_test() {
        use crate::helpers::*;
        let data: Vec<u64> = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let result = partial_sum_parallel(&data, 0, std::num::NonZeroUsize::new(4).unwrap());
        assert_eq!(result, vec![1, 3, 6, 10, 15, 21, 28, 36]);

        let data: Vec<u64> = vec![1, 2, 3, 4, 5, 6, 7, 8];
        let result = partial_sum_parallel(&data, 1, std::num::NonZeroUsize::new(4).unwrap());
        assert_eq!(result, vec![2, 4, 7, 11, 16, 22, 29, 37]);
    }

    #[test]
    fn prefix_sum_parallel_serial_equal() {
        use crate::helpers::*;
        use rand::distributions::Standard;
        use rand::prelude::*;

        let input: Vec<u64> = (0..100usize)
            .into_par_iter()
            .map(|_| {
                let mut s: Vec<u32> = thread_rng().sample_iter(&Standard).take(1).collect();
                s.pop().unwrap() as u64
            })
            .collect();

        let mut res_serial = partial_sum_serial(&input, 0);
        let res_parallel =
            partial_sum_parallel(&input, 0, std::num::NonZeroUsize::new(32).unwrap());
        assert_eq!(res_serial, res_parallel);

        partial_sum_serial_with_buffer(&input, &mut res_serial, 1);
        let res_parallel =
            partial_sum_parallel(&input, 1, std::num::NonZeroUsize::new(32).unwrap());
        assert_eq!(res_serial, res_parallel);
    }
    #[test]
    fn hash_to_bucket() {
        let hash_left: HashColumn = HashColumn {
            data: vec![2, 6, 4, 1, 8, 8, 8, 10],
            bitmap: None,
        };

        let hash_right: HashColumn = HashColumn {
            data: vec![2, 4, 6, 8, 3, 8],
            bitmap: None,
        };

        let (left_index, right_index) = hash_join(&hash_left, &hash_right, 2);
        assert_eq!(left_index.len(), 9);
        assert_eq!(right_index.len(), 9);
        let mut index_combined: Vec<(usize, usize)> = left_index
            .iter()
            .zip(right_index.iter())
            .map(|(left_i, right_i)| {
                assert_eq!(hash_left.data[*left_i], hash_right.data[*right_i]);
                (*left_i, *right_i)
            })
            .collect();
        index_combined.par_sort_unstable();
        let len_before_dedup = index_combined.len();
        index_combined.dedup();
        assert_eq!(len_before_dedup, index_combined.len());
    }
    #[test]
    fn sig_macro() {
        use std::any::TypeId;

        let s = sig!["add"; u64; u64, u64, u64];
        assert_eq!(s.input_len(), 3);

        let s = sig!["add"; u64];
        assert_eq!(s.input_len(), 0);

        let s1 = sig!["add"; u64; u64, u64, u64];
        let s2 = sig!["add"; u64; u64, u64, u64];
        assert!(s1 == s2);

        let s1 = sig!["add"; u64; u64, u64, u64];
        let s2 = sig!["sub"; u64; u64, u64, u64];
        assert!(s1 != s2);

        let s1 = sig!["add"; u64; u64, u64, u64];
        let s2 = sig!["add"; u32; u64, u64, u64];
        assert!(s1 != s2);

        let s1 = sig!["add"; u64; u64, u64, u64];
        let s2 = sig!["add"; u64; u64, u64, u32];
        assert!(s1 != s2);

        let s1 = sig!["add"; u64; u64, u64, u64];
        let s2 = sig!["add"; u64; u64, u64];
        assert!(s1 != s2);
    }

    #[test]
    fn basic_expression_2() {
        use crate::column::*;
        use std::any::TypeId;

        let mut dict: OpDictionary = HashMap::new();
        load_op_dict(&mut dict);

        let s = sig!["+="; OwnedColumn<Vec<u64>>;OwnedColumn<Vec<u64>>];

        let col1: OwnedColumn<Vec<u64>> = OwnedColumn::new(
            vec![1, 2, 3],
            None,
            Some(Bitmap {
                bits: vec![1, 0, 1],
            }),
        );

        let mut col1: Vec<Vec<Box<dyn Any + Send + Sync>>> = vec![
            vec![Box::new(col1)],
            vec![Box::new(OwnedColumn::<Vec<u64>>::new(
                vec![1, 2, 3],
                None,
                None,
            ))],
        ];
        let col2: Vec<Arc<OwnedColumn<Vec<u64>>>> = vec![
            Arc::new(OwnedColumn::new(vec![1, 3, 3], None, None)),
            Arc::new(OwnedColumn::new(vec![1, 3, 3], None, None)),
        ];
        let col3: Vec<Arc<OwnedColumn<Vec<u64>>>> = vec![
            Arc::new(OwnedColumn::new(vec![4, 5, 6], None, None)),
            Arc::new(OwnedColumn::new(vec![4, 5, 6], None, None)),
        ];
        let expr: Expression =
            Expression::new(s.clone(), Binding::OwnedColumn, vec![Binding::RefColumn]);
        let expr: Expression =
            Expression::new(s, Binding::Expr(Box::new(expr)), vec![Binding::RefColumn]);
        col1.par_iter_mut()
            .zip_eq(col2.par_iter())
            .zip_eq(col3.par_iter())
            .for_each(|((c1, c2), c3)| {
                let col2_ref: &OwnedColumn<Vec<u64>> = &(**c2);
                let col3_ref: &OwnedColumn<Vec<u64>> = &(**c3);
                expr.eval(c1, &mut vec![col2_ref, col3_ref], &mut vec![], &dict);
            });

        drop(col2);

        let col1: Vec<Box<OwnedColumn<Vec<u64>>>> = col1
            .into_iter()
            .map(|mut c| {
                (c.pop().unwrap() as Box<dyn Any>)
                    .downcast::<OwnedColumn<Vec<u64>>>()
                    .unwrap()
            })
            .collect();

        assert_eq!(col1[0].col(), &[6, 2, 12]);
        assert_eq!(col1[0].bitmap().as_ref().unwrap().bits, &[1, 0, 1]);
        assert_eq!(col1[1].col(), &[6, 10, 12]);
        assert_eq!(col1[1].bitmap(), &None);
    }

    #[test]
    fn expression_compile() {
        use crate::column::*;
        use std::any::TypeId;

        let mut dict: OpDictionary = HashMap::new();
        load_op_dict(&mut dict);
        let mut init_dict: InitDictionary = HashMap::new();
        load_init_dict(&mut init_dict);

        let s1 = sig!["+"; OwnedColumn<Vec<u64>>;OwnedColumn<Vec<u64>>, OwnedColumn<Vec<u64>>];
        let s2 = sig!["+="; OwnedColumn<Vec<u64>>;OwnedColumn<Vec<u64>>];

        let col1: OwnedColumn<Vec<u64>> = OwnedColumn::new(
            vec![1, 2, 3],
            None,
            Some(Bitmap {
                bits: vec![1, 0, 1],
            }),
        );

        let mut col1: Vec<Arc<OwnedColumn<Vec<u64>>>> = vec![
            Arc::new(col1),
            Arc::new(OwnedColumn::new(vec![1, 2, 3], None, None)),
            Arc::new(OwnedColumn::new(vec![1], Some(vec![0, 0, 0]), None)),
        ];
        let col2: Vec<Arc<OwnedColumn<Vec<u64>>>> = vec![
            Arc::new(OwnedColumn::new(vec![1, 3, 3], None, None)),
            Arc::new(OwnedColumn::new(vec![1, 3, 3], None, None)),
            Arc::new(OwnedColumn::new(vec![1, 3, 3], None, None)),
        ];
        let col3: Vec<Arc<OwnedColumn<Vec<u64>>>> = vec![
            Arc::new(OwnedColumn::new(vec![4, 5, 6], None, None)),
            Arc::new(OwnedColumn::new(vec![4, 5, 6], None, None)),
            Arc::new(OwnedColumn::new(vec![4, 5, 6], None, None)),
        ];

        let expr: Expression = Expression::new(
            s1.clone(),
            Binding::OwnedColumn,
            vec![Binding::RefColumn, Binding::RefColumn],
        );
        let expr: Expression =
            Expression::new(s2, Binding::Expr(Box::new(expr)), vec![Binding::RefColumn]);

        let (ops, mut owned_values) = expr.compile(&dict, &init_dict);
        assert_eq!(owned_values.len(), 1);
        assert_eq!(ops.len(), 2);

        let v = (owned_values.pop().unwrap() as Box<dyn Any>)
            .downcast::<OwnedColumn<Vec<u64>>>()
            .unwrap();

        let output: Vec<_> = col1
            .par_iter()
            .zip_eq(col2.par_iter())
            .zip_eq(col3.par_iter())
            .map(|((c1, c2), c3)| {
                let (ops, mut owned_values) = expr.compile(&dict, &init_dict);

                let col1_ref: &OwnedColumn<Vec<u64>> = &(**c1);
                let col2_ref: &OwnedColumn<Vec<u64>> = &(**c2);
                let col3_ref: &OwnedColumn<Vec<u64>> = &(**c3);
                expr.eval(
                    &mut owned_values,
                    &mut vec![col3_ref, col2_ref, col1_ref],
                    &mut vec![],
                    &dict,
                );

                owned_values
            })
            .collect();

        drop(col2);

        let output: Vec<Box<OwnedColumn<Vec<u64>>>> = output
            .into_iter()
            .map(|mut c| {
                (c.pop().unwrap() as Box<dyn Any>)
                    .downcast::<OwnedColumn<Vec<u64>>>()
                    .unwrap()
            })
            .collect();

        assert_eq!(*output[0].col(), &[6, 0, 12]);
        assert_eq!((*output[0].bitmap()).as_ref().unwrap().bits, &[1, 0, 1]);
        assert_eq!(*output[1].col(), &[6, 10, 12]);
        assert_eq!((*output[1].bitmap()), None);
        assert_eq!(*output[2].col(), &[6, 9, 10]);
        assert_eq!((*output[2].bitmap()), None);
    }
}
