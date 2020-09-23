mod bitmap;
mod bucketcolumn;
mod column;
mod columnflatten;
mod columnpartition;
mod columnrepartition;
mod columnu8;
mod executor;
mod expressions;
mod hashjoin;
mod helpers;
mod sql;

pub use bucketcolumn::*;
pub use column::*;
pub use columnflatten::*;
pub use columnpartition::*;
pub use columnrepartition::*;
pub use columnu8::*;
pub use executor::*;
pub use expressions::*;
pub use hashjoin::*;
pub use sql::*;
//TO-DO: Make library safe (e.g.) safe rust code outside of it can't cause UB

#[cfg(test)]
mod tests {
    use crate::bitmap::*;
    use crate::bucketcolumn::*;
    use crate::columnu8::*;
    //use crate::expressions::operations::init_dict;
    use crate::hashcolumn::*;
    use crate::*;
    use std::{collections::HashMap, ops::Deref};

    use std::collections::hash_map::RandomState;
    use std::rc::*;

    use rayon::prelude::*;
    use sqlparser::ast::{Expr, SelectItem, SetExpr, Statement};

    fn get_first_projection(sqlstmt: &str) -> Expr {
        let ast = sql2ast(&sqlstmt);

        let p: Expr = if let Statement::Query(a) = &ast[0] {
            let query = &(**a);
            if let SetExpr::Select(a) = &query.body {
                let projection = &(**a).projection;
                let selectitem = &projection[0];
                if let SelectItem::UnnamedExpr(e) = selectitem {
                    e.clone()
                } else {
                    panic!()
                }
            } else {
                panic!()
            }
        } else {
            panic!()
        };
        p
    }

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
        assert_eq!(r1.deref(), r2.deref());
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
        assert_eq!(r1.deref(), r2.deref());
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

        *r2.bitmap_mut() = bitmap;
        v.hash_column_append(&None, &None, &s, &mut r2);
        assert_eq!(r1[0], r2[0]);
        assert_eq!(r1[1], r2[1]);
        assert_eq!(r1[2], r2[2]);
        assert_eq!(r1[3], r2[3]);

        assert!(r1.bitmap().is_none());
        assert!(r2.bitmap().is_some());
        assert_eq!(r2.bitmap().as_ref().unwrap().bits, vec![1, 1, 0, 1]);
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

        assert_eq!(r1.deref(), r2.deref());
        assert_eq!(r1.bitmap(), r2.bitmap());
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

        assert_eq!(r1[0], r2[0]);
        assert_eq!(r1[1], r2[1]);
        assert_eq!(r1[2], r2[2]);
        assert!(r1[3] != r2[3]);
        assert_eq!(r1[4], r2[4]);
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
        let hash = HashColumn::new(
            vec![1, 2, 4, 6, 8, 10],
            Some(Bitmap {
                bits: vec![0, 1, 1, 1, 1, 1],
            }),
        );

        let b = BucketColumn::from_hash(hash, 2);
        assert_eq!(*b, vec![1, 2, 0, 2, 0, 2]);
    }

    #[test]
    #[should_panic]
    fn bucket_init_overflow() {
        let hash = HashColumn::new(
            vec![1, 2, 4, 6, 8, 10],
            Some(Bitmap {
                bits: vec![0, 1, 1, 1, 1, 1],
            }),
        );

        let _b = BucketColumn::from_hash(hash, 63);
    }

    #[test]
    fn bucket_map_init() {
        let hash = HashColumn::new(vec![1, 2, 4, 6, 8, 10], None);

        let b = BucketColumn::from_hash(hash, 2);
        assert_eq!(*b, vec![1, 2, 0, 2, 0, 2]);
        let bmap = BucketsSizeMap::from_bucket_column(b, 2);
        assert_eq!(*bmap, vec![vec![1, 1, 1, 0], vec![1, 0, 2, 0]]);
        //println!("{:?}", *bmap)
    }

    #[test]
    fn bucket_map_init_serial() {
        let hash = HashColumn::new(vec![1, 2, 4, 6, 8, 10], None);

        let b = BucketColumn::from_hash(hash, 2);
        assert_eq!(*b, vec![1, 2, 0, 2, 0, 2]);
        let bmap = BucketsSizeMap::from_bucket_column(b, 1);
        assert_eq!(*bmap, vec![vec![2, 1, 3, 0]]);
        //println!("{:?}", *bmap)
    }

    #[test]
    fn bucket_size_map_init() {
        let hash = HashColumn::new(vec![1, 2, 4, 6, 8, 10], None);

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
        let hash = HashColumn::new(vec![1, 2, 4, 6, 8, 10], None);

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
        let hash = HashColumn::new(
            vec![1, 1, 2, 3, 4, 5],
            Some(Bitmap {
                bits: vec![0, 1, 1, 1, 1, 1],
            }),
        );

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

        let hash = HashColumn::new(
            vec![4, 1, 1, 1, 1, 3],
            Some(Bitmap {
                bits: vec![1, 1, 1, 1, 1, 1],
            }),
        );

        let b = BucketColumn::from_hash(hash, 2);
        let bmap = BucketsSizeMap::from_bucket_column(b, 2);
        let part = data.partition_column(&None, &None, &bmap);

        let hash = HashColumn::new(data, None);

        let hash = hash.partition_column(&None, &None, &bmap);

        let hash = if let PartitionedColumn::FixedLenType(hash_inner, _index, _bitmap) = hash {
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

        let part = part.partition_column(&bmap);

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

        let hash = HashColumn::new(
            vec![4, 1, 1, 1, 1, 3],
            Some(Bitmap {
                bits: vec![1, 1, 1, 1, 1, 1],
            }),
        );

        let b = BucketColumn::from_hash(hash, 2);
        let bmap = BucketsSizeMap::from_bucket_column(b, 2);
        let part = strvec.partition_column(&None, &None, &bmap);

        let data: Vec<u64> = vec![1, 2, 4, 6, 8, 10];
        let hash = HashColumn::new(data, None);

        let hash = hash.partition_column(&None, &None, &bmap);
        let hash = if let PartitionedColumn::FixedLenType(hash_inner, _index, _bitmap) = hash {
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

        let part = if let PartitionedColumn::VariableLenType(part_inner, _index, _bitmap) = part {
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

        let hash = HashColumn::new(
            vec![4, 1, 1, 1, 1, 3],
            Some(Bitmap {
                bits: vec![1, 1, 1, 1, 1, 1],
            }),
        );

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

        let hash = HashColumn::new(
            vec![4, 1, 1, 1, 1, 3],
            Some(Bitmap {
                bits: vec![1, 1, 1, 1, 1, 1],
            }),
        );

        let b = BucketColumn::from_hash(hash, 2);
        let bmap = BucketsSizeMap::from_bucket_column(b, 2);

        //This gives column with elements per chunk 1 4 0 1
        let part = data.partition_column(&None, &None, &bmap);

        let index = vec![None, Some(vec![0, 0]), None, Some(vec![0])];

        let flattened_index = part.flatten_index(&index);

        let flattened_column = part.flatten_column(&flattened_index);

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

        let hash = HashColumn::new(
            vec![4, 1, 1, 1, 1, 3],
            Some(Bitmap {
                bits: vec![1, 1, 1, 1, 1, 1],
            }),
        );

        let b = BucketColumn::from_hash(hash, 2);
        let bmap = BucketsSizeMap::from_bucket_column(b, 2);

        //This gives column with elements per chunk 1 4 0 1
        let part = strvec.partition_column(&None, &None, &bmap);

        let index = vec![None, Some(vec![0, 0]), None, Some(vec![0])];

        let flattened_index = part.flatten_index(&index);

        let flattened_column = part.flatten_column(&flattened_index);

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
            PartitionedColumn::VariableLenType(columnu8, _index, _bitmap) => {
                let v: ColumnIndexPartitioned = columnu8.par_iter().map(|_| None).collect();
                v
            }

            _ => panic!(),
        };

        //println!("{:?}", part);

        let flattened_index = part.flatten_index(&part_index);

        let flattened_column = part.flatten_column(&flattened_index);

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
            PartitionedColumn::VariableLenType(columnu8, _index, _bitmap) => {
                let v: ColumnIndexPartitioned = columnu8.par_iter().map(|_| None).collect();
                v
            }

            _ => panic!(),
        };

        //println!("{:?}", part);

        let flattened_index = part.flatten_index(&part_index);

        let flattened_column = part.flatten_column(&flattened_index);

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
            PartitionedColumn::FixedLenType(column, _index, _bitmap) => {
                let v: ColumnIndexPartitioned = column.par_iter().map(|_| None).collect();
                v
            }

            _ => panic!(),
        };

        //println!("{:?}", part);

        let flattened_index = part.flatten_index(&part_index);

        let flattened_column = part.flatten_column(&flattened_index);

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
        let hash_left: HashColumn = HashColumn::new(vec![2, 6, 4, 1, 8, 8, 8, 10], None);
        let hash_right: HashColumn = HashColumn::new(vec![2, 4, 6, 8, 3, 8], None);

        let (left_index, right_index) = hash_join(&hash_left, &hash_right, 2);
        assert_eq!(left_index.len(), 9);
        assert_eq!(right_index.len(), 9);
        let mut index_combined: Vec<(usize, usize)> = left_index
            .iter()
            .zip(right_index.iter())
            .map(|(left_i, right_i)| {
                assert_eq!(hash_left[*left_i], hash_right[*right_i]);
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
        let s = sig!["add";  u64, u64, u64];
        assert_eq!(s.input_len(), 3);

        let s = sig!["add"];
        assert_eq!(s.input_len(), 0);

        let s1 = sig!["add"; u64, u64, u64];
        let s2 = sig!["add"; u64, u64, u64];
        assert!(s1 == s2);

        let s1 = sig!["add";  u64, u64, u64];
        let s2 = sig!["sub";  u64, u64, u64];
        assert!(s1 != s2);

        let s1 = sig!["add";  u64, u64, u64];
        let s2 = sig!["add"; u64, u64, u32];
        assert!(s1 != s2);

        let s1 = sig!["add";  u64, u64, u64];
        let s2 = sig!["add"; u64, u64];
        assert!(s1 != s2);
    }

    #[test]
    fn expression_compile() {
        use crate::column::*;

        let mut dict: OpDictionary = HashMap::new();
        load_op_dict(&mut dict);
        let mut init_dict: InitDictionary = HashMap::new();
        load_init_dict(&mut init_dict);

        let s1 = sig!["+"; Vec<u32>, Vec<u32>];
        let s2 = sig!["+"; Vec<u64>, Vec<u32>];
        let s3 = sig!["+="; Vec<u64>,[u64]];

        let col1: Vec<ColumnWrapper> = vec![
            //let col1: Vec<Arc<OwnedColumn<Vec<u64>>>> = vec![
            ColumnWrapper::new(
                vec![1_u32, 2, 3],
                None,
                Some(Bitmap {
                    bits: vec![1, 0, 1],
                }),
            ),
            ColumnWrapper::new(vec![1_u32, 2, 3], None, None),
            ColumnWrapper::new(vec![1_u32], Some(vec![0, 0, 0]), None),
        ];
        let col2: Vec<ColumnWrapper> = vec![
            ColumnWrapper::new(vec![1_u32, 3, 3], None, None),
            ColumnWrapper::new(vec![1_u32, 3, 3], None, None),
            ColumnWrapper::new(vec![1_u32, 3, 3], None, None),
        ];

        let col3: Vec<ColumnWrapper> = vec![
            ColumnWrapper::new(vec![0_u64, 0, 0], None, None),
            ColumnWrapper::new(vec![0_u64, 0, 0], None, None),
            ColumnWrapper::new(vec![0_u64, 0, 0], None, None),
        ];

        let col4: Vec<ColumnWrapper> = vec![
            ColumnWrapper::new(vec![4_u64, 5, 6], None, None),
            ColumnWrapper::new(vec![4_u64, 5, 6], None, None),
            ColumnWrapper::new(vec![4_u64, 5, 6], None, None),
        ];
        let col4: Vec<u64> = vec![4, 5, 6, 4, 5, 6, 4, 5, 6];
        let expr: Expression = Expression::new(
            s1,
            Binding::OwnedColumn,
            vec![Binding::RefColumn(0), Binding::RefColumn(1)],
        );
        let expr: Expression = Expression::new(
            s2,
            Binding::OwnedColumn,
            vec![Binding::RefColumn(2), Binding::Expr(Box::new(expr))],
        );
        let expr: Expression = Expression::new(
            s3,
            Binding::Expr(Box::new(expr)),
            vec![Binding::RefColumn(3)],
        );

        let (ops, owned_values) = expr.compile(&dict, &init_dict);
        assert_eq!(owned_values.len(), 2);
        assert_eq!(ops.len(), 3);

        let output: Vec<_> = col1
            .iter()
            .zip(col2.iter())
            .zip(col3.iter())
            .zip(col4.chunks(3))
            .map(|(((c1, c2), c3), c4)| {
                let (_, mut owned_values) = expr.compile(&dict, &init_dict);

                let mut owned_values_refmut = owned_values.iter_mut().collect();
                let c4_slice = ColumnWrapper::new_slice(c4, None, None);

                expr.eval(
                    &mut owned_values_refmut,
                    &vec![c1, c2, c3, &c4_slice],
                    &vec![],
                    &dict,
                );

                owned_values.pop().unwrap()
            })
            .collect();

        drop(col2);

        let output: Vec<Vec<u64>> = output.into_iter().map(|c| c.unwrap::<Vec<u64>>()).collect();

        assert_eq!(output[0], &[6, 0, 12]);
        assert_eq!(output[1], &[6, 10, 12]);
        assert_eq!(output[2], &[6, 9, 10]);
    }

    #[test]
    fn parse_expression() {
        let mut dict: OpDictionary = HashMap::new();
        load_op_dict(&mut dict);
        let mut init_dict: InitDictionary = HashMap::new();
        load_init_dict(&mut init_dict);

        let data_col1 = vec![4_u64, 5];
        let mut data_col2 = vec![4_u32, 5, 6];

        let c1 =
            ColumnWrapper::new_ref(&data_col1, Some(vec![0_usize, 0, 0]), None).with_name("col1");
        let c2 = ColumnWrapper::new_ref_mut(
            &mut data_col2,
            None,
            Some(Bitmap {
                bits: vec![1, 1, 0],
            }),
        )
        .with_name("col2");
        let c3 = ColumnWrapper::new(vec![4_u32, 5, 6], None, None).with_name("col3");
        let ref_columns = vec![&c1, &c2, &c3];

        let sqlstmt = "SELECT ((col1+col2)+col3)";
        let p = get_first_projection(sqlstmt);
        let expr = parseexpr(&p, &ref_columns, &dict);

        let mut owned_columns = expr.compile(&dict, &init_dict).1;
        expr.eval(
            &mut owned_columns.iter_mut().collect(),
            &ref_columns,
            &vec![],
            &dict,
        );

        drop(data_col2);
        drop(data_col1);

        assert!(!owned_columns.is_empty());
        let result = owned_columns.pop().unwrap();
        assert_eq!(result.bitmap().as_ref().unwrap().bits, vec![1, 1, 0]);

        let val = result.unwrap::<Vec<u64>>();
        assert_eq!(val, vec![12, 14, 0]);
    }
    #[test]
    fn test_eqinit() {
        let mut dict: OpDictionary = HashMap::new();
        load_op_dict(&mut dict);
        let mut init_dict: InitDictionary = HashMap::new();
        load_init_dict(&mut init_dict);

        let c1 = ColumnWrapper::new(
            vec![1_u64, 2, 3],
            None,
            Some(Bitmap {
                bits: vec![1, 0, 1],
            }),
        )
        .with_name("col1");
        let c2 = ColumnWrapper::new(vec![1_u64, 2, 3], Some(vec![0, 1, 1]), None).with_name("col2");

        let ref_columns = vec![&c1, &c2];

        let sqlstmt = "SELECT col1=col2";
        let p = get_first_projection(sqlstmt);
        let expr = parseexpr(&p, &ref_columns, &dict);

        let mut owned_columns = expr.compile(&dict, &init_dict).1;
        expr.eval(
            &mut owned_columns.iter_mut().collect(),
            &ref_columns,
            &vec![],
            &dict,
        );

        assert!(!owned_columns.is_empty());
        let result = owned_columns.pop().unwrap();

        let val = result.unwrap::<Vec<bool>>();
        assert_eq!(val, vec![true, false, false]);
    }

    #[test]
    fn test_applyoneif() {
        struct CrazyVec<'a, T> {
            data: Vec<T>,
            phantom: std::marker::PhantomData<&'a T>,
        }

        /*     Ref(&'a (dyn Any + Send + Sync)),
               RefMut(&'a mut (dyn Any + Send + Sync)),
        */

        let mut dict: OpDictionary = HashMap::new();
        load_op_dict(&mut dict);
        let mut init_dict: InitDictionary = HashMap::new();
        load_init_dict(&mut init_dict);

        let mut c1 = ColumnWrapper::new(
            vec![1_u64, 2, 3, 10, 13],
            None,
            Some(Bitmap {
                bits: vec![1, 0, 1, 1, 1],
            }),
        )
        .with_name("col1");

        let c1_index_orig = c1.index().clone();
        let mut c2 =
            ColumnWrapper::new(vec![1_u64, 2, 3], Some(vec![0, 1, 1]), None).with_name("col2");
        let c2_index_orig = c2.index().clone();

        let sqlstmt = "SELECT col1=col2";
        let p = get_first_projection(sqlstmt);
        let ref_columns = vec![&c1, &c2];
        let expr = parseexpr(&p, &ref_columns, &dict);

        //println!("{:?}", expr);

        let mut index_left = vec![0, 0, 1, 2, 0];
        let mut index_right = vec![0, 1, 1, 2, 0];

        applyoneif(
            &mut vec![&mut c1],
            &mut vec![&mut c2],
            &mut index_left,
            &mut index_right,
            &expr,
            &dict,
            &init_dict,
        );

        assert_eq!(index_left, vec![0, 0]);
        assert_eq!(index_right, vec![0, 0]);
        assert_eq!(c1.index(), &c1_index_orig);
        assert_eq!(c2.index(), &c2_index_orig);
        let mut t: Vec<u64> = vec![1, 2, 3, 4, 5];
        let (l, r) = t.split_at_mut(3);
        let mut srm = SliceRefMut::new(l);
        let a = srm.downcast_mut::<[u64]>().unwrap();
        a[1] = 7;
        drop(a);
        l[0] = 6;
        drop(l);
        t.iter().for_each(|i| println!("{}", i));
        //let v = unsafe { slice_to_vec(l) };
    }
    #[test]
    fn test_partition() {
        let col4: Vec<u64> = vec![1, 5, 6, 4, 5, 6, 4, 5, 6, 8];
        let mut col4 = ColumnWrapper::new(col4, None, None);
        let signature = sig!["part"; Vec<u64>];
        let mut part_dict: PartitionDictionary = HashMap::new();
        load_part_dict(&mut part_dict);
        {
            let inp: InputTypes = InputTypes::Ref(&col4);
            let op = part_dict.get(&signature).unwrap();
            let col4_part = (*op).part(&inp, 3);
            col4_part
                .iter()
                .for_each(|c| println!("{:?}", c.downcast_slice_ref::<[u64]>()));
        }

        let tmp = col4.downcast_mut::<Vec<u64>>();
        tmp[0] = 4;
        let inp: InputTypes = InputTypes::Ref(&col4);
        let op = part_dict.get(&signature).unwrap();
        let col4_part = (*op).part(&inp, 3);
        col4_part
            .iter()
            .for_each(|c| println!("{:?}", c.downcast_slice_ref::<[u64]>()));
    }
}
