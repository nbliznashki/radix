mod bucketcolumn;
mod columnpartition;
mod columnrepartition;
mod columnu8;
mod hashcolumn;

pub use columnpartition::*;
pub use columnrepartition::*;

//TO-DO: Make library safe (e.g.) safe rust code outside of it can't cause UB

#[cfg(test)]
mod tests {
    use crate::bucketcolumn::*;
    use crate::columnu8::*;
    use crate::hashcolumn::*;
    use crate::*;

    use std::collections::hash_map::RandomState;
    use std::rc::*;

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
        }
    }
    #[test]
    fn compare_hash_with_and_without_index() {
        let s = RandomState::new();
        let v: Vec<usize> = vec![1, 1, 2, 3];

        let r1 = v.hash_column(&s, None);
        let index: Vec<Option<usize>> = vec![Some(0), Some(1), Some(2), Some(3)];
        let r2 = v.hash_column(&s, Some(index));
        assert_eq!(r1.data, r2.data);
    }

    //Validate that the function behaves the same way when given an index and when not given an index
    #[test]
    fn compare_hash_append_with_and_without_index() {
        let s = RandomState::new();
        let v: Vec<usize> = vec![1, 1, 2, 3];

        let mut r1 = v.hash_column(&s, None);
        v.hash_column_append(&s, &mut r1);

        let _index: Vec<Option<usize>> = vec![Some(0), Some(1), Some(2), Some(3)];
        let mut r2 = v.hash_column(&s, None);
        v.hash_column_append(&s, &mut r2);
        assert_eq!(r1, r2);
    }
    //Validate that the function behaves the same way when given an index and when not given an index
    #[test]
    fn compare_hash_with_null() {
        let s = RandomState::new();
        let v: Vec<usize> = vec![1, 1, 2, 3];

        let mut r1 = v.hash_column(&s, None);
        v.hash_column_append(&s, &mut r1);

        let index: Vec<Option<usize>> = vec![Some(0), Some(1), None, Some(3)];
        let mut r2 = v.hash_column(&s, None);
        r2.index = Some(index);
        v.hash_column_append(&s, &mut r2);
        assert_eq!(r1.data[0], r2.data[0]);
        assert_eq!(r1.data[1], r2.data[1]);
        assert!(r1.data[2] != r2.data[2]);
        assert_eq!(r1.data[3], r2.data[3]);
    }
    //Validate that the function behaves the same way when given an index and when not given an index
    #[test]
    fn first_bit_hash_with_null() {
        let s = RandomState::new();
        let v: Vec<usize> = vec![1, 1, 2, 3];

        let index: Vec<Option<usize>> = vec![Some(0), Some(1), None, Some(2), Some(3)];
        let r1 = v.hash_column(&s, Some(index));

        assert_eq!(r1[0] & 1, 0);
        assert_eq!(r1[1] & 1, 0);
        assert_eq!(r1[2] & 1, 1);
        assert_eq!(r1[3] & 1, 0);
        assert_eq!(r1[4] & 1, 0);
    }
    //Validate that the function behaves the same way when given an index and when not given an index
    #[test]
    fn first_bit_hash_add_with_null() {
        let s = RandomState::new();
        let v1: Vec<usize> = vec![1, 1, 2, 3, 5];
        let v2: Vec<usize> = vec![1, 1, 2, 3];

        let index: Vec<Option<usize>> = vec![Some(0), Some(1), None, Some(2), Some(3)];
        let mut r1 = v1.hash_column(&s, Some(index));
        v2.hash_column_append(&s, &mut r1);

        assert_eq!(r1[0] & 1, 0);
        assert_eq!(r1[1] & 1, 0);
        assert_eq!(r1[2] & 1, 1);
        assert_eq!(r1[3] & 1, 0);
        assert_eq!(r1[4] & 1, 0);
    }

    //Should not be possible to add a column of different length
    #[test]
    #[should_panic]
    fn different_len_hash_append() {
        let s = RandomState::new();
        let v1: Vec<usize> = vec![1, 1, 2, 3, 5];
        let v2: Vec<usize> = vec![1, 1, 2, 3];

        let index: Vec<Option<usize>> = vec![Some(0), Some(1), None, Some(2), Some(3)];
        let mut r1 = v1.hash_column(&s, Some(index));
        r1.index = None;
        v2.hash_column_append(&s, &mut r1);
    }

    #[test]
    fn bucket_init() {
        let hash: HashColumn = HashColumn {
            data: vec![1, 2, 4, 6, 8, 10],
            index: None,
        };
        let b = BucketColumn::from_hash(hash, 2);
        assert_eq!(*b, vec![1, 2, 4, 6, 0, 2]);
    }

    #[test]
    #[should_panic]
    fn bucket_init_overflow() {
        let hash: HashColumn = HashColumn {
            data: vec![1, 2, 4, 6, 8, 10],
            index: None,
        };
        let _b = BucketColumn::from_hash(hash, 63);
    }

    #[test]
    fn bucket_map_init() {
        let hash: HashColumn = HashColumn {
            data: vec![1, 2, 4, 6, 8, 10],
            index: None,
        };
        let b = BucketColumn::from_hash(hash, 2);
        assert_eq!(*b, vec![1, 2, 4, 6, 0, 2]);
        let bmap = BucketsSizeMap::from_bucket_column(b, 2);
        assert_eq!(*bmap, vec![vec![0, 1, 1, 0], vec![1, 1, 0, 1]]);
        //println!("{:?}", *bmap)
    }

    #[test]
    fn bucket_map_init_serial() {
        let hash: HashColumn = HashColumn {
            data: vec![1, 2, 4, 6, 8, 10],
            index: None,
        };
        let b = BucketColumn::from_hash(hash, 2);
        assert_eq!(*b, vec![1, 2, 4, 6, 0, 2]);
        let bmap = BucketsSizeMap::from_bucket_column(b, 1);
        assert_eq!(*bmap, vec![vec![1, 2, 1, 1]]);
        //println!("{:?}", *bmap)
    }

    #[test]
    fn bucket_size_map_init() {
        let hash: HashColumn = HashColumn {
            data: vec![1, 2, 4, 6, 8, 10],
            index: None,
        };
        let b = BucketColumn::from_hash(hash, 2);
        assert_eq!(*b, vec![1, 2, 4, 6, 0, 2]);
        let bmap = BucketsSizeMap::from_bucket_column(b, 2);
        assert_eq!(*bmap, vec![vec![0, 1, 1, 0], vec![1, 1, 0, 1]]);
        assert_eq!(bmap.offsets, vec![vec![0, 0, 0, 0], vec![0, 1, 1, 0]]);
        assert_eq!(bmap.bucket_sizes, vec![1, 2, 1, 1]);
        //println!("{:?}", *bmap)
    }
    #[test]
    fn partition_column_test() {
        let hash: HashColumn = HashColumn {
            data: vec![1, 2, 4, 6, 8, 10],
            index: None,
        };
        let data = vec![1, 2, 4, 6, 8, 10];
        let b = BucketColumn::from_hash(hash, 2);
        assert_eq!(*b, vec![1, 2, 4, 6, 0, 2]);
        let bmap = BucketsSizeMap::from_bucket_column(b, 2);
        assert_eq!(*bmap, vec![vec![0, 1, 1, 0], vec![1, 1, 0, 1]]);
        assert_eq!(bmap.offsets, vec![vec![0, 0, 0, 0], vec![0, 1, 1, 0]]);
        assert_eq!(bmap.bucket_sizes, vec![1, 2, 1, 1]);

        let part = data.partition_column(&bmap);
        assert_eq!(
            part,
            PartitionedColumn::FixedLenType(vec![vec![8], vec![2, 10], vec![4], vec![6]])
        );
    }

    #[test]
    fn partition_column_test_string() {
        let hash: HashColumn = HashColumn {
            data: vec![1, 2, 4, 6, 8, 10],
            index: None,
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
        assert_eq!(*b, vec![1, 2, 4, 6, 0, 2]);
        let bmap = BucketsSizeMap::from_bucket_column(b, 2);
        assert_eq!(*bmap, vec![vec![0, 1, 1, 0], vec![1, 1, 0, 1]]);
        assert_eq!(bmap.offsets, vec![vec![0, 0, 0, 0], vec![0, 1, 1, 0]]);
        assert_eq!(bmap.bucket_sizes, vec![1, 2, 1, 1]);

        let part = strvec.partition_column(&bmap);

        let expected_result = PartitionedColumn::<String>::VariableLenType(vec![
            ColumnU8 {
                data: vec![101, 101],
                start_pos: vec![0],
                len: vec![2],
            },
            ColumnU8 {
                data: vec![98, 98, 102, 102, 102],
                start_pos: vec![0, 2],
                len: vec![2, 3],
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
        ]);
        assert_eq!(part, expected_result);
    }

    #[test]
    fn repartition_column_test() {
        let data: Vec<u64> = vec![1, 2, 4, 6, 8, 10];

        let hash: HashColumn = HashColumn {
            data: vec![8, 2, 2, 2, 2, 6],
            index: Some(vec![Some(0), Some(1), Some(2), Some(3), Some(4), Some(5)]),
        };

        let b = BucketColumn::from_hash(hash, 2);
        let bmap = BucketsSizeMap::from_bucket_column(b, 2);
        let part = data.partition_column(&bmap);

        let hash = HashColumn { data, index: None };

        let hash = hash.data.partition_column(&bmap);
        let hash = if let PartitionedColumn::FixedLenType(hash_inner) = hash {
            hash_inner
        } else {
            panic!()
        };
        let index = hash.iter().map(|_| None).collect();
        let hash: HashColumnPartitioned = HashColumnPartitioned { data: hash, index };

        let b = BucketColumnPartitioned::from_hash(hash, 2);
        let mut bmap = BucketsSizeMapPartitioned::from_bucket_column(b);

        let new_index = bmap
            .bucket_column
            .iter()
            .enumerate()
            .map(|(i, v)| {
                if i & 2 == 0 {
                    Some((0..v.len()).map(Some).collect())
                } else {
                    None
                }
            })
            .collect();

        bmap.hash.index = new_index;

        println!("{:?}", part);
        let part = part.partition_column(&bmap);
        println!("{:?}", part);

        assert_eq!(
            part,
            PartitionedColumn::FixedLenType(vec![vec![8], vec![2, 10], vec![4], vec![6]])
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
            data: vec![8, 2, 2, 2, 2, 6],
            index: Some(vec![Some(0), Some(1), Some(2), Some(3), Some(4), Some(5)]),
        };

        let b = BucketColumn::from_hash(hash, 2);
        let bmap = BucketsSizeMap::from_bucket_column(b, 2);
        let part = strvec.partition_column(&bmap);

        let data: Vec<u64> = vec![1, 2, 4, 6, 8, 10];
        let hash = HashColumn { data, index: None };

        let hash = hash.data.partition_column(&bmap);
        let hash = if let PartitionedColumn::FixedLenType(hash_inner) = hash {
            hash_inner
        } else {
            panic!()
        };

        let index = hash.iter().map(|_| None).collect();
        let hash: HashColumnPartitioned = HashColumnPartitioned { data: hash, index };

        let b = BucketColumnPartitioned::from_hash(hash, 2);
        let mut bmap = BucketsSizeMapPartitioned::from_bucket_column(b);

        let mut part = if let PartitionedColumn::VariableLenType(part_inner) = part {
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
                    Some((0..v.len()).map(Some).collect())
                } else {
                    None
                }
            })
            .collect();

        bmap.hash.index = new_index;
        let part = PartitionedColumn::VariableLenType(part);

        println!("{:?}", part);
        let part = part.partition_column(&bmap);
        println!("{:?}", part);
        let expected_result = PartitionedColumn::<String>::VariableLenType(vec![
            ColumnU8 {
                data: vec![101, 101],
                start_pos: vec![0],
                len: vec![2],
            },
            ColumnU8 {
                data: vec![98, 98, 102, 102, 102],
                start_pos: vec![0, 2],
                len: vec![2, 3],
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
        ]);
        assert_eq!(part, expected_result);
    }
}
