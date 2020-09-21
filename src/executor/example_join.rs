println!("4d. Example of join of 2 tables, using vectorized executor");
    let start = Instant::now();
    let s1 = StringVec { strvec: v11 };
    let s2 = StringVec { strvec: v21 };
    let s = RandomState::new();

    let h1 = s1.hash_column(&None, &None, &s);
    let h11 = s1.hash_column(&None, &None, &s);
    let h2 = s2.hash_column(&None, &None, &s);
    let h21 = s2.hash_column(&None, &None, &s);

    let b1 = BucketColumn::from_hash(h1, 6);
    let bmap1 = BucketsSizeMap::from_bucket_column(b1, 32);
    let c11_part = s1.partition_column(&None, &None, &bmap1);
    let mut c11_part = match c11_part {
        PartitionedColumn::VariableLenType(v, _, _) => v,
        _ => panic!(),
    };

    let h11: ColumnWrapper<'static> = h11.into();
    let (h11, _, _) = h11.all_unwrap::<Vec<u64>>();
    let h11 = h11.partition_column(&None, &None, &bmap1);
    let h11 = match h11 {
        PartitionedColumn::FixedLenType::<u64>(v, _, _) => v,
        _ => panic!(),
    };

    let c10_part = v10.partition_column(&None, &None, &bmap1);
    let mut c10_part = match c10_part {
        PartitionedColumn::FixedLenType(v, _, _) => v,
        _ => panic!(),
    };

    let b2 = BucketColumn::from_hash(h2, 6);
    let bmap2 = BucketsSizeMap::from_bucket_column(b2, 6);
    let c21_part = s2.partition_column(&None, &None, &bmap2);
    let mut c21_part = match c21_part {
        PartitionedColumn::VariableLenType(v, _, _) => v,
        _ => panic!(),
    };

    let h21: ColumnWrapper<'static> = h21.into();
    let (h21, _, _) = h21.all_unwrap::<Vec<u64>>();
    let h21 = h21.partition_column(&None, &None, &bmap2);
    let h21 = match h21 {
        PartitionedColumn::FixedLenType::<u64>(v, _, _) => v,
        _ => panic!(),
    };

    let c20_part = v20.partition_column(&None, &None, &bmap2);
    let mut c20_part = match c20_part {
        PartitionedColumn::FixedLenType(v, _, _) => v,
        _ => panic!(),
    };

    let iter_1 = h11
        .par_iter()
        .zip_eq(c10_part.par_iter_mut())
        .zip_eq(c11_part.par_iter_mut());

    let iter_2 = h21
        .par_iter()
        .zip_eq(c20_part.par_iter_mut())
        .zip_eq(c21_part.par_iter_mut());

    let mut dict: OpDictionary = HashMap::new();
    load_op_dict(&mut dict);
    let mut init_dict: InitDictionary = HashMap::new();
    load_init_dict(&mut init_dict);

    let res: Vec<_> = iter_1
        .zip_eq(iter_2)
        .map(|(((h1, c10), c11), ((h2, c20), c21))| {
            let h1 = HashColumn::new_ref(h1, None);
            let mut col10 = ColumnWrapper::new_ref_mut(c10, None, None).with_name("col10");
            let mut col11 = ColumnWrapper::new_ref_u8(c11, None, None).with_name("col11");

            let h2 = HashColumn::new_ref(h2, None);
            let mut col20 = ColumnWrapper::new_ref_mut(c20, None, None).with_name("col20");
            let mut col21 = ColumnWrapper::new_ref_u8(c21, None, None).with_name("col21");
            let (mut index_left, mut index_right) = build_ind(&h1, &h2, 2_usize.pow(6));

            let sqlstmt = "SELECT col11=col21";
            let p = get_first_projection(sqlstmt);
            let ref_columns = vec![&col11, &col21];
            let expr = radix::parseexpr(&p, &ref_columns, &dict);

            applyoneif(
                &mut vec![&mut col11],
                &mut vec![&mut col21],
                &mut index_left,
                &mut index_right,
                &expr,
                &dict,
                &init_dict,
            );

            let sqlstmt = "SELECT col10>=col20";
            let p = get_first_projection(sqlstmt);
            let ref_columns = vec![&col10, &col20];
            let expr = radix::parseexpr(&p, &ref_columns, &dict);

            applyoneif(
                &mut vec![&mut col10],
                &mut vec![&mut col20],
                &mut index_left,
                &mut index_right,
                &expr,
                &dict,
                &init_dict,
            );

            (index_left, index_right)
        })
        .collect();

    let v11 = s1.strvec;
    let v21 = s2.strvec;

    println!(
        "Time elapsed: {:?}, s: {}",
        start.elapsed(),
        res.iter().map(|(l, r)| l.len() + r.len()).sum::<usize>() / 2
    );
	
	
