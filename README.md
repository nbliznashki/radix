# radix

Library for parallel partitioning of vectors of data based on hash.
Also contains an sql statements executor, e.g.:

'''
let mut dict: OpDictionary = HashMap::new();
        load_op_dict(&mut dict);
        let mut init_dict: InitDictionary = HashMap::new();
        load_init_dict(&mut init_dict);

        //Initialize the source data - col1, col2, col3
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
        let ref_columns = vec![c1, c2, c3];


        //Define SQL statement to be executed
        let sqlstmt = "SELECT ((col1+col2)+col3)";
        let p = get_first_projection(sqlstmt);
        //Parse expression
        let expr = parseexpr(&p, &ref_columns);

        //Create temporary calculation columns
        let mut owned_columns = expr.compile(&dict, &init_dict).1;
        //Evaluate expression
        expr.eval(
            &mut owned_columns.iter_mut().collect(),
            &(ref_columns.iter().collect()),
            &vec![],
            &dict,
        );

        drop(data_col2);
        drop(data_col1);

        //The SQL statement processed is "SELECT ((col1+col2)+col3)""
        //The value of of the columns is as folows:
        //col1: [4,4,4], type u64
        //col2: [4,5,null], type u32
        //col3: [4,5,6], type u32
        //expected result after summation: [12, 14, 0], null index: [1,1,0]

        assert!(!owned_columns.is_empty());
        let result = owned_columns.pop().unwrap();
        assert_eq!(result.bitmap().as_ref().unwrap().bits, vec![1, 1, 0]);

        let val = result.unwrap::<Vec<u64>>();
        assert_eq!(val, vec![12, 14, 0]);

'''


