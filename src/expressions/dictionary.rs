use std::collections::HashMap;

use crate::{
    load_init_dict, load_len_dict, load_op_dict, load_part_dict, ColumnWrapper, InitDictionary,
    LenDictionary, OpDictionary, PartitionDictionary, Signature,
};

pub struct Dictionary {
    pub op: OpDictionary,
    pub init: InitDictionary,
    pub part: PartitionDictionary,
    pub len_data: LenDictionary,
}

impl Dictionary {
    pub fn new() -> Self {
        let mut init: InitDictionary = HashMap::new();
        load_init_dict(&mut init);
        let mut op: OpDictionary = HashMap::new();
        load_op_dict(&mut op);
        let mut part: PartitionDictionary = HashMap::new();
        load_part_dict(&mut part);
        let mut len_data: LenDictionary = HashMap::new();
        load_len_dict(&mut len_data);
        Self {
            op,
            init,
            part,
            len_data,
        }
    }
}
