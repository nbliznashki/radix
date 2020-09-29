use crate::bitmap::Bitmap;
use crate::*;

const OP: &str = "==";

pub(crate) fn load_op_dict(dict: &mut OpDictionary) {
    let signature = sig![OP; ColumnU8,ColumnU8];
    let op = Operation {
        f: eqinit_vecu64_columnu8,
        output_type: std::any::TypeId::of::<Vec<bool>>(),
        output_typename: std::any::type_name::<Vec<bool>>().to_string(),
    };
    dict.insert(signature, op);
}

fn eqinit_vecu64_columnu8(
    output: &mut ColumnWrapper,
    input: Vec<InputTypes>,
) -> Result<(), ErrorDesc> {
    type T1 = ColumnU8;
    type T2 = ColumnU8;

    //naming convention:
    // left->output
    //right[0]-->input
    //if right[0] and right[1]-> input_lhs, input_rhs

    let (data_output, index_output, bitmap_output) = output.all_mut::<Vec<bool>>()?;

    let (data_input_lhs, index_input_lhs, bitmap_input_lhs) = match &input[0] {
        InputTypes::Ref(a) => (a.downcast_ref::<T1>()?, a.index(), a.bitmap()),
        InputTypes::Owned(a) => (a.downcast_ref::<T1>()?, a.index(), a.bitmap()),
    };

    let (data_input_rhs, index_input_rhs, bitmap_input_rhs) = match &input[1] {
        InputTypes::Ref(a) => (a.downcast_ref::<T2>()?, a.index(), a.bitmap()),
        InputTypes::Owned(a) => (a.downcast_ref::<T2>()?, a.index(), a.bitmap()),
    };

    let len_input_rhs = if let Some(ind) = index_input_rhs {
        ind.len()
    } else {
        data_input_rhs.len.len()
    };

    let len_input_lhs = if let Some(ind) = index_input_lhs {
        ind.len()
    } else {
        data_input_lhs.len.len()
    };

    //The two input columns should have the same length
    if len_input_rhs != len_input_lhs {
        Err(format!(
                        "The two input columns should have the same length, but they are {} and {} respectively",
                        len_input_lhs, len_input_rhs
                    ))?
    };

    //Clean up
    data_output.truncate(0);
    *index_output = None;
    *bitmap_output = None;
    //Reserve enough storage for result
    data_output.reserve(len_input_lhs);

    let bits_new = if (bitmap_input_lhs.is_some()) || (bitmap_input_rhs.is_some()) {
        let mut v: Vec<u8> = Vec::new();

        if let Some(bitm_lhs) = &bitmap_input_lhs {
            if let Some(ind_lhs) = &index_input_lhs {
                v.extend(ind_lhs.iter().map(|i| bitm_lhs[*i]))
            } else {
                v.extend(bitm_lhs.iter())
            }
        };
        if v.len() == 0 {
            if let Some(bitm_rhs) = &bitmap_input_rhs {
                if let Some(ind_rhs) = &index_input_rhs {
                    v.extend(ind_rhs.iter().map(|i| bitm_rhs[*i]))
                } else {
                    v.extend(bitm_rhs.iter())
                }
            };
        } else {
            if let Some(bitm_rhs) = &bitmap_input_rhs {
                if let Some(ind_rhs) = &index_input_rhs {
                    v.iter_mut()
                        .zip(ind_rhs.iter())
                        .for_each(|(b, i)| *b &= bitm_rhs[*i])
                } else {
                    v.iter_mut()
                        .zip(bitm_rhs.iter())
                        .for_each(|(b, bl)| *b &= *bl)
                }
            };
        }

        Some(v)
    } else {
        None
    };

    let slice_len_lhs = &data_input_lhs.len;
    let slice_start_pos_lhs = &data_input_lhs.start_pos;
    let data_lhs = &data_input_lhs.data;

    let slice_len_rhs = &data_input_rhs.len;
    let slice_start_pos_rhs = &data_input_rhs.start_pos;
    let data_rhs = &data_input_rhs.data;

    match (index_input_lhs, index_input_rhs, &bits_new) {
        (None, None, None) => {
            data_output.extend(
                slice_start_pos_lhs
                    .iter()
                    .zip(slice_len_lhs.iter())
                    .zip(slice_start_pos_rhs.iter().zip(slice_len_rhs.iter()))
                    .map(|((ls, ll), (rs, rl))| {
                        data_lhs[*ls..*ls + *ll].eq(&data_rhs[*rs..*rs + *rl])
                    }),
            );
        }
        (Some(ind_lhs), None, None) => {
            data_output.extend(
                ind_lhs
                    .iter()
                    .zip(slice_start_pos_rhs.iter().zip(slice_len_rhs.iter()))
                    .map(|(li, (rs, rl))| {
                        data_lhs[slice_start_pos_lhs[*li]
                            ..slice_start_pos_lhs[*li] + slice_len_lhs[*li]]
                            .eq(&data_rhs[*rs..*rs + *rl])
                    }),
            );
        }
        (None, Some(ind_rhs), None) => {
            data_output.extend(
                slice_start_pos_lhs
                    .iter()
                    .zip(slice_len_lhs.iter())
                    .zip(ind_rhs.iter())
                    .map(|((ls, ll), ri)| {
                        data_lhs[*ls..*ls + *ll].eq(&data_rhs[slice_start_pos_rhs[*ri]
                            ..slice_start_pos_rhs[*ri] + slice_len_rhs[*ri]])
                    }),
            );
        }
        (Some(ind_lhs), Some(ind_rhs), None) => {
            data_output.extend(ind_lhs.iter().zip(ind_rhs.iter()).map(|(li, ri)| {
                data_lhs[slice_start_pos_lhs[*li]..slice_start_pos_lhs[*li] + slice_len_lhs[*li]]
                    .eq(&data_rhs
                        [slice_start_pos_rhs[*ri]..slice_start_pos_rhs[*ri] + slice_len_rhs[*ri]])
            }));
        }

        (None, None, Some(bits)) => {
            data_output.extend(
                slice_start_pos_lhs
                    .iter()
                    .zip(slice_len_lhs.iter())
                    .zip(slice_start_pos_rhs.iter().zip(slice_len_rhs.iter()))
                    .zip(bits.iter())
                    .map(|(((ls, ll), (rs, rl)), b)| {
                        if *b != 0 {
                            data_lhs[*ls..*ls + *ll].eq(&data_rhs[*rs..*rs + *rl])
                        } else {
                            Default::default()
                        }
                    }),
            );
        }
        (Some(ind_lhs), None, Some(bits)) => {
            data_output.extend(
                ind_lhs
                    .iter()
                    .zip(slice_start_pos_rhs.iter().zip(slice_len_rhs.iter()))
                    .zip(bits.iter())
                    .map(|((li, (rs, rl)), b)| {
                        if *b != 0 {
                            data_lhs[slice_start_pos_lhs[*li]
                                ..slice_start_pos_lhs[*li] + slice_len_lhs[*li]]
                                .eq(&data_rhs[*rs..*rs + *rl])
                        } else {
                            Default::default()
                        }
                    }),
            );
        }
        (None, Some(ind_rhs), Some(bits)) => {
            data_output.extend(
                slice_start_pos_lhs
                    .iter()
                    .zip(slice_len_lhs.iter())
                    .zip(ind_rhs.iter())
                    .zip(bits.iter())
                    .map(|(((ls, ll), ri), b)| {
                        if *b != 0 {
                            data_lhs[*ls..*ls + *ll].eq(&data_rhs[slice_start_pos_rhs[*ri]
                                ..slice_start_pos_rhs[*ri] + slice_len_rhs[*ri]])
                        } else {
                            Default::default()
                        }
                    }),
            );
        }
        (Some(ind_lhs), Some(ind_rhs), Some(bits)) => {
            data_output.extend(ind_lhs.iter().zip(ind_rhs.iter()).zip(bits.iter()).map(
                |((li, ri), b)| {
                    if *b != 0 {
                        data_lhs[slice_start_pos_lhs[*li]
                            ..slice_start_pos_lhs[*li] + slice_len_lhs[*li]]
                            .eq(&data_rhs[slice_start_pos_rhs[*ri]
                                ..slice_start_pos_rhs[*ri] + slice_len_rhs[*ri]])
                    } else {
                        Default::default()
                    }
                },
            ));
        }
    };

    if let Some(bits) = bits_new {
        *bitmap_output = Some(Bitmap { bits })
    };
    Ok(())
}
