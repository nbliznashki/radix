use crate::expressions::initdictionary::InitDictionary;
use crate::expressions::opdictionary::OpDictionary;

pub mod add;
pub mod addassign;
pub mod addhash;
pub mod mulassign;

pub fn load_op_dict(dict: &mut OpDictionary) {
    addassign::load_op_dict(dict);
    mulassign::load_op_dict(dict);
    addhash::load_op_dict(dict);
    add::load_op_dict(dict);
}
