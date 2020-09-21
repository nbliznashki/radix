use crate::expressions::opdictionary::OpDictionary;

pub mod addassign;
pub mod addinit;
pub mod eqinit;
pub mod eqinitu8;
pub mod gteqinit;
pub mod gtinit;
pub mod hash;
pub mod hashinit;
pub mod hashu8;
//pub mod mulassign;

pub fn load_op_dict(dict: &mut OpDictionary) {
    addassign::load_op_dict(dict);
    // mulassign::load_op_dict(dict);
    hash::load_op_dict(dict);
    hashinit::load_op_dict(dict);
    hashu8::load_op_dict(dict);
    addinit::load_op_dict(dict);
    eqinit::load_op_dict(dict);
    eqinitu8::load_op_dict(dict);
    gteqinit::load_op_dict(dict);
    gtinit::load_op_dict(dict);
}
