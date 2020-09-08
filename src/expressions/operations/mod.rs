use crate::expressions::dictionary::Dictionary;

pub mod addassign;
pub mod addhash;
pub mod mulassign;

pub fn init_dict(dict: &mut Dictionary) {
    addassign::init_dict(dict);
    mulassign::init_dict(dict);
    addhash::init_dict(dict);
}
