use crate::column::column::{Column, ColumnMut};

use std::any::{Any, TypeId};

pub type Operation = fn(&mut dyn Any, Vec<&dyn Any>);
