use crocksdb_ffi::{
    self, DBFileChecksumContext, DBFileChecksumGenerator, DBFileChecksumGeneratorFactory,
};
use libc::{c_char, c_int, c_void, malloc, memcpy, size_t};
use std::ffi::CString;
use std::{ptr, slice, usize};


