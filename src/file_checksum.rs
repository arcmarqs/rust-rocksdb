use crocksdb_ffi::{
    self, DBFileChecksumContext, DBFileChecksumGenerator, DBFileChecksumGeneratorFactory,
};
use libc::{c_char, c_int, c_void, malloc, memcpy, size_t};
use std::ffi::CString;
use std::{ptr, slice, usize};

/// Full file checksums are used for additional protection when replicating or storing SSTS remotely. 
/// It includes additional metadata when compared to per block checksums.
pub trait FileChecksumGenerator {
    
    /// Update the checksum with a hash function;
    fn update(_checksum: &mut [u8],_data: &[u8], _size: usize) {
        _checksum = 0;
    }

    fn finalize(_checksum: &mut [u8], _checksum_str: &mut str) {
        _checksum_str = String::from(_checksum);
    }
    
    
}

#[repr(C)]
struct FileChecksumGenProxy<H : FileChecksumGenerator> {
    checksum_func_name_: CString,
    checksum_: c_void,
    checksum_str: CString,
    checksum_generator: H
}

extern "C" fn name<H: Checksum>(checksum: *mut c_void) -> *const c_char {
    unsafe { (*(checksum as *mut FileChecksumGenProxy<H>)).name.as_ptr() }
}

extern "C" fn destructor<H: checksum>(filter: *mut c_void) {
    unsafe {
        let _ = Box::from_raw(checksum as *mut FileChecksumGenProxy<H>);
    }
}

extern "C" fn update<H: FileChecksumGenerator>(_checksum: &mut [u8],_data: &[u8], _size: size_t) {
    unsafe {

    }
}