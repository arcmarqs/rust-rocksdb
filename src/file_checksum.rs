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
    fn update(&mut self, data: &[u8] ) {
    }

    /// Finalize the Hasher and convert the digest to string
    fn finalize(&mut self, _checksum_str: &mut str) {
    }
    
    /// Return checksum string
    fn get_checksum(&self) -> String {
    }
}

#[repr(C)]
struct FileChecksumGenProxy<H : FileChecksumGenerator> {
    checksum_func_name_: CString,
    checksum_generator: H
}

extern "C" fn name<H: FileChecksumGenerator>(checksum: *mut c_void) -> *const c_char {
    unsafe { (*(checksum as *mut FileChecksumGenProxy<H>)).name.as_ptr() }
}

extern "C" fn destructor<H: FileChecksumGenerator>(filter: *mut c_void) {
    unsafe {
        let _ = Box::from_raw(checksum as *mut FileChecksumGenProxy<H>);
    }
}

extern "C" fn update<H: FileChecksumGenerator>(data: *const u8, size: size_t) {
    unsafe {
        let mut checksum_gen = &mut (*(checksum as *mut FileChecksumGenProxy<h>)).checksum_generator;
        let data = slice::from_raw_parts(data,size);
        checksum_gen.update(data);
    }
}

extern "C" fn finalize<H: FileChecksumGenerator>(){
    unsafe {
        let mut checksum_gen = &mut (*(checksum as *mut FileChecksumGenProxy<h>)).checksum_generator;
        checksum_gen.finalize();
    }
}

extern "C" fn get_checksum<H: FileChecksumGenerator>() -> *const c_char {
    unsafe {
        (*(checksum_gen as *mut FileChecksumGenProxy<H>)).get_checksum();
    }
}

pub struct FileChecksumGenHandler {
    inner: *mut DBFileChecksumGenerator,
}

impl Drop for FileChecksumGenHandle {
    fn drop(&mut self) {
        unsafe {
            crocksdb_ffi::crocksdb_file_checksum_gen_destroy(self.inner);
        }
    }
}

pub unsafe fn new_checksum_generator<H: FileChecksumGenerator>(
    checksum_func_name: CString,
    generator: H,
) -> FileChecksumGenHandle {
    let filter = new_compaction_filter_raw(checksum_func_name, generator);
    FileChecksumGenHandle { inner: generator }
}

