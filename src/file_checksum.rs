use crocksdb_ffi::{
    self, DBFileChecksumContext, DBFileChecksumGeneratorFactory,
};
pub use crocksdb_ffi::DBFileChecksumGenerator;
use libc::{c_char, c_int, c_void, malloc, memcpy, size_t};
use std::ffi::CString;
use std::{ptr, slice, usize};

/// Full file checksums are used for additional protection when replicating or storing SSTS remotely. 
/// It includes additional metadata when compared to per block checksums.
pub trait FileChecksumGenerator {
    
    /// Update the checksum with a hash function;
    fn update(&mut self, data: &[u8] );

    /// Finalize the Hasher and convert the digest to string
    fn finalize(&mut self, _checksum_str: &mut str);
    
    /// Return checksum string
    fn get_checksum(&self) -> String;
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
    let filter = new_file_checksum_gen_raw(checksum_func_name, generator);
    FileChecksumGenHandle { inner: generator }
}

unsafe fn new_file_checksum_gen_raw<H: FileChecksumGenerator>(
    checksum_func_name: CString,
    generator: H,
) -> *mut DBFileChecksumGenerator {
    let proxy = Box::into_raw(Box::new(FileChecksumGen {
        checksum_func_name_: checksum_func_name,
        checksum_generator: generator,
    }));
    crocksdb_ffi::crocksdb_file_checksum_generator_create(
        proxy as *mut c_void,
        destructor::<H>,
        update::<H>,
        finalize::<H>,
        get_checksum::<H>,
        name::<H>,
    )
}

pub struct FileChecksumContext(DBFileChecksumContext);

impl FileChecksumContext {
    pub fn file_name() -> String {
        let ctx = &self.0 as *const DBFileChecksumContext;
        let mut data;
        unsafe {data = crocksdb_ffi::crocksdb_file_checksum_gen_context_file_name(ctx);}

    }
}

pub trait FileChecksumGenFactory {
    type FilechecksumGenHandle;
    
    fn create_file_checksum_generator(&self, context: &mut FileChecksumContext) -> Option<(CString,Self::Filter)>;
}

#[repr(C)]
struct FileChecksumGenFactoryProxy<F:FileChecksumGenFactory> {
    name: CString,
    factory: F
}

mod factory {
    use super::{FileChecksumGenerator,FileChecksumGenFactory,FileChecksumContext};
    use crocksdb_ffi::{DBFileChecksumGenerator, DBFileChecksumContext};
    use libc::{c_char, c_uchar, c_void};

    pub(super) extern "C" fn name<F: FileChecksumGenFactory>(
        factory: *mut c_void,
    ) -> *const c_char {
        unsafe {
            let proxy = &*(factory as *mut FileChecksumGenFactoryProxy<F>);
            proxy.name.as_ptr()
        }
    }

    pub(super) extern "C" fn destructor<F: FileChecksumGenFactory>(factory: *mut c_void) {
        unsafe {
            let _ = Box::from_raw(factory as *mut FileChecksumGenFactoryProxy<F>);
        }
    }

    pub(super) extern "C" fn create_compaction_filter<F: FileChecksumGenFactory>(
        factory: *mut c_void,
        context: *const DBFileChecksumContext,
    ) -> *mut DBFileChecksumGenerator {
        unsafe {
            let factory = &mut *(factory as *mut FileChecksumGenFactoryProxy<F>);
            let context: &FileChecksumContext = &*(context as *const FileChecksumContext);
            if let Some((name, filter)) = factory.factory.create_file_checksum_generator(context) {
                super::file_checksum_gen_raw(name, filter)
            } else {
                std::ptr::null_mut()
            }
        }
    }

}

pub struct FileChecksumFactoryHandle {
    pub(crate) inner:*mut DBFileChecksumGeneratorFactory,
}

impl Drop for FileChecksumFactoryHandle {
    fn drop(&mut self) {
        unsafe {
            crocksdb_ffi::crocksdb_file_checksum_gen_factory_destroy(self.inner);
        }
    }
}

pub unsafe fn new_file_checksum_gen_factory<F: FileChecksumGenFactory>(name: CString, factory: F) -> Result<FileChecksumFactoryHandle,String> {
    let proxy = Box::into_raw(Box::new(FileChecksumGenFactoryProxy {
        name: c_name,
        factory: f,
    }));

    let factory = crocksdb::crocksdb_file_checksum_gen_factory_create(
        proxy as *mut c_void,
        self::factory::destructor::<F>,
        self::factory::create_file_checksum_generator::<F>,
        self::factory::name::<F>,
    );

    Ok(FileChecksumGenFactoryHandle { inner: factory })
}

