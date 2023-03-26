pub use crocksdb_ffi::DBFileChecksumGenerator;
use crocksdb_ffi::{self, DBFileChecksumContext, DBFileChecksumGeneratorFactory};
use libc::{c_char,c_void, size_t};
use std::ffi::{CString, CStr};
use std::slice;

/// Full file checksums are used for additional protection when replicating or storing SSTS remotely.
/// It includes additional metadata when compared to per block checksums.
pub trait FileChecksumGenerator {
    /// Update the checksum with a hash function;
    fn update(&mut self, data: &[u8]);

    /// Finalize the Hasher and convert the digest to string
    fn finalize(&mut self);

    /// Return checksum string
    fn get_checksum(&self) -> CString;
}

#[repr(C)]
struct FileChecksumGenProxy<H: FileChecksumGenerator> {
    checksum_func_name_: CString,
    checksum_generator: H,
}

extern "C" fn name<H: FileChecksumGenerator>(checksum: *mut c_void) -> *const c_char {
    unsafe { (*(checksum as *mut FileChecksumGenProxy<H>)).checksum_func_name_.as_ptr() }
}

extern "C" fn destructor<H: FileChecksumGenerator>(checksum: *mut c_void) {
    unsafe {
        let _ = Box::from_raw(checksum as *mut FileChecksumGenProxy<H>);
    }
}

extern "C" fn update<H: FileChecksumGenerator>(checksum: *mut c_void, data: *const u8, size: size_t) {
    unsafe {
        let checksum_gen =
            &mut (*(checksum as *mut FileChecksumGenProxy<H>)).checksum_generator;
        let data = slice::from_raw_parts(data, size);
        checksum_gen.update(data);
    }
}

extern "C" fn finalize<H: FileChecksumGenerator>(checksum:*mut c_void ) {
    unsafe {
        let checksum_gen =
            &mut (*(checksum as *mut FileChecksumGenProxy<H>)).checksum_generator;
        checksum_gen.finalize();
    }
}

extern "C" fn get_checksum<H: FileChecksumGenerator>(checksum: *mut c_void) -> *const c_char {
    unsafe {
        let checksum_gen = & mut (*(checksum as *mut FileChecksumGenProxy<H>)).checksum_generator;
        checksum_gen.get_checksum().as_ptr()
    }
}

pub struct FileChecksumGenHandle {
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
    let checksum_gen = new_file_checksum_gen_raw(checksum_func_name, generator);
    FileChecksumGenHandle { inner: checksum_gen }
}

unsafe fn new_file_checksum_gen_raw<H: FileChecksumGenerator>(
    checksum_func_name: CString,
    generator: H,
) -> *mut DBFileChecksumGenerator {
    let proxy = Box::into_raw(Box::new(FileChecksumGenProxy {
        checksum_func_name_: checksum_func_name,
        checksum_generator: generator,
    }));
    crocksdb_ffi::crocksdb_file_checksum_gen_create(
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
    pub fn file_name(&self) -> String {
        let ctx = &self.0 as *const DBFileChecksumContext;
        unsafe {
            let data = crocksdb_ffi::crocksdb_file_checksum_gen_context_file_name(ctx);
            CStr::from_ptr(data).to_string_lossy().into_owned()
        }
    }

    pub fn checksum_func_name(&self) -> String {
        let ctx = &self.0 as *const DBFileChecksumContext;
        unsafe {
            let data = crocksdb_ffi::crocksdb_file_checksum_gen_context_checksum_func_name(ctx);
            CStr::from_ptr(data).to_string_lossy().into_owned()
        }
    }
}

pub trait FileChecksumGenFactory {
    type ChecksumGen:FileChecksumGenerator;

    fn create_file_checksum_generator(
        &self,
        context: &FileChecksumContext,
    ) -> Option<(CString, Self::ChecksumGen)>;
}

#[repr(C)]
struct FileChecksumGenFactoryProxy<F: FileChecksumGenFactory> {
    name: CString,
    factory: F,
}

mod factory {
    use super::{FileChecksumContext, FileChecksumGenFactory, FileChecksumGenFactoryProxy};
    use crocksdb_ffi::{DBFileChecksumContext, DBFileChecksumGenerator};
    use libc::{c_char, c_void};

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

    pub(super) extern "C" fn create_file_checksum_generator<F: FileChecksumGenFactory>(
        factory: *mut c_void,
        context: *const DBFileChecksumContext,
    ) -> *mut DBFileChecksumGenerator {
        unsafe {
            let factory = &mut *(factory as *mut FileChecksumGenFactoryProxy<F>);
            let context: &FileChecksumContext = &*(context as *const FileChecksumContext);
            if let Some((name, generator)) = factory.factory.create_file_checksum_generator(context) {
                super::new_file_checksum_gen_raw(name, generator)
            } else {
                std::ptr::null_mut()
            }
        }
    }
}

pub struct FileChecksumFactoryHandle {
    pub(crate) inner: *mut DBFileChecksumGeneratorFactory,
}

impl Drop for FileChecksumFactoryHandle {
    fn drop(&mut self) {
        unsafe {
            crocksdb_ffi::crocksdb_file_checksum_gen_factory_destroy(self.inner);
        }
    }
}

pub unsafe fn new_file_checksum_gen_factory<F: FileChecksumGenFactory>(
    name: CString,
    factory: F,
) -> Result<FileChecksumFactoryHandle, String> {
    let proxy = Box::into_raw(Box::new(FileChecksumGenFactoryProxy {
        name: name,
        factory: factory,
    }));

    let factory = crocksdb_ffi::crocksdb_file_checksum_gen_factory_create(
        proxy as *mut c_void,
        self::factory::destructor::<F>,
        self::factory::create_file_checksum_generator::<F>,
        self::factory::name::<F>,
    );

    Ok(FileChecksumFactoryHandle { inner: factory })
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;
    use std::convert::TryFrom;
    use std::ffi::CStr;
    use std::hash::{Hasher};
    use std::{ffi::CString, hash::Hash};
    use std::str;


    use crate::{
        ColumnFamilyOptions,FileChecksumContext,FileChecksumGenerator,FileChecksumGenFactory,
        DBOptions, Writable, DB,
    };

    struct FileChecksumHashGen {
        hasher: DefaultHasher,
        checksum: u64,
        checksum_str: String,
        checksum_func_name: String,
    }

    impl FileChecksumGenerator for FileChecksumHashGen {
        fn update(&mut self, data: &[u8]) {
            data.hash(&mut self.hasher);
        }

        fn finalize(&mut self) {
            self.checksum = self.hasher.finish();
            self.checksum_str = self.checksum.to_string();
        }

        fn get_checksum(&self) -> CString {
            CString::new(self.checksum_str.clone()).unwrap()
        }
    }

    impl FileChecksumHashGen {
        fn new() -> Self {
            Self {
                hasher: DefaultHasher::new(),
                checksum: 0,
                checksum_str: "".to_string(),
                checksum_func_name: "CustomChecksumFunc".to_string(),
            }
        }
    }

    struct Factory {}
    impl FileChecksumGenFactory for Factory {
        type ChecksumGen = FileChecksumHashGen;

        fn create_file_checksum_generator(
        &self,
        context: &FileChecksumContext,
    ) -> Option<(CString, Self::ChecksumGen)> {
        Some((CString::new("CustomChecksumFunc").unwrap(), FileChecksumHashGen::new()))
    }
    }

    #[test]
    fn test_factory() {
        let mut opts = DBOptions::new();
        let factory = Factory {};
        let name = CString::new("CustomChecksumFuncFactory").unwrap();
        opts.create_if_missing(true);
        opts.file_checksum_gen_factory::<CString,Factory>(name,factory);

        let db = DB::open(opts, "./store").unwrap();

        db.put(b"1",b"2");
        db.flush(true);
    }
}
