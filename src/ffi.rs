use crate::{engine::sql, Client, Error};

use libc::{c_char, c_int, c_uint, c_ulonglong};
use std::{cell::RefCell, ffi::CStr, sync::Arc};

type ItemPointer = u64;

#[no_mangle]
pub extern "C" fn sq_init() {
    env_logger::init();
}

// error handling code borrowed from https://michael-f-bryan.github.io/rust-ffi-guide/errors/return_types.html
thread_local! {
    static LAST_ERROR: RefCell<Option<Box<Error>>> = RefCell::new(None);
}

fn update_last_error(err: Error) {
    LAST_ERROR.with(|prev| {
        *prev.borrow_mut() = Some(Box::new(err));
    });
}

fn take_last_error() -> Option<Box<Error>> {
    LAST_ERROR.with(|prev| prev.borrow_mut().take())
}

#[no_mangle]
pub extern "C" fn sq_last_error_length() -> c_int {
    LAST_ERROR.with(|prev| match *prev.borrow() {
        Some(ref err) => err.to_string().len() as c_int + 1,
        None => 0,
    })
}

#[no_mangle]
pub unsafe extern "C" fn sq_last_error_message(buffer: *mut c_char, length: c_int) -> c_int {
    if buffer.is_null() {
        return -1;
    }

    let last_error = match take_last_error() {
        Some(err) => err,
        None => return 0,
    };

    let error_message = last_error.to_string();

    let buffer = std::slice::from_raw_parts_mut(buffer as *mut u8, length as usize);

    if error_message.len() >= buffer.len() {
        return -1;
    }

    std::ptr::copy_nonoverlapping(
        error_message.as_ptr(),
        buffer.as_mut_ptr(),
        error_message.len(),
    );

    buffer[error_message.len()] = 0;

    error_message.len() as c_int
}

#[no_mangle]
pub extern "C" fn sq_create_db(addr: *const c_char) -> *const sql::Client {
    let addr = unsafe {
        assert!(!addr.is_null());
        CStr::from_ptr(addr)
    };
    let addr_str = addr.to_str().unwrap();
    let client = futures::executor::block_on(Client::new(addr_str)).map(|r| sql::Client::new(r));
    let client = match client {
        Ok(client) => client,
        Err(e) => {
            update_last_error(e);
            return std::ptr::null();
        }
    };
    Arc::into_raw(Arc::new(client))
}

#[no_mangle]
pub extern "C" fn sq_free_db(db: *const sql::Client) {
    if db.is_null() {
        return;
    }
    unsafe {
        Arc::from_raw(db);
    }
}

#[no_mangle]
pub extern "C" fn sq_start_transaction(
    db: *const sql::Client,
    _isolation_level: c_int,
) -> *mut sql::Transaction {
    let db = unsafe {
        assert!(!db.is_null());
        &*db
    };

    let txn = match db.begin() {
        Ok(txn) => txn,
        Err(e) => {
            update_last_error(e);
            return std::ptr::null_mut();
        }
    };

    Box::into_raw(Box::new(txn))
}

#[no_mangle]
pub extern "C" fn sq_free_transaction(_txn: *mut sql::Transaction) {}

#[no_mangle]
pub extern "C" fn sq_commit_transaction(_db: *const sql::Client, txn: *mut sql::Transaction) {
    let txn = unsafe {
        assert!(!txn.is_null());
        Box::from_raw(txn)
    };

    match txn.commit() {
        Ok(_) => {}
        Err(e) => {
            update_last_error(e);
        }
    }
}

#[no_mangle]
pub extern "C" fn sq_create_table(
    db: *const sql::Client,
    _db_id: u64,
    table_id: u64,
) -> *const sql::Table {
    let db = unsafe {
        assert!(!db.is_null());
        &*db
    };

    let table = match db.create_table(table_id) {
        Ok(table) => table,
        Err(e) => {
            update_last_error(e);
            return std::ptr::null();
        }
    };

    Box::into_raw(Box::new(table))
}

#[no_mangle]
pub extern "C" fn sq_open_table(
    db: *const sql::Client,
    _db_id: u64,
    table_id: u64,
) -> *const sql::Table {
    let db = unsafe {
        assert!(!db.is_null());
        &*db
    };

    let table = match db.open_table(table_id) {
        Ok(Some(table)) => table,
        Ok(None) => {
            return std::ptr::null();
        }
        Err(e) => {
            update_last_error(e);
            return std::ptr::null();
        }
    };

    Box::into_raw(Box::new(table))
}

#[no_mangle]
pub extern "C" fn sq_free_table(table: *const sql::Table) {
    if table.is_null() {
        return;
    }
    unsafe {
        Box::from_raw(table as *mut sql::Table);
    }
}

#[no_mangle]
pub extern "C" fn sq_table_get_file_size(
    _table: *const sql::Table,
    _db: *const sql::Client,
) -> c_ulonglong {
    0 as c_ulonglong
}

#[no_mangle]
pub extern "C" fn sq_table_insert_tuple(
    table: *const sql::Table,
    _db: *const sql::Client,
    txn: *const sql::Transaction,
    data: *const u8,
    len: u64,
) -> *const ItemPointer {
    let table = unsafe {
        assert!(!table.is_null());
        &*table
    };
    let txn = unsafe {
        assert!(!txn.is_null());
        &*txn
    };

    let tuple = unsafe { std::slice::from_raw_parts(data, len as usize) };

    let item_pointer = match txn.insert_tuple(table, tuple) {
        Ok(ptr) => ptr,
        Err(e) => {
            update_last_error(e);
            return std::ptr::null();
        }
    };

    Box::into_raw(Box::new(item_pointer))
}

#[no_mangle]
pub extern "C" fn sq_free_item_pointer(pointer: *const ItemPointer) {
    if pointer.is_null() {
        return;
    }
    unsafe {
        Box::from_raw(pointer as *mut ItemPointer);
    }
}

#[no_mangle]
pub extern "C" fn sq_table_begin_scan<'a>(
    table: *const sql::Table,
    _db: *const sql::Client,
    txn: *mut sql::Transaction,
) -> *mut sql::TableScan {
    let table = unsafe {
        assert!(!table.is_null());
        &*table
    };
    let txn = unsafe {
        assert!(!txn.is_null());
        &mut *txn
    };

    let scan = match txn.scan_table(table) {
        Ok(scan) => scan,
        Err(e) => {
            update_last_error(e);
            return std::ptr::null_mut();
        }
    };

    Box::into_raw(Box::new(scan))
}

#[no_mangle]
pub extern "C" fn sq_free_table_scan_iterator<'a>(scan: *mut sql::TableScan) {
    if scan.is_null() {
        return;
    }
    unsafe {
        Box::from_raw(scan);
    }
}

fn get_scan_direction(dir: c_int) -> sql::ScanDirection {
    if dir == 0 {
        sql::ScanDirection::Forward
    } else {
        sql::ScanDirection::Backward
    }
}

#[no_mangle]
pub extern "C" fn sq_table_scan_next<'a>(
    scan: *mut sql::TableScan,
    _db: *const sql::Client,
    dir: c_int,
) -> *const Vec<u8> {
    let scan = unsafe {
        assert!(!scan.is_null());
        &mut *scan
    };
    let dir = get_scan_direction(dir);

    let tuple = match match dir {
        sql::ScanDirection::Forward => scan.next(),
        sql::ScanDirection::Backward => scan.next_back(),
    } {
        Some(Ok(tuple)) => tuple,
        Some(Err(e)) => {
            update_last_error(e);
            return std::ptr::null();
        }
        None => {
            return std::ptr::null();
        }
    };

    Box::into_raw(Box::new(tuple))
}

#[no_mangle]
pub extern "C" fn sq_free_tuple<'a>(tuple: *const Vec<u8>) {
    if tuple.is_null() {
        return;
    }
    unsafe {
        Box::from_raw(tuple as *mut Vec<u8>);
    }
}

#[no_mangle]
pub extern "C" fn sq_tuple_get_data_len<'a>(tuple: *const Vec<u8>) -> c_int {
    let tuple = unsafe {
        assert!(!tuple.is_null());
        &*tuple
    };

    tuple.len() as c_int
}

#[no_mangle]
pub unsafe extern "C" fn sq_tuple_get_data<'a>(
    tuple: *const Vec<u8>,
    buffer: *mut c_char,
    length: c_int,
) -> c_int {
    if buffer.is_null() {
        return -1;
    }

    let tuple = {
        assert!(!tuple.is_null());
        &*tuple
    };

    let buffer = std::slice::from_raw_parts_mut(buffer as *mut u8, length as usize);

    if tuple.len() > buffer.len() {
        return -1;
    }

    std::ptr::copy_nonoverlapping(tuple.as_ptr(), buffer.as_mut_ptr(), tuple.len());

    tuple.len() as c_int
}

#[no_mangle]
pub extern "C" fn sq_get_next_oid(db: *const sql::Client) -> u64 {
    let db = unsafe {
        assert!(!db.is_null());
        &*db
    };

    match db.get_next_oid() {
        Ok(oid) => oid,
        Err(e) => {
            update_last_error(e);
            0
        }
    }
}

#[no_mangle]
pub extern "C" fn sq_create_index(
    db: *const sql::Client,
    _db_id: u64,
    index_id: u64,
    _key_comparator_func: *const (),
) -> *const sql::Index {
    let db = unsafe {
        assert!(!db.is_null());
        &*db
    };

    let index = match db.create_index(index_id) {
        Ok(index) => index,
        Err(e) => {
            update_last_error(e);
            return std::ptr::null();
        }
    };

    Box::into_raw(Box::new(index))
}

#[no_mangle]
pub extern "C" fn sq_open_index(
    db: *const sql::Client,
    _db_id: u64,
    index_id: u64,
    _key_comparator_func: *const (),
) -> *const sql::Index {
    let db = unsafe {
        assert!(!db.is_null());
        &*db
    };

    let index = match db.open_index(index_id) {
        Ok(Some(index)) => index,
        Ok(None) => {
            return std::ptr::null();
        }
        Err(e) => {
            update_last_error(e);
            return std::ptr::null();
        }
    };

    Box::into_raw(Box::new(index))
}

#[no_mangle]
pub extern "C" fn sq_free_index(index: *const sql::Index) {
    if index.is_null() {
        return;
    }
    unsafe {
        Box::from_raw(index as *mut sql::Index);
    }
}

#[no_mangle]
pub extern "C" fn sq_index_insert(
    index: *const sql::Index,
    _db: *const sql::Client,
    txn: *const sql::Transaction,
    key: *const u8,
    length: c_int,
    item_pointer: *const ItemPointer,
) {
    let index = unsafe {
        assert!(!index.is_null());
        &*index
    };

    let txn = unsafe {
        assert!(!txn.is_null());
        &*txn
    };

    let item_pointer = unsafe {
        assert!(!item_pointer.is_null());
        *item_pointer
    };

    let key = unsafe { std::slice::from_raw_parts(key, length as usize) };

    match txn.insert_index(index, key, item_pointer) {
        Ok(_) => {}
        Err(e) => {
            update_last_error(e);
        }
    };
}

#[no_mangle]
pub extern "C" fn sq_index_begin_scan<'a>(
    index: *const sql::Index,
    _db: *const sql::Client,
    txn: *mut sql::Transaction,
    table: *const sql::Table,
) -> *mut sql::IndexScan<'a> {
    let index = unsafe {
        assert!(!index.is_null());
        &*index
    };
    let txn = unsafe {
        assert!(!txn.is_null());
        &mut *txn
    };
    let table = unsafe {
        assert!(!table.is_null());
        &*table
    };

    let scan = match txn.scan_index(index, table) {
        Ok(scan) => scan,
        Err(e) => {
            update_last_error(e);
            return std::ptr::null_mut();
        }
    };

    Box::into_raw(Box::new(scan))
}

#[no_mangle]
pub extern "C" fn sq_free_index_scan_iterator<'a>(scan: *mut sql::IndexScan<'a>) {
    if scan.is_null() {
        return;
    }
    unsafe {
        Box::from_raw(scan);
    }
}

#[no_mangle]
pub extern "C" fn sq_index_rescan<'a>(
    scan: *mut sql::IndexScan<'a>,
    _db: *const sql::Client,
    _start_key: *const u8,
    _length: c_int,
    predicate_func: *const (),
) {
    let scan = unsafe {
        assert!(!scan.is_null());
        &mut *scan
    };

    let predicate_func: extern "C" fn(*const u8, c_uint) -> c_int =
        unsafe { std::mem::transmute(predicate_func) };

    let predicate = sql::IndexScanPredicate::new(move |a: &[u8]| {
        let result = predicate_func(a.as_ptr(), a.len() as c_uint);

        match result {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(Error::InvalidArgument(
                "cannot match keys with predicates".to_owned(),
            )),
        }
    });

    match scan.rescan(predicate) {
        Ok(_) => {}
        Err(e) => {
            update_last_error(e);
        }
    };
}

#[no_mangle]
pub extern "C" fn sq_index_scan_next<'a>(
    scan: *mut sql::IndexScan<'a>,
    _db: *const sql::Client,
    dir: c_int,
) -> *const Vec<u8> {
    let scan = unsafe {
        assert!(!scan.is_null());
        &mut *scan
    };

    let tuple = match scan.next(get_scan_direction(dir)) {
        Ok(Some(tuple)) => tuple,
        Ok(None) => {
            return std::ptr::null();
        }
        Err(e) => {
            update_last_error(e);
            return std::ptr::null();
        }
    };

    Box::into_raw(Box::new(tuple))
}
