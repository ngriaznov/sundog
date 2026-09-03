//! Arbitrary bytes into the frame decoder always return `Ok` or `CodecError`, never a panic.

#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = sundog::wire::decode(&Bytes::copy_from_slice(data));
});
