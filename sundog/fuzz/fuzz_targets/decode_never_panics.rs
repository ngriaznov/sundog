//! Arbitrary bytes into the frame decoder: whatever the network sends, the
//! decoder returns `Ok` or a `CodecError` — it never panics.

#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = sundog::wire::decode(&Bytes::copy_from_slice(data));
});
