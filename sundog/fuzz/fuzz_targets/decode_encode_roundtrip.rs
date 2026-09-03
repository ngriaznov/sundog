//! Any frame the decoder accepts re-encodes, and decoding that re-encoding
//! yields the same message.

#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(msg) = sundog::wire::decode(&Bytes::copy_from_slice(data)) else {
        return;
    };
    let encoded = sundog::wire::encode(&msg).expect("a decoded message re-encodes");
    let redecoded = sundog::wire::decode(&encoded).expect("a re-encoded frame decodes");
    assert_eq!(msg, redecoded, "decode/encode/decode is a fixed point");
});
