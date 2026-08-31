//! Semantic consistency behind the panic check: any frame the decoder
//! accepts must re-encode, and decoding that re-encoding must yield the
//! same message — otherwise two nodes could read one frame differently.

#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let Ok(msg) = sundog::wire::decode(&Bytes::copy_from_slice(data)) else {
        return;
    };
    let encoded = sundog::wire::encode(&msg).expect("a decoded message must re-encode");
    let redecoded = sundog::wire::decode(&encoded).expect("a re-encoded frame must decode");
    assert_eq!(msg, redecoded, "decode/encode/decode must be a fixed point");
});
