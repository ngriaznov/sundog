//! Arbitrary bytes into the CRDT record decoders always return `Ok` or the
//! codec's error, never a panic. On a successful decode, also exercises the
//! decoded value's read accessors, its own encode/decode round trip, and
//! merge idempotence: `PnCounter::decode` refuses the one shape (a writer
//! both live and retired) whose first self-merge would change its bytes, so
//! a decoded counter merges with itself to itself; an `OrSet` keeps every
//! add it holds through a self-merge whatever its watermarks say.
#![no_main]

use libfuzzer_sys::fuzz_target;
use sundog::crdt::{OrSet, PnCounter};

fuzz_target!(|data: &[u8]| {
    if let Ok(counter) = PnCounter::decode(data) {
        let _ = counter.value();

        let encoded = counter
            .encode()
            .expect("a decoded PnCounter always re-encodes");
        let redecoded = PnCounter::decode(&encoded).expect("a re-encoded PnCounter always decodes");
        assert_eq!(
            redecoded, counter,
            "decode(encode(x)) must round-trip to the same PnCounter"
        );

        let once = counter.merge(&counter);
        assert_eq!(once, counter, "merging a decoded PnCounter with itself is a no-op");
        assert_eq!(once.value(), counter.value());
    }

    if let Ok(set) = OrSet::<String>::decode(data) {
        for element in set.iter() {
            assert!(
                set.contains(element),
                "every element yielded by iter() must also satisfy contains()"
            );
        }

        let encoded = set.encode().expect("a decoded OrSet always re-encodes");
        let redecoded =
            OrSet::<String>::decode(&encoded).expect("a re-encoded OrSet always decodes");
        assert_eq!(
            redecoded, set,
            "decode(encode(x)) must round-trip to the same OrSet"
        );

        let once = set.merge(&set);
        assert_eq!(once, set, "merging a decoded OrSet with itself is a no-op");
    }
});
