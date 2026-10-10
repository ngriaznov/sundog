//! A cache key typed as text, and the postcard bytes a cache hashes it as.
//!
//! A cache routes a key by the hash of its postcard encoding, and the lens
//! knows no cache's key type, so a person says which encoding to hash:
//!
//! | Text | Key |
//! |---|---|
//! | `k17` | a `String`: a varint length, then UTF-8 |
//! | `uint:N` | a `u16`, `u32` or `u64`: one LEB128 varint |
//! | `int:N` | an `i16`, `i32` or `i64`: one zigzag varint |
//! | `hex:036b` | the postcard bytes `03 6b` verbatim, for any other key type |
//! | `str:..` | a `String` whose text starts with a prefix |
//!
//! A key of another type lands on another part, so the answer is only as good
//! as the choice of prefix; [`KeySpec::hex`] states the bytes that were
//! hashed. [`printable`] makes any text safe to draw.

use std::fmt;

use crate::ui::theme;

/// The most characters a typed key holds.
pub const MAX_CHARS: usize = 256;

/// Which encoding a typed key stands for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyKind {
    /// A `String`, written bare or after `str:`.
    Str,
    /// An unsigned integer, written after `uint:`.
    Uint,
    /// A signed integer, written after `int:`.
    Int,
    /// Postcard bytes, written after `hex:`.
    Hex,
}

impl KeyKind {
    /// The prefix that selects the kind: `str`, `uint`, `int` or `hex`.
    #[must_use]
    pub const fn token(self) -> &'static str {
        match self {
            Self::Str => "str",
            Self::Uint => "uint",
            Self::Int => "int",
            Self::Hex => "hex",
        }
    }

    /// The kind in words: `String`, `unsigned integer`, `signed integer` or
    /// `postcard bytes`.
    #[must_use]
    pub const fn describe(self) -> &'static str {
        match self {
            Self::Str => "String",
            Self::Uint => "unsigned integer",
            Self::Int => "signed integer",
            Self::Hex => "postcard bytes",
        }
    }
}

/// Why a typed key is not a key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyError {
    /// The text is longer than [`MAX_CHARS`].
    TooLong {
        /// The characters it has.
        chars: usize,
    },
    /// The text after `uint:` or `int:` is not a number.
    NotANumber(KeyKind),
    /// The text after `uint:` starts with a minus sign.
    NegativeUnsigned,
    /// The number after `uint:` or `int:` does not fit 64 bits.
    OutOfRange(KeyKind),
    /// Nothing follows `hex:`.
    EmptyHex,
    /// The text after `hex:` has an odd number of digits.
    OddHex {
        /// The digits it has.
        digits: usize,
    },
    /// The text after `hex:` holds a character that is not a hex digit.
    NotHex {
        /// The first such character.
        found: char,
    },
}

impl fmt::Display for KeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::TooLong { chars } => write!(
                f,
                "a key holds at most {MAX_CHARS} characters and this one has {chars}: shorten it"
            ),
            Self::NotANumber(KeyKind::Int) => {
                f.write_str("int: takes an optional minus sign and decimal digits, as in int:-7")
            }
            Self::NotANumber(_) => f.write_str("uint: takes decimal digits, as in uint:42"),
            Self::NegativeUnsigned => {
                f.write_str("uint: takes no sign: write a negative key as int:, as in int:-7")
            }
            Self::OutOfRange(KeyKind::Int) => {
                f.write_str("the number does not fit an i64: use hex: for a wider key")
            }
            Self::OutOfRange(_) => {
                f.write_str("the number does not fit a u64: use hex: for a wider key")
            }
            Self::EmptyHex => {
                f.write_str("hex: takes at least one pair of hex digits, as in hex:6b")
            }
            Self::OddHex { digits } => write!(
                f,
                "hex: takes pairs of hex digits and this has {digits}: add or drop a digit"
            ),
            Self::NotHex { found } => write!(
                f,
                "hex: takes the digits 0-9 and a-f and '{}' is not one: remove it",
                printable(&found.to_string())
            ),
        }
    }
}

impl std::error::Error for KeyError {}

/// A typed key and the postcard bytes it stands for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeySpec {
    kind: KeyKind,
    text: String,
    bytes: Vec<u8>,
}

impl KeySpec {
    /// Reads a typed key: bare text is a `String`, and `str:`, `uint:`,
    /// `int:` and `hex:` select the encoding. The empty text is the empty
    /// `String`.
    ///
    /// # Errors
    ///
    /// Returns a [`KeyError`] for text longer than [`MAX_CHARS`], for
    /// `uint:` or `int:` without a number that fits 64 bits, and for `hex:`
    /// without at least one whole pair of hex digits.
    pub fn parse(input: &str) -> Result<Self, KeyError> {
        let chars = input.chars().count();
        if chars > MAX_CHARS {
            return Err(KeyError::TooLong { chars });
        }
        if let Some(digits) = input.strip_prefix("uint:") {
            unsigned(digits)
        } else if let Some(digits) = input.strip_prefix("int:") {
            signed(digits)
        } else if let Some(digits) = input.strip_prefix("hex:") {
            hex_bytes(digits)
        } else {
            Ok(string(input.strip_prefix("str:").unwrap_or(input)))
        }
    }

    /// Which encoding the key stands for.
    #[must_use]
    pub const fn kind(&self) -> KeyKind {
        self.kind
    }

    /// The text after the prefix, as typed.
    #[must_use]
    pub fn text(&self) -> &str {
        &self.text
    }

    /// The postcard bytes a cache hashes: the same bytes `Cache` encodes a
    /// key of the matching type to.
    #[must_use]
    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// [`bytes`](Self::bytes) as lowercase hex pairs separated by a space:
    /// `03 6b 31 37`.
    #[must_use]
    pub fn hex(&self) -> String {
        self.bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<Vec<_>>()
            .join(" ")
    }
}

/// The key as the echo line writes it: `"k17" as String`, `42 as unsigned
/// integer`. A `String` shows through [`printable`].
impl fmt::Display for KeySpec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.kind {
            KeyKind::Str => write!(f, "\"{}\"", printable(&self.text))?,
            KeyKind::Uint | KeyKind::Int => f.write_str(&self.text)?,
            KeyKind::Hex => f.write_str(&self.hex())?,
        }
        write!(f, " as {}", self.kind.describe())
    }
}

/// `text` as a `String` key.
fn string(text: &str) -> KeySpec {
    KeySpec {
        kind: KeyKind::Str,
        text: text.to_owned(),
        bytes: encode(&text),
    }
}

/// `digits` as a `u64` key.
fn unsigned(digits: &str) -> Result<KeySpec, KeyError> {
    if digits.starts_with('-') {
        return Err(KeyError::NegativeUnsigned);
    }
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(KeyError::NotANumber(KeyKind::Uint));
    }
    let number: u64 = digits
        .parse()
        .map_err(|_| KeyError::OutOfRange(KeyKind::Uint))?;
    Ok(KeySpec {
        kind: KeyKind::Uint,
        text: digits.to_owned(),
        bytes: encode(&number),
    })
}

/// `digits`, with an optional minus sign, as an `i64` key.
fn signed(digits: &str) -> Result<KeySpec, KeyError> {
    let magnitude = digits.strip_prefix('-').unwrap_or(digits);
    if magnitude.is_empty() || !magnitude.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(KeyError::NotANumber(KeyKind::Int));
    }
    let number: i64 = digits
        .parse()
        .map_err(|_| KeyError::OutOfRange(KeyKind::Int))?;
    Ok(KeySpec {
        kind: KeyKind::Int,
        text: digits.to_owned(),
        bytes: encode(&number),
    })
}

/// `digits` as postcard bytes, two hex digits to the byte.
fn hex_bytes(digits: &str) -> Result<KeySpec, KeyError> {
    if digits.is_empty() {
        return Err(KeyError::EmptyHex);
    }
    if let Some(found) = digits.chars().find(|c| !c.is_ascii_hexdigit()) {
        return Err(KeyError::NotHex { found });
    }
    if !digits.len().is_multiple_of(2) {
        return Err(KeyError::OddHex {
            digits: digits.len(),
        });
    }
    let bytes = digits
        .as_bytes()
        .chunks(2)
        .map(|pair| (nibble(pair[0]) << 4) | nibble(pair[1]))
        .collect();
    Ok(KeySpec {
        kind: KeyKind::Hex,
        text: digits.to_owned(),
        bytes,
    })
}

/// The value of one ASCII hex digit; 0 for any other byte.
const fn nibble(digit: u8) -> u8 {
    match digit {
        b'0'..=b'9' => digit - b'0',
        b'a'..=b'f' => digit - b'a' + 10,
        b'A'..=b'F' => digit - b'A' + 10,
        _ => 0,
    }
}

/// The postcard encoding of `value`, which is how a cache encodes a key.
fn encode<T: serde::Serialize + ?Sized>(value: &T) -> Vec<u8> {
    postcard::to_stdvec(value).expect("a string or an integer encodes")
}

/// `text` with every character the interface cannot draw replaced by `·`: a
/// control character, and any character that is neither ASCII nor in the
/// glyph allowlist ([`theme::is_allowed`]).
#[must_use]
pub fn printable(text: &str) -> String {
    text.chars()
        .map(|c| {
            if c.is_control() || !theme::is_allowed(c) {
                '·'
            } else {
                c
            }
        })
        .collect()
}

/// The control line that asks a test node about this key, `explain <text>`.
///
/// A test node holds `String` keys and splits a line at spaces, so only a
/// non-empty `String` key of ASCII graphic characters is askable. Every other
/// kind is located but never asked: the node would explain the `String` of the
/// same text, a different key.
#[must_use]
pub fn request_line(key: &KeySpec) -> Option<String> {
    let askable = key.kind == KeyKind::Str
        && !key.text.is_empty()
        && key.text.chars().all(|c| c.is_ascii_graphic());
    askable.then(|| format!("explain {}", key.text))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The encoding a cache applies to a key of type `T`.
    fn postcard_of<T: serde::Serialize>(value: &T) -> Vec<u8> {
        postcard::to_stdvec(value).expect("a plain value encodes")
    }

    fn spec(input: &str) -> KeySpec {
        KeySpec::parse(input).unwrap_or_else(|error| panic!("{input:?} parses: {error}"))
    }

    #[test]
    fn keyspec_table() {
        // (input, kind, text, bytes, echo)
        let ok: [(&str, KeyKind, &str, &[u8], &str); 12] = [
            (
                "k17",
                KeyKind::Str,
                "k17",
                &[3, b'k', b'1', b'7'],
                "\"k17\" as String",
            ),
            ("", KeyKind::Str, "", &[0], "\"\" as String"),
            ("é", KeyKind::Str, "é", &[2, 0xC3, 0xA9], "\"·\" as String"),
            // Spaces are kept.
            (
                " a b ",
                KeyKind::Str,
                " a b ",
                &[5, b' ', b'a', b' ', b'b', b' '],
                "\" a b \" as String",
            ),
            // `str:` is the escape for a String that starts with a prefix.
            (
                "str:uint:1",
                KeyKind::Str,
                "uint:1",
                &[6, b'u', b'i', b'n', b't', b':', b'1'],
                "\"uint:1\" as String",
            ),
            (
                "str:k1",
                KeyKind::Str,
                "k1",
                &[2, b'k', b'1'],
                "\"k1\" as String",
            ),
            ("uint:0", KeyKind::Uint, "0", &[0], "0 as unsigned integer"),
            (
                "uint:300",
                KeyKind::Uint,
                "300",
                &[0xAC, 0x02],
                "300 as unsigned integer",
            ),
            (
                "uint:007",
                KeyKind::Uint,
                "007",
                &[7],
                "007 as unsigned integer",
            ),
            ("int:-1", KeyKind::Int, "-1", &[1], "-1 as signed integer"),
            ("int:7", KeyKind::Int, "7", &[14], "7 as signed integer"),
            (
                "hex:036B",
                KeyKind::Hex,
                "036B",
                &[0x03, 0x6B],
                "03 6b as postcard bytes",
            ),
        ];
        for (input, kind, text, bytes, echo) in ok {
            let key = spec(input);
            assert_eq!(key.kind(), kind, "{input:?}");
            assert_eq!(key.text(), text, "{input:?}");
            assert_eq!(key.bytes(), bytes, "{input:?}");
            assert_eq!(key.to_string(), echo, "{input:?}");
        }
        assert_eq!(spec("k17").hex(), "03 6b 31 37");
        assert_eq!(spec("uint:300").hex(), "ac 02");
        assert_eq!(
            spec("uint:18446744073709551615").bytes().len(),
            10,
            "u64::MAX is a ten byte varint"
        );
        assert_eq!(spec("int:-9223372036854775808").bytes().len(), 10);

        let errors = [
            ("hex:", KeyError::EmptyHex),
            ("hex:abc", KeyError::OddHex { digits: 3 }),
            ("hex:0g", KeyError::NotHex { found: 'g' }),
            ("hex:03 6b", KeyError::NotHex { found: ' ' }),
            ("uint:", KeyError::NotANumber(KeyKind::Uint)),
            ("uint:4x", KeyError::NotANumber(KeyKind::Uint)),
            ("uint:+4", KeyError::NotANumber(KeyKind::Uint)),
            ("uint:-1", KeyError::NegativeUnsigned),
            (
                "uint:18446744073709551616",
                KeyError::OutOfRange(KeyKind::Uint),
            ),
            ("int:", KeyError::NotANumber(KeyKind::Int)),
            ("int:-", KeyError::NotANumber(KeyKind::Int)),
            ("int:--1", KeyError::NotANumber(KeyKind::Int)),
            ("int:+1", KeyError::NotANumber(KeyKind::Int)),
            (
                "int:9223372036854775808",
                KeyError::OutOfRange(KeyKind::Int),
            ),
        ];
        for (input, error) in errors {
            assert_eq!(KeySpec::parse(input), Err(error), "{input:?}");
        }
    }

    #[test]
    fn a_key_holds_at_most_256_characters() {
        let longest = "k".repeat(MAX_CHARS);
        assert_eq!(
            spec(&longest).bytes().len(),
            MAX_CHARS + 2,
            "a two byte length"
        );
        assert_eq!(
            KeySpec::parse(&"k".repeat(MAX_CHARS + 1)),
            Err(KeyError::TooLong { chars: 257 })
        );
        // Characters count, not bytes: 256 two-byte characters fit.
        assert_eq!(
            spec(&"é".repeat(MAX_CHARS)).bytes().len(),
            2 * MAX_CHARS + 2
        );
        // The cap covers the prefix too.
        let prefixed = format!("hex:{}", "00".repeat(MAX_CHARS / 2));
        assert_eq!(
            KeySpec::parse(&prefixed),
            Err(KeyError::TooLong {
                chars: MAX_CHARS + 4
            })
        );
    }

    #[test]
    fn u16_u32_u64_share_one_varint_and_i16_i32_i64_share_zigzag() {
        for n in [0u16, 1, 127, 128, 300, u16::MAX] {
            let key = spec(&format!("uint:{n}"));
            assert_eq!(key.bytes(), postcard_of(&n), "u16 {n}");
            assert_eq!(key.bytes(), postcard_of(&u32::from(n)), "u32 {n}");
            assert_eq!(key.bytes(), postcard_of(&u64::from(n)), "u64 {n}");
        }
        for n in [70_000u32, u32::MAX] {
            let key = spec(&format!("uint:{n}"));
            assert_eq!(key.bytes(), postcard_of(&n), "u32 {n}");
            assert_eq!(key.bytes(), postcard_of(&u64::from(n)), "u64 {n}");
        }
        assert_eq!(
            spec(&format!("uint:{}", u64::MAX)).bytes(),
            postcard_of(&u64::MAX)
        );
        for n in [0i16, 1, -1, 63, 64, -64, -65, 300, i16::MIN, i16::MAX] {
            let key = spec(&format!("int:{n}"));
            assert_eq!(key.bytes(), postcard_of(&n), "i16 {n}");
            assert_eq!(key.bytes(), postcard_of(&i32::from(n)), "i32 {n}");
            assert_eq!(key.bytes(), postcard_of(&i64::from(n)), "i64 {n}");
        }
        for n in [i32::MIN, i32::MAX] {
            let key = spec(&format!("int:{n}"));
            assert_eq!(key.bytes(), postcard_of(&n), "i32 {n}");
            assert_eq!(key.bytes(), postcard_of(&i64::from(n)), "i64 {n}");
        }
        // A u8 and an i8 are one raw byte, not a varint: `hex:` reaches them.
        assert_eq!(spec("hex:ff").bytes(), postcard_of(&u8::MAX));
        assert_eq!(spec("hex:80").bytes(), postcard_of(&i8::MIN));
        assert_ne!(spec("uint:255").bytes(), postcard_of(&u8::MAX));
        // A String is its length as a varint, then its UTF-8.
        for text in ["", "k1", "é", &"a".repeat(127), &"a".repeat(128)] {
            assert_eq!(
                spec(text).bytes(),
                postcard_of(&text.to_owned()),
                "{text:?}"
            );
        }
    }

    #[test]
    fn printable_replaces_glyphs_outside_the_allowlist() {
        assert_eq!(printable("k17 user:42 ~!"), "k17 user:42 ~!");
        // Control characters, tabs and newlines.
        assert_eq!(printable("a\tb\nc\u{1b}[31m\u{7f}"), "a·b·c·[31m·");
        // Characters outside ASCII and the glyph list.
        assert_eq!(printable("é世🙂"), "···");
        // Glyphs the interface draws, and braille.
        assert_eq!(printable("↻ ✔ ▌ …"), "↻ ✔ ▌ …");
        assert_eq!(printable("\u{2801}"), "\u{2801}");
        // Glyphs the interface bans.
        assert_eq!(printable("⟳⏸❶⏱⬤☼"), "······");
        // Bidirectional overrides and zero-width characters.
        assert_eq!(printable("a\u{202e}b\u{200b}c"), "a·b·c");
        for c in printable(&('\u{0}'..='\u{2fff}').collect::<String>()).chars() {
            assert!(theme::is_allowed(c) && !c.is_control(), "{c:?}");
        }
    }

    #[test]
    fn a_hostile_key_displays_inside_the_allowlist() {
        let key = spec("a\u{1b}[2J\u{202e}é");
        assert_eq!(key.to_string(), "\"a·[2J··\" as String");
        let error = KeySpec::parse("hex:\u{1b}").expect_err("an escape is not a hex digit");
        assert!(error.to_string().contains("'·' is not one"), "{error}");
    }

    #[test]
    fn request_line_asks_only_a_single_token_string_key() {
        let line = |input: &str| request_line(&spec(input));
        assert_eq!(line("k17").as_deref(), Some("explain k17"));
        assert_eq!(line("user:42").as_deref(), Some("explain user:42"));
        // `str:` sends the text after the prefix: the String `uint:1`.
        assert_eq!(line("str:uint:1").as_deref(), Some("explain uint:1"));
        // Not one token, not ASCII graphic, or not empty.
        assert_eq!(line("a b"), None);
        assert_eq!(line(" a"), None);
        assert_eq!(line("é"), None);
        assert_eq!(line("a\tb"), None);
        assert_eq!(line(""), None);
        // A key of another kind is located but never asked.
        assert_eq!(line("uint:5"), None);
        assert_eq!(line("int:5"), None);
        assert_eq!(line("hex:6b"), None);
    }

    #[test]
    fn kinds_have_a_prefix_token_and_a_name() {
        let kinds = [
            (KeyKind::Str, "str", "String"),
            (KeyKind::Uint, "uint", "unsigned integer"),
            (KeyKind::Int, "int", "signed integer"),
            (KeyKind::Hex, "hex", "postcard bytes"),
        ];
        for (kind, token, name) in kinds {
            assert_eq!(kind.token(), token);
            assert_eq!(kind.describe(), name);
        }
    }

    #[test]
    fn each_key_error_reads_as_one_sentence_naming_the_remedy() {
        let cases = [
            (KeyError::TooLong { chars: 300 }, "has 300: shorten it"),
            (KeyError::NotANumber(KeyKind::Uint), "as in uint:42"),
            (KeyError::NotANumber(KeyKind::Int), "as in int:-7"),
            (KeyError::NegativeUnsigned, "write a negative key as int:"),
            (
                KeyError::OutOfRange(KeyKind::Uint),
                "use hex: for a wider key",
            ),
            (
                KeyError::OutOfRange(KeyKind::Int),
                "use hex: for a wider key",
            ),
            (
                KeyError::OddHex { digits: 3 },
                "this has 3: add or drop a digit",
            ),
            (KeyError::NotHex { found: 'g' }, "'g' is not one: remove it"),
            // A space between byte pairs is the likeliest slip: it shows.
            (KeyError::NotHex { found: ' ' }, "' ' is not one: remove it"),
            (KeyError::EmptyHex, "at least one pair of hex digits"),
        ];
        for (error, remedy) in cases {
            let text = error.to_string();
            assert!(text.contains(remedy), "{text}");
            assert!(!text.contains('\n') && !text.ends_with('.'), "{text}");
        }
    }
}
