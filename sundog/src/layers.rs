//! The crate's lower modules name only the modules beneath them, the way a
//! workspace's crates depend one way: `node` and `hlc` at the bottom, the
//! anti-entropy `sketch` on them, `wire` on those, then `store`, then `net`.
//! A path in a checked module that reaches the crate root, through `crate::`
//! or a chain of `super::`, fails here with the file and line when it names
//! a module outside the allowed list, when it names a crate-root re-export,
//! which would hide the module it reaches, and when it aliases the crate
//! root itself.
//!
//! The scan reads code only. Comments, doc links among them, and string and
//! character literals are blanked before it looks, except a path an
//! attribute holds in a string, as serde's `with = "crate::x::codec"` does,
//! which the compiler resolves like code. The files it reads are
//! the ones the module's `mod` declarations name; a module moved by
//! `#[path]` or code taken in by `include!` fails the check, and so does a
//! `lib.rs` that aliases the crate root, as `extern crate self as x` does.
//! Test code may reach anywhere: a module declared under a `cfg` that
//! requires `test`, or one that opens with such a `#![cfg]`, is not read,
//! and an item under such a `cfg` is blanked through its end, every
//! attribute on it included. That end is the first `;` outside any
//! bracket, the first `}` that leaves no bracket open, or, unless the gate
//! is on an item or a `let` ([`opens_item`]), the first `,` outside any
//! bracket; it never passes the bracket that closes the block around the
//! item. A gate on an element of a list ([`in_list`]), just after a `,`,
//! an opening `(`, `[` or `<`, or a closure's `|`, is never on an item, and
//! the element also ends at a `>` or `|` outside any bracket, where its list
//! can close. Each mark comes at or before the item's real end, so a gate
//! never hides the code after its item. It can end one early, as at a comma
//! in a gated field's generic arguments, and the rest of the item is then
//! read as code.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// Each checked module and every other top-level module its code may name.
const LAYERS: &[(&str, &[&str])] = &[
    ("node", &[]),
    ("hlc", &["node"]),
    ("sketch", &["hlc", "node"]),
    ("wire", &["error", "hlc", "node", "sketch"]),
    (
        "store",
        &[
            "config",
            "error",
            "explain",
            "hlc",
            "node",
            "ownership",
            "verdict",
            "wire",
        ],
    ),
    (
        "net",
        &[
            "config",
            "error",
            "hlc",
            "membership",
            "node",
            "sketch",
            "store",
            "wire",
        ],
    ),
];

/// Whether `byte` can be part of a word: an ASCII letter, digit or `_`, or
/// a byte of a non-ASCII character, which outside a literal is part of an
/// identifier once [`lex`] has blanked the non-ASCII whitespace.
const fn is_ident(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_' || byte >= 0x80
}

/// Overwrites `bytes[range]` with spaces, keeping its newlines.
fn blank(bytes: &mut [u8], range: std::ops::Range<usize>) {
    for byte in &mut bytes[range] {
        if *byte != b'\n' {
            *byte = b' ';
        }
    }
}

/// The index just past the literal or comment that starts at `bytes[i]`, or
/// `None` when none starts there.
fn literal_end(bytes: &[u8], i: usize) -> Option<usize> {
    let at = |k: usize| bytes.get(k).copied();
    let len = bytes.len();
    match bytes[i] {
        b'/' if at(i + 1) == Some(b'/') => {
            Some((i..len).find(|&k| bytes[k] == b'\n').unwrap_or(len))
        }
        b'/' if at(i + 1) == Some(b'*') => {
            let (mut depth, mut k) = (0usize, i);
            while k < len {
                if bytes[k..].starts_with(b"/*") {
                    depth += 1;
                    k += 2;
                } else if bytes[k..].starts_with(b"*/") {
                    depth -= 1;
                    k += 2;
                    if depth == 0 {
                        return Some(k);
                    }
                } else {
                    k += 1;
                }
            }
            Some(len)
        }
        b'"' => {
            let mut k = i + 1;
            while k < len && bytes[k] != b'"' {
                k += if bytes[k] == b'\\' { 2 } else { 1 };
            }
            Some((k + 1).min(len))
        }
        b'r' if i == 0 || !is_ident(bytes[i - 1]) || raw_prefix(bytes, i) => {
            let hashes = bytes[i + 1..].iter().take_while(|&&b| b == b'#').count();
            if at(i + 1 + hashes) != Some(b'"') {
                return None;
            }
            let mut close = vec![b'"'];
            close.extend(std::iter::repeat_n(b'#', hashes));
            let body = i + 2 + hashes;
            Some(
                (body..len)
                    .find(|&k| bytes[k..].starts_with(&close))
                    .map_or(len, |k| k + close.len()),
            )
        }
        b'\'' if at(i + 1) == Some(b'\\') => Some(
            (i + 3..len)
                .find(|&k| bytes[k] == b'\'')
                .map_or(len, |k| k + 1),
        ),
        b'\'' => {
            let width = match at(i + 1) {
                Some(0xc0..=0xdf) => 2,
                Some(0xe0..=0xef) => 3,
                Some(0xf0..=0xff) => 4,
                _ => 1,
            };
            (at(i + 1 + width) == Some(b'\'')).then_some(i + 2 + width)
        }
        _ => None,
    }
}

/// Whether the `r` at `bytes[i]` follows a lone `b` or `c`, as in `br"…"`.
fn raw_prefix(bytes: &[u8], i: usize) -> bool {
    i >= 1 && matches!(bytes[i - 1], b'b' | b'c') && (i == 1 || !is_ident(bytes[i - 2]))
}

/// `source` with every comment and every string, raw string, byte string
/// and character literal overwritten by spaces, newlines kept, so line
/// numbers stay where they were and brackets count right, and with the
/// non-ASCII characters rustc reads as whitespace overwritten too. A
/// lifetime stays.
fn lex(source: &str) -> Vec<u8> {
    let mut bytes = source.as_bytes().to_vec();
    let mut i = 0;
    while i < bytes.len() {
        if let Some(end) = literal_end(&bytes, i) {
            blank(&mut bytes, i..end);
            i = end;
        } else {
            i += 1;
        }
    }
    for space in ["\u{85}", "\u{200e}", "\u{200f}", "\u{2028}", "\u{2029}"] {
        let space = space.as_bytes();
        let mut k = 0;
        while let Some(at) = bytes[k..].windows(space.len()).position(|w| w == space) {
            blank(&mut bytes, k + at..k + at + space.len());
            k += at + space.len();
        }
    }
    bytes
}

/// The index of the bracket that closes the one at `bytes[open]`.
fn matching_close(bytes: &[u8], open: usize) -> usize {
    let mut depth = 0usize;
    for (k, &byte) in bytes.iter().enumerate().skip(open) {
        match byte {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' => {
                depth -= 1;
                if depth == 0 {
                    return k;
                }
            }
            _ => {}
        }
    }
    bytes.len() - 1
}

/// The index of the bracket that opens the one at `bytes[close]`.
fn matching_open(bytes: &[u8], close: usize) -> usize {
    let mut depth = 0usize;
    for k in (0..=close).rev() {
        match bytes[k] {
            b')' | b']' | b'}' => depth += 1,
            b'(' | b'[' | b'{' => {
                depth -= 1;
                if depth == 0 {
                    return k;
                }
            }
            _ => {}
        }
    }
    0
}

/// The index of the bracket that closes the block `bytes[from]` sits in, or
/// the end of `bytes` at its top level.
fn block_end(bytes: &[u8], from: usize) -> usize {
    let mut depth = 0usize;
    for (k, &byte) in bytes.iter().enumerate().skip(from) {
        match byte {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' if depth == 0 => return k,
            b')' | b']' | b'}' => depth -= 1,
            _ => {}
        }
    }
    bytes.len()
}

/// Whether the `cfg` predicate `predicate` holds only under `test`: `test`
/// itself, an `all(…)` with such an argument, or an `any(…)` of them. A
/// predicate whose parentheses do not pair up holds under no `test`.
fn requires_test(predicate: &str) -> bool {
    let predicate = predicate.trim();
    let mut open = 0usize;
    for c in predicate.chars() {
        match c {
            '(' => open += 1,
            ')' if open == 0 => return false,
            ')' => open -= 1,
            _ => {}
        }
    }
    if open != 0 {
        return false;
    }
    let args = |inner: &str| -> Vec<String> {
        let (mut parts, mut depth, mut part) = (Vec::new(), 0usize, String::new());
        for c in inner.chars() {
            match c {
                '(' => depth += 1,
                ')' => depth -= 1,
                ',' if depth == 0 => {
                    parts.push(std::mem::take(&mut part));
                    continue;
                }
                _ => {}
            }
            part.push(c);
        }
        parts.push(part);
        parts.retain(|part| !part.trim().is_empty());
        parts
    };
    if predicate == "test" {
        true
    } else if let Some(inner) = predicate
        .strip_prefix("all(")
        .and_then(|p| p.strip_suffix(')'))
    {
        args(inner).iter().any(|arg| requires_test(arg))
    } else if let Some(inner) = predicate
        .strip_prefix("any(")
        .and_then(|p| p.strip_suffix(')'))
    {
        let args = args(inner);
        !args.is_empty() && args.iter().all(|arg| requires_test(arg))
    } else {
        false
    }
}

/// The index of the first of the outer attributes that run up to
/// `bytes[at]`, or `at` when none does.
fn attributes_start(bytes: &[u8], mut at: usize) -> usize {
    while let Some(k) = bytes[..at].iter().rposition(|b| !b.is_ascii_whitespace())
        && bytes[k] == b']'
    {
        let open = matching_open(bytes, k);
        if open == 0 || bytes[open - 1] != b'#' {
            break;
        }
        at = open - 1;
    }
    at
}

/// Whether the attribute at `bytes[at]` sits on an element of a list: just
/// after a `,`, an opening `(`, `[` or `<`, or a closure's `|`, past any
/// attributes before it.
fn in_list(bytes: &[u8], at: usize) -> bool {
    let at = attributes_start(bytes, at);
    bytes[..at]
        .iter()
        .rposition(|b| !b.is_ascii_whitespace())
        .is_some_and(|k| matches!(bytes[k], b',' | b'(' | b'[' | b'<' | b'|'))
}

/// Whether the code at `bytes[start]`, past a gate and its attributes, opens
/// an item or a `let` rather than a field, variant, arm or expression. After
/// any visibility and any of `async`, `const`, `unsafe` and `extern`, that is
/// a named `fn`, a named `union` with a body or generics, `macro_rules!`,
/// `extern crate`, a `const` item, a keyword only an item or a `let` starts
/// with, or a qualified block. A field or binding named `union` or
/// `macro_rules` is no item.
fn opens_item(bytes: &[u8], start: usize) -> bool {
    let window = String::from_utf8_lossy(&bytes[start..bytes.len().min(start + 256)]);
    let tokens = tokens(&window);
    let at = |k: usize| tokens.get(k).map(|&(_, token)| token);
    let mut k = 0;
    if at(k) == Some(Token::Word("pub")) {
        k += 1;
        if at(k) == Some(Token::Punct(b'(')) {
            while at(k).is_some_and(|token| token != Token::Punct(b')')) {
                k += 1;
            }
            k += 1;
        }
    }
    let is_qualifier = |token| {
        matches!(
            token,
            Some(Token::Word("async" | "const" | "unsafe" | "extern"))
        )
    };
    let mut qualified = false;
    let mut external = false;
    while is_qualifier(at(k)) {
        let const_item = at(k) == Some(Token::Word("const"))
            && matches!(at(k + 1), Some(Token::Word(name)) if name != "fn")
            && !is_qualifier(at(k + 1));
        if const_item {
            return true;
        }
        external |= at(k) == Some(Token::Word("extern"));
        qualified = true;
        k += 1;
    }
    match at(k) {
        Some(Token::Word("fn")) => matches!(at(k + 1), Some(Token::Word(_))),
        Some(Token::Word("union")) => {
            matches!(at(k + 1), Some(Token::Word(_)))
                && matches!(
                    at(k + 2),
                    Some(Token::Punct(b'{' | b'<') | Token::Word("where"))
                )
        }
        Some(Token::Word("macro_rules")) => at(k + 1) == Some(Token::Punct(b'!')),
        Some(Token::Word("crate")) => external,
        Some(Token::Word(
            "enum" | "impl" | "let" | "mod" | "static" | "struct" | "trait" | "type" | "use",
        )) => true,
        Some(Token::Punct(b'{')) => qualified,
        _ => false,
    }
}

/// The index just past the item a gate ending at `bytes[from - 1]` covers.
/// `list` says the gate is on an element of a list ([`in_list`]), which also
/// ends at a `>` or `|` outside any bracket, where a generic or closure
/// parameter list closes; the `>` of `->` and `=>` does not count.
fn gated_item_end(bytes: &[u8], from: usize, list: bool) -> usize {
    let skip_space = |mut k: usize| {
        while k < bytes.len() && bytes[k].is_ascii_whitespace() {
            k += 1;
        }
        k
    };
    let mut start = skip_space(from);
    while bytes[start..].starts_with(b"#[") {
        start = skip_space(matching_close(bytes, start + 1) + 1);
    }
    let item = !list && opens_item(bytes, start);
    let mut depth = 0usize;
    for (k, &byte) in bytes.iter().enumerate().skip(start) {
        match byte {
            b'(' | b'[' | b'{' => depth += 1,
            b')' | b']' | b'}' if depth == 0 => return k,
            b')' | b']' | b'}' => {
                depth -= 1;
                if byte == b'}' && depth == 0 {
                    return k + 1;
                }
            }
            b';' if depth == 0 => return k + 1,
            b',' if depth == 0 && !item => return k + 1,
            b'|' if depth == 0 && list => return k,
            b'>' if depth == 0
                && list
                && !matches!(k.checked_sub(1).map(|p| bytes[p]), Some(b'-' | b'=')) =>
            {
                return k;
            }
            _ => {}
        }
    }
    bytes.len()
}

/// The keys whose string values are prose rather than a path the compiler
/// resolves.
const PROSE_KEYS: &[&str] = &[
    "alias", "doc", "label", "message", "note", "reason", "since",
];

/// The word that ends just before `code[at]`, past any whitespace, or `""`.
fn word_before(code: &[u8], at: usize) -> &str {
    let end = code[..at]
        .iter()
        .rposition(|b| !b.is_ascii_whitespace())
        .map_or(0, |k| k + 1);
    let start = code[..end]
        .iter()
        .rposition(|&b| !is_ident(b))
        .map_or(0, |k| k + 1);
    std::str::from_utf8(&code[start..end]).unwrap_or_default()
}

/// The key of the `key = "…"` string that starts at `code[at]`, or `None`
/// when no `=` comes before it.
fn string_key(code: &[u8], at: usize) -> Option<&str> {
    let eq = code[..at].iter().rposition(|b| !b.is_ascii_whitespace())?;
    (code[eq] == b'=').then(|| word_before(code, eq))
}

/// Whether the attribute whose `[` is `code[open]` is a `cfg_attr` whose
/// predicate requires `test`.
fn under_test_cfg_attr(code: &[u8], open: usize) -> bool {
    if !code[open..].starts_with(b"[cfg_attr(") {
        return false;
    }
    let args = open + "[cfg_attr(".len();
    let close = matching_close(code, args - 1);
    let mut depth = 0usize;
    let predicate_end = (args..close)
        .find(|&k| {
            match code[k] {
                b'(' | b'[' | b'{' => depth += 1,
                b')' | b']' | b'}' => depth = depth.saturating_sub(1),
                b',' if depth == 0 => return true,
                _ => {}
            }
            false
        })
        .unwrap_or(close);
    requires_test(&String::from_utf8_lossy(&code[args..predicate_end]))
}

/// Writes back into `code`, which [`lex`] made of `source`, each path an
/// attribute holds in a string, such as serde's `with = "crate::x::codec"`:
/// the text of every `key = "…"` string, raw or not, in an attribute whose
/// key is not in [`PROSE_KEYS`] and whose text holds `crate::`, `super::` or
/// `self::`. An attribute under a `cfg_attr` that requires `test` stays
/// blank, and so does every bracket and quote in the text, so the brackets
/// of `code` still pair up.
fn restore_attribute_paths(source: &[u8], code: &mut [u8]) {
    let mut i = 0;
    while i < code.len() {
        let open = i + 1 + usize::from(code.get(i + 1) == Some(&b'!'));
        if code[i] != b'#' || code.get(open) != Some(&b'[') {
            i += 1;
            continue;
        }
        let close = matching_close(code, open);
        i = close + 1;
        if under_test_cfg_attr(code, open) {
            continue;
        }
        let mut k = open;
        while k < close {
            let Some(end) = literal_end(source, k) else {
                k += 1;
                continue;
            };
            let text = match source[k] {
                b'"' => k + 1..end.saturating_sub(1).max(k + 1),
                b'r' => {
                    let hashes = source[k + 1..].iter().take_while(|&&b| b == b'#').count();
                    let body = k + 2 + hashes;
                    body..end.saturating_sub(1 + hashes).max(body)
                }
                _ => k..k,
            };
            let holds_path = [&b"crate::"[..], b"super::", b"self::"]
                .iter()
                .any(|root| source[text.clone()].windows(root.len()).any(|w| w == *root));
            let path_key = string_key(code, k)
                .is_some_and(|key| !key.is_empty() && !PROSE_KEYS.contains(&key));
            if holds_path && path_key {
                for at in text {
                    code[at] = match source[at] {
                        b'(' | b')' | b'[' | b']' | b'{' | b'}' | b'"' | b'\\' => b' ',
                        byte => byte,
                    };
                }
            }
            k = end;
        }
    }
}

/// `source`'s code ([`lex`]), with the paths attributes hold in strings
/// written back ([`restore_attribute_paths`]), every item under a `cfg` that
/// requires `test` blanked through its end, every attribute on it included,
/// and every module that opens with such a `#![cfg]` blanked whole.
fn code_only(source: &str) -> String {
    let mut bytes = lex(source);
    restore_attribute_paths(source.as_bytes(), &mut bytes);
    let mut i = 0;
    while i < bytes.len() {
        let inner = bytes[i..].starts_with(b"#![cfg(");
        if !inner && !bytes[i..].starts_with(b"#[cfg(") {
            i += 1;
            continue;
        }
        let open = i + if inner { 2 } else { 1 };
        let close = matching_close(&bytes, open);
        let paren = open + "[cfg".len();
        let gated = bytes
            .get(paren + 1..matching_close(&bytes, paren))
            .is_some_and(|predicate| {
                requires_test(std::str::from_utf8(predicate).expect("lexed code is UTF-8"))
            });
        let end = match (gated, inner) {
            (false, _) => close + 1,
            (true, true) => block_end(&bytes, close + 1),
            (true, false) => gated_item_end(&bytes, close + 1, in_list(&bytes, i)),
        };
        if gated {
            let start = if inner {
                i
            } else {
                attributes_start(&bytes, i)
            };
            blank(&mut bytes, start..end);
        }
        i = end.max(close + 1);
    }
    String::from_utf8(bytes).expect("lexed code is UTF-8")
}

/// A token of code: a word, or any one other character but whitespace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Token<'a> {
    Word(&'a str),
    Punct(u8),
}

/// `code`'s tokens, each with the offset it starts at. A raw identifier,
/// `r#name`, is one word.
fn tokens(code: &str) -> Vec<(usize, Token<'_>)> {
    let bytes = code.as_bytes();
    let mut out = Vec::new();
    let mut i = 0;
    while i < bytes.len() {
        if is_ident(bytes[i]) {
            let start = i;
            if bytes[i..].starts_with(b"r#") && bytes.get(i + 2).is_some_and(|&b| is_ident(b)) {
                i += 2;
            }
            while i < bytes.len() && is_ident(bytes[i]) {
                i += 1;
            }
            out.push((start, Token::Word(&code[start..i])));
        } else {
            if !bytes[i].is_ascii_whitespace() {
                out.push((i, Token::Punct(bytes[i])));
            }
            i += 1;
        }
    }
    out
}

/// The modules `code` declares in files of their own, `mod name;`.
fn outline_mods(code: &str) -> Vec<String> {
    let tokens = tokens(code);
    let mut braces = 0usize;
    let mut names = Vec::new();
    for (k, &(_, token)) in tokens.iter().enumerate() {
        match token {
            Token::Punct(b'{') => braces += 1,
            Token::Punct(b'}') => braces = braces.saturating_sub(1),
            Token::Word("mod") => {
                if let (Some((_, Token::Word(name))), Some((_, Token::Punct(b';')))) =
                    (tokens.get(k + 1), tokens.get(k + 2))
                {
                    assert_eq!(
                        braces, 0,
                        "`mod {name};` inside a block: teach src/layers.rs where its file is"
                    );
                    names.push(name.strip_prefix("r#").unwrap_or(name).to_owned());
                }
            }
            _ => {}
        }
    }
    names
}

/// One file's tokens, read for the paths that reach the crate root
/// ([`root_names`]), and the names found so far.
struct Paths<'a> {
    code: &'a str,
    tokens: Vec<(usize, Token<'a>)>,
    found: Vec<(usize, String)>,
}

impl<'a> Paths<'a> {
    fn at(&self, k: usize) -> Option<Token<'a>> {
        self.tokens.get(k).map(|&(_, token)| token)
    }

    /// Whether the tokens from `k` start with `::`.
    fn path_sep(&self, k: usize) -> bool {
        self.at(k) == Some(Token::Punct(b':')) && self.at(k + 1) == Some(Token::Punct(b':'))
    }

    /// Records `name` on the line of token `k`.
    fn record(&mut self, k: usize, name: &str) {
        let line = self.code[..self.tokens[k].0].matches('\n').count() + 1;
        let name = name.strip_prefix("r#").unwrap_or(name);
        self.found.push((line, name.to_owned()));
    }

    /// Reads the path or use tree at token `k`, `up` modules above the
    /// file's own with the crate root `root` modules up, records each name it
    /// reaches the root with, and returns the index after it.
    fn path(&mut self, k: usize, up: usize, root: usize) -> usize {
        match self.at(k) {
            Some(Token::Punct(b'{')) => self.group(k, up, root),
            Some(Token::Punct(b'*')) if up == root => {
                self.record(k, "*");
                k + 1
            }
            Some(Token::Punct(b'$')) if up == root => {
                if let Some(Token::Word(variable)) = self.at(k + 1) {
                    self.record(k, &format!("${variable}"));
                    return self.rest(k + 1);
                }
                self.record(k, "$");
                self.past_repetition(k + 1)
            }
            Some(Token::Word("self")) if up == root => {
                if self.at(k + 1) == Some(Token::Word("as")) {
                    self.record(k, "self");
                }
                k + 1
            }
            Some(Token::Word(name)) if up == root => {
                self.record(k, name);
                self.rest(k)
            }
            Some(Token::Word(word @ ("self" | "super"))) => {
                let up = up + usize::from(word == "super");
                if self.path_sep(k + 1) {
                    return self.path(k + 3, up, root);
                }
                if up == root && word == "super" {
                    self.record(k, "self");
                }
                k + 1
            }
            _ => self.rest(k),
        }
    }

    /// Reads each member of the use group that opens at token `k` as
    /// [`Self::path`] does, and returns the index after the group.
    fn group(&mut self, k: usize, up: usize, root: usize) -> usize {
        let mut k = k + 1;
        loop {
            if matches!(self.at(k), None | Some(Token::Punct(b'}'))) {
                return k + 1;
            }
            k = self.path(k, up, root);
            let mut depth = 0usize;
            loop {
                match self.at(k) {
                    None => return k,
                    Some(Token::Punct(b'{')) => depth += 1,
                    Some(Token::Punct(b'}')) if depth == 0 => return k + 1,
                    Some(Token::Punct(b'}')) => depth -= 1,
                    Some(Token::Punct(b',')) if depth == 0 => {
                        k += 1;
                        break;
                    }
                    _ => {}
                }
                k += 1;
            }
        }
    }

    /// The index after the macro repetition `$( … ) sep op` whose `(` is
    /// token `k`, or `k` when no `(` is there.
    fn past_repetition(&self, k: usize) -> usize {
        if self.at(k) != Some(Token::Punct(b'(')) {
            return k;
        }
        let mut depth = 0usize;
        let mut close = k;
        while let Some(token) = self.at(close) {
            match token {
                Token::Punct(b'(') => depth += 1,
                Token::Punct(b')') => {
                    depth -= 1;
                    if depth == 0 {
                        break;
                    }
                }
                _ => {}
            }
            close += 1;
        }
        (close + 1..close + 4)
            .find(|&op| matches!(self.at(op), Some(Token::Punct(b'*' | b'+' | b'?'))))
            .map_or(close + 1, |op| op + 1)
    }

    /// The index after the rest of the path at token `k`, its segments and
    /// any use group, none of which reaches the root again.
    fn rest(&self, mut k: usize) -> usize {
        loop {
            match self.at(k) {
                Some(Token::Word(_) | Token::Punct(b'*')) => k += 1,
                Some(Token::Punct(b'{')) => {
                    let mut depth = 0usize;
                    while let Some(token) = self.at(k) {
                        k += 1;
                        match token {
                            Token::Punct(b'{') => depth += 1,
                            Token::Punct(b'}') => {
                                depth -= 1;
                                if depth == 0 {
                                    break;
                                }
                            }
                            _ => {}
                        }
                    }
                }
                _ => return k,
            }
            if !self.path_sep(k) {
                return k;
            }
            k += 2;
        }
    }
}

/// The name every path in `code` reaches the crate root with, and the
/// 1-based line it is on: the segment after `crate::`, or after a chain of
/// `super::` as long as the module's depth below the root, `depth` for the
/// file plus one for each inline `mod` around the path. A chain counts
/// across use groups, as in `super::{super::x}`. A grouped
/// `crate::{a::X, b::Y}` names each member, on that member's own line,
/// `crate::*` names `*`, and a macro variable such as `crate::$m` names
/// `$m`, or `$` for a repetition. The root itself, named anywhere but at the
/// head of a path, in a `pub(…)` restriction or in `extern crate`, names
/// `self`: an alias such as `use crate as x`, `use super as x` at depth one,
/// `crate::{self as x}` or `extern crate self as x`, and the root passed to
/// a macro, as in `m!(crate)`.
fn root_names(code: &str, depth: usize) -> Vec<(usize, String)> {
    let mut paths = Paths {
        code,
        tokens: tokens(code),
        found: Vec::new(),
    };
    let mut inline_mods = Vec::new();
    let mut braces = 0usize;
    let mut k = 0;
    while k < paths.tokens.len() {
        let root = depth + inline_mods.len();
        let token = paths.tokens[k].1;
        let next = match token {
            Token::Punct(b'{') => {
                braces += 1;
                k + 1
            }
            Token::Punct(b'}') => {
                if inline_mods.last() == Some(&braces) {
                    inline_mods.pop();
                }
                braces = braces.saturating_sub(1);
                k + 1
            }
            Token::Word("mod") => {
                let name = if paths.at(k + 1) == Some(Token::Punct(b'$')) {
                    k + 2
                } else {
                    k + 1
                };
                if matches!(paths.at(name), Some(Token::Word(_)))
                    && paths.at(name + 1) == Some(Token::Punct(b'{'))
                {
                    inline_mods.push(braces + 1);
                }
                k + 1
            }
            Token::Word("pub") if paths.at(k + 1) == Some(Token::Punct(b'(')) => {
                let restriction = matches!(paths.at(k + 2), Some(Token::Word("in")))
                    || (matches!(
                        paths.at(k + 2),
                        Some(Token::Word("crate" | "self" | "super"))
                    ) && paths.at(k + 3) == Some(Token::Punct(b')')));
                if restriction {
                    (k + 2..paths.tokens.len())
                        .find(|&close| paths.at(close) == Some(Token::Punct(b')')))
                        .map_or(k + 1, |close| close + 1)
                } else {
                    k + 1
                }
            }
            Token::Word("crate") if paths.path_sep(k + 1) => paths.path(k + 3, root, root),
            Token::Word("crate") if k > 0 && paths.at(k - 1) == Some(Token::Word("extern")) => {
                if paths.at(k + 1) == Some(Token::Word("self")) {
                    paths.record(k, "self");
                }
                k + 1
            }
            Token::Word("crate") => {
                paths.record(k, "self");
                k + 1
            }
            Token::Word("self" | "super") => paths.path(k, 0, root),
            _ => k + 1,
        };
        k = next.max(k + 1);
    }
    paths.found
}

/// Whether `code` takes code from a file the walk does not read: a module
/// moved by `#[path]`, or by a `#[cfg_attr(…)]` that applies one, or an
/// `include!`.
fn reads_code_the_walk_skips(code: &str) -> bool {
    let bytes = code.as_bytes();
    let includes = tokens(code)
        .windows(2)
        .any(|pair| matches!(pair, [(_, Token::Word("include")), (_, Token::Punct(b'!'))]));
    includes
        || (0..bytes.len()).filter(|&i| bytes[i] == b'#').any(|i| {
            let open = i + 1 + usize::from(bytes.get(i + 1) == Some(&b'!'));
            if bytes.get(open) != Some(&b'[') {
                return false;
            }
            let close = matching_close(bytes, open);
            let attribute = String::from_utf8_lossy(&bytes[open..=close]);
            tokens(&attribute).windows(3).any(|window| {
                matches!(
                    window,
                    [
                        (_, Token::Punct(b'[' | b',' | b'(')),
                        (_, Token::Word("path")),
                        (_, Token::Punct(b'=')),
                    ]
                )
            })
        })
}

/// One file of a checked module, its code ([`code_only`]), and how many
/// modules deep it sits below the crate root.
struct ModuleFile {
    path: PathBuf,
    code: String,
    depth: usize,
}

/// The file that holds module `name`, declared by a file whose children
/// live in `dir`: `dir/name.rs` or `dir/name/mod.rs`.
fn declared_file(dir: &Path, name: &str) -> PathBuf {
    let flat = dir.join(format!("{name}.rs"));
    let nested = dir.join(name).join("mod.rs");
    match (flat.is_file(), nested.is_file()) {
        (true, false) => flat,
        (false, true) => nested,
        _ => panic!("`mod {name};` needs one of {flat:?} and {nested:?}"),
    }
}

/// `path`, read as a module `depth` deep, and every file its `mod`
/// declarations reach outside test code.
fn walk(path: PathBuf, depth: usize, out: &mut Vec<ModuleFile>) {
    let source = std::fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("{} reads: {err}", path.display()));
    let code = code_only(&source);
    assert!(
        !reads_code_the_walk_skips(&code),
        "{}: code from a file of another name, by `#[path]` or `include!`; \
         teach src/layers.rs to follow it",
        path.display()
    );
    let dir = if path.ends_with("mod.rs") {
        path.parent().expect("a mod.rs has a directory").to_owned()
    } else {
        path.with_extension("")
    };
    for name in outline_mods(&code) {
        walk(declared_file(&dir, &name), depth + 1, out);
    }
    out.push(ModuleFile { path, code, depth });
}

/// Every file of top-level module `module` outside test code.
fn module_files(src: &Path, module: &str) -> Vec<ModuleFile> {
    let mut files = Vec::new();
    walk(declared_file(src, module), 1, &mut files);
    files
}

/// Whether `code` aliases the crate root with `extern crate self`, which in
/// `lib.rs` lets every module name the root by the alias.
fn aliases_the_root(code: &str) -> bool {
    tokens(code).windows(3).any(|window| {
        matches!(
            window,
            [
                (_, Token::Word("extern")),
                (_, Token::Word("crate")),
                (_, Token::Word("self")),
            ]
        )
    })
}

/// `lib.rs`'s code ([`code_only`]).
fn lib_code(src: &Path) -> String {
    code_only(&std::fs::read_to_string(src.join("lib.rs")).expect("src/lib.rs reads"))
}

/// The top-level modules `lib.rs` declares outside test code.
fn top_level_modules(src: &Path) -> BTreeSet<String> {
    outline_mods(&lib_code(src)).into_iter().collect()
}

fn src_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("src")
}

#[test]
fn lower_modules_name_only_the_modules_beneath_them() {
    let src = src_dir();
    let modules = top_level_modules(&src);
    let mut violations = Vec::new();
    for &(module, allowed) in LAYERS {
        for file in module_files(&src, module) {
            let relative = file.path.strip_prefix(&src).expect("a file under src/");
            for (line, name) in root_names(&file.code, file.depth) {
                if name == module || allowed.contains(&name.as_str()) {
                    continue;
                }
                let what = if name == "self" {
                    "an alias of the crate root, which hides every module behind it"
                } else if name.starts_with('$') {
                    "a macro variable, which can name any module"
                } else if modules.contains(&name) {
                    "a module above its layer"
                } else {
                    "a crate-root re-export; name it by its module"
                };
                violations.push(format!(
                    "{}:{line} names crate::{name}, {what}",
                    relative.display()
                ));
            }
        }
    }
    if aliases_the_root(&lib_code(&src)) {
        violations.push(
            "lib.rs aliases the crate root, and a checked module could name any module \
             through the alias"
                .to_owned(),
        );
    }
    assert!(
        violations.is_empty(),
        "a lower module reaches above its layer (see LAYERS in src/layers.rs):\n{}",
        violations.join("\n")
    );
}

#[test]
fn every_layer_and_every_allowed_name_is_a_declared_module() {
    let src = src_dir();
    let modules = top_level_modules(&src);
    for &(module, allowed) in LAYERS {
        assert!(modules.contains(module), "{module} is not a module");
        for name in allowed {
            assert!(
                modules.contains(*name),
                "{module} allows {name}, not a module"
            );
        }
    }
    for module in ["cache", "cluster", "store", "wire"] {
        assert!(modules.contains(module), "{module} in {modules:?}");
    }
    assert!(
        !modules.contains("layers"),
        "a test-only module is no layer"
    );
}

#[test]
fn the_walk_follows_declarations_and_skips_test_modules() {
    let src = src_dir();
    let files: Vec<(String, usize)> = module_files(&src, "store")
        .into_iter()
        .map(|file| {
            let relative = file.path.strip_prefix(&src).expect("a file under src/");
            (relative.display().to_string(), file.depth)
        })
        .collect();
    let has = |path: &str| files.iter().find(|(file, _)| file == path).map(|f| f.1);
    assert_eq!(has("store/mod.rs"), Some(1));
    assert_eq!(has("store/load.rs"), Some(2));
    assert_eq!(has("store/engine/mod.rs"), Some(2));
    assert_eq!(has("store/engine/apply.rs"), Some(3));
    assert_eq!(
        has("store/model.rs"),
        Some(2),
        "cfg(any(test, fuzzing)) is not test-only"
    );
    assert_eq!(
        has("store/spill/mod.rs"),
        Some(2),
        "a feature gate is not a test gate"
    );
    for test_only in [
        "store/tests/mod.rs",
        "store/tests/write_api.rs",
        "store/read_cases.rs",
        "store/test_support.rs",
        "store/prop_regressions.rs",
    ] {
        assert_eq!(has(test_only), None, "{test_only}");
    }
}

#[test]
fn outline_mods_names_the_declarations_outside_test_code() {
    let source = "\
mod a;
pub(crate) mod b;
#[cfg(test)]
mod tests;
#[cfg(all(feature = \"spill\", test))]
mod spill_tests;
#[cfg(feature = \"spill\")]
pub mod spill;
#[cfg(kani)]
mod kani_proofs {
    use super::*;
}
// mod commented;
";
    assert_eq!(outline_mods(&code_only(source)), ["a", "b", "spill"]);
}

#[test]
fn requires_test_reads_cfg_predicates() {
    for predicate in [
        "test",
        "all(test, feature = x)",
        "all(feature = x, test)",
        "any(test, all(test, unix))",
    ] {
        assert!(requires_test(predicate), "{predicate}");
    }
    for predicate in [
        "kani",
        "not(test)",
        "any(test, feature = x)",
        "all(not(test), unix)",
        "feature = x",
    ] {
        assert!(!requires_test(predicate), "{predicate}");
    }
}

#[test]
fn lexing_blanks_comments_and_literals_and_keeps_lines() {
    let source = r##"use crate::wire::Msg; // crate::cache::Cache
/* crate::cache::A /* nested crate::cache::B */ crate::cache::C */
const S: &str = "crate::cache::D \" { crate::cache::E";
const R: &str = r#"crate::cache::F " { "#;
const B: &[u8] = br"crate::cache::G {";
const C: [char; 3] = ['{', '\'', '\u{7b}'];
fn f<'a>(x: &'a str) -> crate::net::Mesh { todo!() }
/// See [`crate::cache::Cache`].
fn r#type() -> crate::hlc::Hlc { todo!() }
"##;
    let code = code_only(source);
    assert_eq!(code.lines().count(), source.lines().count());
    assert_eq!(
        root_names(&code, 1),
        [
            (1, "wire".to_owned()),
            (7, "net".to_owned()),
            (9, "hlc".to_owned()),
        ]
    );
    assert!(
        code.contains("<'a>(x: &'a str)"),
        "a lifetime is code: {code}"
    );
}

#[test]
fn a_test_gate_hides_its_own_item_and_nothing_after() {
    let source = r#"pub struct Engine {
    a: u8,
    #[cfg(test)]
    probe: crate::cache::Probe,
    b: crate::cluster::Thing,
}
struct Tail {
    #[cfg(test)]
    last: crate::cache::Probe
}
fn after() -> crate::net::Mesh { todo!() }
#[cfg(test)]
#[allow(dead_code)]
fn helper() {
    let brace = '{';
    let close = "}";
    let _ = crate::observe::X;
}
#[cfg(all(test, feature = "sim"))]
use crate::{cache::A, observe::B};
#[cfg(any(test, feature = "fuzzing"))]
use crate::verdict::V;
#[cfg(not(test))]
use crate::explain::E;
fn g() {
    #[cfg(test)]
    {
        crate::cache::f();
    }
    #[cfg(test)]
    crate::cache::g();
    match 1 {
        #[cfg(test)]
        0 => crate::cache::h(1, 2),
        _ => crate::config::i(),
    }
    #[cfg(test)]
    let pair = |a, b| crate::cache::j(a, b);
    crate::error::k();
}
#[cfg(test)]
mod tests {
    use crate::cache::Cache;
}
fn last() -> crate::node::NodeId { todo!() }
"#;
    let names: Vec<(usize, String)> = root_names(&code_only(source), 1);
    let expected = [
        (5, "cluster"),
        (11, "net"),
        (22, "verdict"),
        (24, "explain"),
        (35, "config"),
        (39, "error"),
        (45, "node"),
    ];
    assert_eq!(names, expected.map(|(line, name)| (line, name.to_owned())),);
}

#[test]
fn a_comma_in_a_gated_fields_type_ends_it_early_and_hides_nothing() {
    let source = "\
struct S {
    #[cfg(test)]
    map: HashMap<u8, crate::cache::Probe>,
    next: crate::cluster::Thing,
}
";
    assert_eq!(
        root_names(&code_only(source), 1),
        [(3, "cache".to_owned()), (4, "cluster".to_owned())]
    );
}

#[test]
fn super_chains_that_reach_the_root_are_read() {
    let source = "\
use super::shard::Shard;
use super::super::cluster::View;
fn f() -> u64 { super::super::now_ms() }
mod inner {
    use super::super::wire::Msg;
    use super::super::super::hlc::Hlc;
}
use self::super::super::{node::NodeId, *};
";
    assert_eq!(
        root_names(&code_only(source), 2),
        [
            (2, "cluster".to_owned()),
            (3, "now_ms".to_owned()),
            (6, "hlc".to_owned()),
            (8, "node".to_owned()),
            (8, "*".to_owned()),
        ]
    );
}

#[test]
fn a_grouped_import_names_each_member() {
    let source = "\
use crate::{hlc::Hlc, store::{Mode, Shard}};
use crate::{
    node::NodeId,
    wire::Msg,
};
pub(in crate::store) fn f() {}
pub(crate) fn g() {}
";
    assert_eq!(
        root_names(&code_only(source), 1),
        [
            (1, "hlc".to_owned()),
            (1, "store".to_owned()),
            (3, "node".to_owned()),
            (4, "wire".to_owned()),
        ]
    );
}

#[test]
fn a_gated_list_element_or_keyword_named_field_ends_at_its_comma() {
    let source = r#"pub struct Hooks(
    #[cfg(test)]
    fn(u8),
    crate::cache::Probe,
);
pub struct Ext(#[cfg(test)] unsafe extern "C" fn(u8), crate::cluster::Thing);
pub struct S {
    #[cfg(test)]
    union: u8,
    #[cfg(test)]
    macro_rules: u8,
    next: crate::net::Mesh,
}
pub fn f<#[cfg(test)] const N: usize, T>(x: crate::observe::Probe, y: T) {}
fn g() {
    match x {
        crate::wire::A => {}
        #[cfg(test)]
        crate::cache::B => crate::cache::c(),
        _ => crate::config::d(),
    }
}
"#;
    let expected = [
        (4, "cache"),
        (6, "cluster"),
        (12, "net"),
        (14, "observe"),
        (17, "wire"),
        (20, "config"),
    ];
    assert_eq!(
        root_names(&code_only(source), 1),
        expected.map(|(line, name)| (line, name.to_owned())),
    );
}

#[test]
fn an_alias_of_the_crate_root_is_named_self() {
    let source = "\
use crate as root;
use crate::{self as top, node::NodeId};
extern crate self as sundog;
use super::{self as parent};
";
    let expected = [
        (1, "self"),
        (2, "self"),
        (2, "node"),
        (3, "self"),
        (4, "self"),
    ];
    assert_eq!(
        root_names(&code_only(source), 1),
        expected.map(|(line, name)| (line, name.to_owned())),
    );
}

#[test]
fn an_inline_mod_with_a_raw_or_macro_name_counts_toward_depth() {
    let source = "\
mod r#type {
    use super::super::cache::Cache;
}
macro_rules! m {
    ($name:ident) => {
        mod $name {
            use super::super::net::Mesh;
        }
    };
}
use crate::r#type::X;
";
    let expected = [(2, "cache"), (7, "net"), (11, "type")];
    assert_eq!(
        root_names(&code_only(source), 1),
        expected.map(|(line, name)| (line, name.to_owned())),
    );
}

#[test]
fn an_inner_test_gate_hides_its_module() {
    assert_eq!(
        root_names(&code_only("#![cfg(test)]\nuse crate::cache::Cache;\n"), 2),
        []
    );
    let source = "\
mod inner {
    #![cfg(all(test, unix))]
    use crate::cache::Cache;
}
use crate::net::Mesh;
";
    assert_eq!(root_names(&code_only(source), 1), [(5, "net".to_owned())]);
}

#[test]
fn opens_item_reads_an_items_head() {
    for head in [
        "fn f() {}",
        "pub(in crate::store) unsafe fn f() {}",
        "const fn f() {}",
        "const X: u8 = 1;",
        "const _: () = ();",
        "unsafe impl Send for X {}",
        "extern crate self as x;",
        "extern \"C\" {}",
        "union U { a: u8 }",
        "macro_rules! m { () => {} }",
        "let x = 1;",
        "pub(crate) struct S;",
        "unsafe { f() }",
        "async fn f() {}",
        "fn r#type() {}",
    ] {
        assert!(opens_item(&lex(head), 0), "{head}");
    }
    for head in [
        "fn(u8),",
        "unsafe extern \"C\" fn(u8),",
        "union: u8,",
        "macro_rules: u8,",
        "crate::cache::X => y,",
        "union(a, b);",
        "pub(crate) probe: u8,",
        "async move {}",
        "Probe(u8),",
    ] {
        assert!(!opens_item(&lex(head), 0), "{head}");
    }
}

#[test]
fn in_list_reads_what_comes_before_a_gate() {
    for (code, list) in [
        ("f(a, #[cfg(test)] b)", true),
        ("fn f<#[cfg(test)] const N: usize>() {}", true),
        ("(#[allow(x)] #[cfg(test)] a)", true),
        ("[#[cfg(test)] 1, 2]", true),
        ("|#[cfg(test)] a| a", true),
        ("{ #[cfg(test)] a: u8 }", false),
        ("x(); #[cfg(test)] fn f() {}", false),
        ("#[cfg(test)] fn f() {}", false),
        ("#[derive(Debug)] #[cfg(test)] struct S;", false),
        ("v[0] #[cfg(test)] x", false),
    ] {
        let gate = code.find("#[cfg").expect("a gate");
        assert_eq!(in_list(code.as_bytes(), gate), list, "{code}");
    }
}

#[test]
fn a_gated_last_generic_or_closure_parameter_ends_where_its_list_closes() {
    let source = "\
pub fn f<T, #[cfg(test)] const N: usize>(x: crate::cache::X<T>) {}
pub fn g<#[cfg(test)] T>(x: crate::net::Mesh) {}
fn h() {
    let _g = |#[cfg(test)] a: u8| crate::observe::go();
    match x {
        #[cfg(test)]
        union if union > 3 => crate::cache::a(),
        _ => crate::config::b(),
    }
}
";
    let expected = [(1, "cache"), (2, "net"), (4, "observe"), (8, "config")];
    assert_eq!(
        root_names(&code_only(source), 1),
        expected.map(|(line, name)| (line, name.to_owned())),
    );
}

#[test]
fn a_super_chain_counts_across_use_groups_and_aliases() {
    assert_eq!(
        root_names(&code_only("use super as root;\n"), 1),
        [(1, "self".to_owned())]
    );
    assert_eq!(
        root_names(&code_only("use super::super as root;\n"), 2),
        [(1, "self".to_owned())]
    );
    let source = "\
use super::{super::cache::Cache, Engine};
use super::{
    self,
    super::{net::Mesh, store::Shard},
};
use self::{super::super::wire::Msg};
";
    assert_eq!(
        root_names(&code_only(source), 2),
        [
            (1, "cache".to_owned()),
            (4, "net".to_owned()),
            (4, "store".to_owned()),
            (6, "wire".to_owned()),
        ]
    );
    assert_eq!(
        root_names(
            &code_only("use super::{super::{super::node::NodeId}};\n"),
            3
        ),
        [(1, "node".to_owned())]
    );
}

#[test]
fn a_non_ascii_name_is_read_whole() {
    let source = "use crate::\u{e9}::Thing;\nuse crate::na\u{ef}ve::Other;\n";
    assert_eq!(
        root_names(&code_only(source), 1),
        [(1, "\u{e9}".to_owned()), (2, "na\u{ef}ve".to_owned())]
    );
}

#[test]
fn the_scan_survives_truncated_and_unbalanced_input() {
    for source in [
        "#[cfg(",
        "#[cfg(test)]",
        "#![cfg(test)",
        "'",
        "'\\",
        "\"",
        "r#\"",
        "/*",
        "}",
        "{",
        "mod a { } }",
        "use crate::{",
        "use super::{super::",
        "crate::",
        "#[cfg(test)] fn f(",
    ] {
        let code = code_only(source);
        let _ = root_names(&code, 1);
        let _ = outline_mods(&code);
    }
}

#[test]
fn code_from_a_file_the_walk_skips_is_refused() {
    for code in [
        "#[path = \"x.rs\"]\nmod x;",
        "#[cfg_attr(unix, path = \"unix.rs\")]\nmod imp;",
        "include!(\"generated.rs\");",
    ] {
        assert!(reads_code_the_walk_skips(&code_only(code)), "{code}");
    }
    for code in [
        "#[cfg_attr(test, derive(Debug))]\nstruct S;",
        "mod path;",
        "fn f(path: u8) {}",
        "const S: &str = include_str!(\"x.txt\");",
        "let include = 1;",
        "#[path = \"tests_of_x.rs\"]\n#[cfg(test)]\nmod tests;",
    ] {
        assert!(!reads_code_the_walk_skips(&code_only(code)), "{code}");
    }
}

#[test]
fn a_test_gate_blanks_the_attributes_before_it_too() {
    let source = "\
#[derive(crate::cache::Probe)]
#[cfg(test)]
struct S;
#[derive(crate::net::Mesh)]
struct T;
";
    assert_eq!(root_names(&code_only(source), 1), [(4, "net".to_owned())]);
}

#[test]
fn a_path_an_attribute_holds_in_a_string_is_read() {
    let source = r#"#[derive(Serialize)]
struct Entry {
    #[serde(with = "crate::cache::codec")]
    value: u64,
    #[serde(default = "super::super::cluster::ttl")]
    ttl: u64,
    #[serde(bound(serialize = "T: crate::wire::Codec<u8>"))]
    inner: T,
}
#[deprecated(note = "use crate::observe::X")]
#[error("crate::observe failed")]
#[allow(dead_code, reason = "crate::observe is above")]
#[cfg_attr(test, serde(with = "crate::observe::codec"))]
const S: &str = "crate::observe::X";
fn after() -> crate::net::Mesh { todo!() }
"#;
    let expected = [(3, "cache"), (5, "cluster"), (7, "wire"), (15, "net")];
    assert_eq!(
        root_names(&code_only(source), 2),
        expected.map(|(line, name)| (line, name.to_owned())),
    );
}

#[test]
fn lib_rs_is_held_to_the_walks_rules() {
    let src = src_dir();
    assert!(
        !reads_code_the_walk_skips(&lib_code(&src)),
        "lib.rs moves a module or includes a file"
    );
}

#[test]
fn string_key_reads_the_key_before_a_string() {
    let code = lex(r#"#[serde(with = "x", default="y")] #[error("z")]"#);
    let at = |needle: &str| {
        let source = r#"#[serde(with = "x", default="y")] #[error("z")]"#;
        source.find(needle).expect("in the source")
    };
    assert_eq!(string_key(&code, at("\"x")), Some("with"));
    assert_eq!(string_key(&code, at("\"y")), Some("default"));
    assert_eq!(string_key(&code, at("\"z")), None);
    assert!(under_test_cfg_attr(
        &lex("#[cfg_attr(test, derive(Debug))]"),
        1
    ));
    assert!(under_test_cfg_attr(
        &lex("#[cfg_attr(all(test, unix), x)]"),
        1
    ));
    assert!(!under_test_cfg_attr(&lex("#[cfg_attr(unix, x)]"), 1));
    assert!(!under_test_cfg_attr(&lex("#[serde(with = \"x\")]"), 1));
}

#[test]
fn a_raw_string_attribute_path_and_a_macro_variable_root_are_read() {
    let source = r##"struct S {
    #[serde(with = r"crate::cache::codec")]
    a: u8,
    #[serde(bound = r#"T: crate::net::Codec"#)]
    b: T,
}
macro_rules! get {
    ($m:ident) => {
        crate::$m::X
    };
}
"##;
    let expected = [(2, "cache"), (4, "net"), (9, "$m")];
    assert_eq!(
        root_names(&code_only(source), 1),
        expected.map(|(line, name)| (line, name.to_owned())),
    );
}

#[test]
fn a_gated_element_with_an_arrow_still_ends_at_its_comma() {
    let source = "\
pub struct F(#[cfg(test)] fn(u8) -> crate::cache::P, crate::net::M);
fn g() {
    match x {
        0 => {}
        1 => crate::wire::a(),
        #[cfg(test)]
        2 => crate::cache::b(),
        _ => crate::config::c(),
    }
}
";
    let expected = [(1, "net"), (5, "wire"), (8, "config")];
    assert_eq!(
        root_names(&code_only(source), 1),
        expected.map(|(line, name)| (line, name.to_owned())),
    );
}

#[test]
fn lexing_reads_prefixed_raw_strings_and_wide_chars_whole() {
    let source = "\
const B: &[u8] = br#\"a \"crate::cache::H {\"#;
const C: [char; 2] = ['\u{e9}', '\u{1f600}'];
fn after() -> crate::net::Mesh { todo!() }
";
    assert_eq!(root_names(&code_only(source), 1), [(3, "net".to_owned())]);
    let code = String::from_utf8(lex(source)).expect("lexed code is UTF-8");
    assert!(
        code.is_ascii(),
        "every wide char literal is blanked: {code}"
    );
}

#[test]
fn every_macro_built_root_path_is_reported() {
    let source = "\
macro_rules! p {
    ($($s:ident)::+) => {
        crate::$($s)::+
    };
}
macro_rules! from_root {
    ($root:ident) => {
        use $root::cache::Cache;
    };
}
from_root!(crate);
use crate::{$($m::X),*};
pub(crate) fn f() {}
pub(in crate::store) fn g() {}
extern crate alloc;
";
    let expected = [(3, "$"), (11, "self"), (12, "$")];
    assert_eq!(
        root_names(&code_only(source), 1),
        expected.map(|(line, name)| (line, name.to_owned())),
    );
    assert_eq!(
        root_names(&code_only("from_root!(super);\npub(super) fn f() {}\n"), 1),
        [(1, "self".to_owned())]
    );
    assert_eq!(
        root_names(
            &code_only("from_root!(super);\npub(in super::super) fn f() {}\n"),
            2
        ),
        []
    );
}

#[test]
fn unicode_whitespace_separates_words() {
    let source = "use crate\u{200e}::wire::Msg;\nuse crate::\u{2028}net::Mesh;\n";
    assert_eq!(
        root_names(&code_only(source), 1),
        [(1, "wire".to_owned()), (2, "net".to_owned())]
    );
}

#[test]
fn only_extern_crate_self_aliases_the_root_in_lib_rs() {
    assert!(aliases_the_root(&code_only(
        "extern crate self as sundog;\n"
    )));
    for code in [
        "impl Foo { fn p(&self) -> *const Foo { self as *const Foo } }",
        "use std::fmt::{self as f};",
        "extern crate alloc;",
    ] {
        assert!(!aliases_the_root(&code_only(code)), "{code}");
    }
}

#[test]
fn a_comment_inside_a_cfg_attribute_leaves_its_predicate_whole() {
    let source = "\
#[cfg(all(test, unix)) /* c */]
fn a() -> crate::cache::A { todo!() }
#[cfg(test) // c
]
fn b() -> crate::cache::B { todo!() }
#[cfg(any(test, doc)) /* c */]
fn c() -> crate::wire::C { todo!() }
fn after() -> crate::net::Mesh { todo!() }
";
    let expected = [(7, "wire"), (8, "net")];
    assert_eq!(
        root_names(&code_only(source), 1),
        expected.map(|(line, name)| (line, name.to_owned())),
    );
    assert!(!requires_test("all(test, unix))"));
}
