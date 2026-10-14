//! Small rules on text that the parser and the frontmatter share.

use unicode_normalization::UnicodeNormalization as _;

/// `text` in Unicode NFC.
pub(crate) fn nfc(text: &str) -> String {
    text.nfc().collect()
}

/// Whether `character` can be part of a tag. Obsidian ends a tag at white space, at ASCII
/// punctuation other than `-`, `_` and `/`, and at the Unicode general and supplemental
/// punctuation blocks.
pub(crate) fn is_tag_char(character: char) -> bool {
    if character.is_whitespace() {
        return false;
    }
    if character.is_ascii_punctuation() {
        return matches!(character, '-' | '_' | '/');
    }
    !matches!(character, '\u{2000}'..='\u{206F}' | '\u{2E00}'..='\u{2E7F}')
}

/// Whether `body`, a tag without its `#`, is a tag: one or more tag characters, and not only
/// digits (`#123` is not a tag, `#1a` is).
pub(crate) fn tag_body_is_valid(body: &str) -> bool {
    !body.is_empty() && body.chars().all(is_tag_char) && !body.chars().all(|c| c.is_ascii_digit())
}

/// The text that Obsidian shows for a wikilink without an alias: the target, with each `#` of the
/// subpath shown as ` > `. `Beta#Section One` shows `Beta > Section One`, and `#Top` shows `Top`.
pub(crate) fn wikilink_display(target: &str) -> String {
    let mut parts = target.split('#');
    let linkpath = parts.next().unwrap_or_default();
    let mut shown: Vec<&str> = Vec::new();
    if !linkpath.is_empty() {
        shown.push(linkpath);
    }
    shown.extend(parts.filter(|part| !part.is_empty()));
    shown.join(" > ")
}

/// `text` with each `%XX` escape decoded. An escape that does not decode to UTF-8 stays as it is.
pub(crate) fn percent_decode(text: &str) -> String {
    let bytes = text.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut at = 0;
    while let Some(&byte) = bytes.get(at) {
        let escaped = (byte == b'%')
            .then(|| bytes.get(at + 1..at + 3))
            .flatten()
            .and_then(|hex| std::str::from_utf8(hex).ok())
            .and_then(|hex| u8::from_str_radix(hex, 16).ok());
        if let Some(value) = escaped {
            decoded.push(value);
            at += 3;
        } else {
            decoded.push(byte);
            at += 1;
        }
    }
    String::from_utf8(decoded).unwrap_or_else(|_| text.to_owned())
}

/// Whether `target` starts with a URL scheme, such as `https:` or `mailto:`.
pub(crate) fn has_scheme(target: &str) -> bool {
    let Some((scheme, _)) = target.split_once(':') else {
        return false;
    };
    let mut characters = scheme.chars();
    characters.next().is_some_and(|c| c.is_ascii_alphabetic())
        && characters.all(|c| c.is_ascii_alphanumeric() || matches!(c, '+' | '.' | '-'))
}
