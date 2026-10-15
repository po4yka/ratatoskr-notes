//! The change of one frontmatter property of a Markdown file, which notes applies itself on the
//! `sync` channel (workspace change `add-notes-write-back`, `set_property`).
//!
//! The change edits lines and keeps every other byte: the other keys, their order, comments, the
//! line endings and the body stay as they are. A property is a key at the start of a line of the
//! frontmatter, with the lines below it that are indented or start a list item. The value is
//! written as compact JSON, which is valid YAML: `status: "done"`, `tags: ["a","b"]`.

use serde_json::Value;

/// The bytes of `file` with the property `key` set to `value`, or removed when `value` is `None`.
/// A file without frontmatter gets one for a set. Returns `None` when the file is not UTF-8 or its
/// frontmatter has no end, because a change could not keep the rest of it.
#[must_use]
pub(crate) fn change(file: &[u8], key: &str, value: Option<&Value>) -> Option<Vec<u8>> {
    let text = std::str::from_utf8(file).ok()?;
    let mut lines: Vec<String> = text.split_inclusive('\n').map(str::to_owned).collect();
    if lines.first().map(|first| bare(first)) != Some("---") {
        return Some(match value {
            Some(value) => format!("---\n{}: {value}\n---\n{text}", render_key(key)).into_bytes(),
            None => file.to_vec(),
        });
    }
    let close = lines
        .iter()
        .skip(1)
        .position(|line| matches!(bare(line), "---" | "..."))?
        .checked_add(1)?;
    let found = (1..close).find_map(|at| {
        let (found, written) = key_of(lines.get(at)?)?;
        (found == key).then(|| (at, written.to_owned()))
    });
    match (found, value) {
        (Some((start, written)), value) => {
            let mut end = start + 1;
            while end < close && lines.get(end).is_some_and(|line| continues(line)) {
                end += 1;
            }
            // The key keeps the form in which the file writes it.
            lines.splice(
                start..end,
                value.map(|value| format!("{written}: {value}\n")),
            );
        }
        (None, Some(value)) => lines.insert(close, format!("{}: {value}\n", render_key(key))),
        (None, None) => {}
    }
    Some(lines.concat().into_bytes())
}

/// A line without its line ending.
fn bare(line: &str) -> &str {
    line.trim_end_matches(['\n', '\r'])
}

/// Whether `line` belongs to the value of the key above it: it is indented, or it is a list item.
fn continues(line: &str) -> bool {
    line.starts_with([' ', '\t']) || line.starts_with("- ") || bare(line) == "-"
}

/// The key that `line` starts, when it starts one, and the text that writes it: a plain key, or a
/// key in double or single quotes, followed by `:` and a space or the end of the line.
fn key_of(line: &str) -> Option<(String, &str)> {
    let line = bare(line);
    let (key, rest) = if line.starts_with('"') {
        let mut values = serde_json::Deserializer::from_str(line).into_iter::<String>();
        let key = values.next()?.ok()?;
        (key, line.get(values.byte_offset()..)?)
    } else if let Some(quoted) = line.strip_prefix('\'') {
        let end = quoted.find('\'')?;
        (quoted.get(..end)?.to_owned(), quoted.get(end + 1..)?)
    } else {
        if line.starts_with([' ', '\t', '#', '-']) || line.is_empty() {
            return None;
        }
        let colon = line
            .char_indices()
            .find(|&(at, character)| {
                character == ':'
                    && line
                        .get(at + 1..)
                        .is_some_and(|after| after.is_empty() || after.starts_with([' ', '\t']))
            })
            .map(|(at, _)| at)?;
        (line.get(..colon)?.trim_end().to_owned(), line.get(colon..)?)
    };
    let after = rest.strip_prefix(':')?;
    let written = line.get(..line.len() - rest.len())?.trim_end();
    (after.is_empty() || after.starts_with([' ', '\t'])).then_some((key, written))
}

/// `key` as a plain YAML key when it is one, otherwise in double quotes.
fn render_key(key: &str) -> String {
    let plain = key
        .chars()
        .next()
        .is_some_and(|first| first.is_alphanumeric() || first == '_')
        && !key.ends_with(' ')
        && key
            .chars()
            .all(|character| character.is_alphanumeric() || "_- .".contains(character));
    if plain {
        key.to_owned()
    } else {
        Value::String(key.to_owned()).to_string()
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::change;

    #[test]
    fn a_quoted_key_is_found_and_kept_quoted() {
        let file = b"---\n\"my key\": 1\nother: 2\n---\nbody\n";
        let changed = change(file, "my key", Some(&json!(3))).expect("an editable file");
        assert_eq!(changed, b"---\n\"my key\": 3\nother: 2\n---\nbody\n");
    }

    #[test]
    fn a_block_scalar_continues_over_a_blank_line() {
        let file = b"---\ndesc: |\n  one\n\n  two\nstatus: a\n---\nbody\n";
        let changed = change(file, "desc", Some(&json!("x"))).expect("an editable file");
        assert_eq!(
            String::from_utf8_lossy(&changed),
            "---\ndesc: \"x\"\nstatus: a\n---\nbody\n"
        );
    }

    #[test]
    fn a_list_continues_over_blank_lines_between_items() {
        let file = b"---\ntags:\n  - a\n\n  - b\nplain:\n- c\n  \n- d\nstatus: a\n---\nbody\n";
        let without_tags = change(file, "tags", None).expect("an editable file");
        assert_eq!(
            String::from_utf8_lossy(&without_tags),
            "---\nplain:\n- c\n  \n- d\nstatus: a\n---\nbody\n"
        );
        let without_plain = change(&without_tags, "plain", None).expect("an editable file");
        assert_eq!(
            String::from_utf8_lossy(&without_plain),
            "---\nstatus: a\n---\nbody\n"
        );
    }

    #[test]
    fn a_blank_line_before_the_next_key_ends_the_value() {
        let file = b"---\ntitle: T\n\nstatus: a\n---\nbody\n";
        let changed = change(file, "title", Some(&json!("U"))).expect("an editable file");
        assert_eq!(
            String::from_utf8_lossy(&changed),
            "---\ntitle: \"U\"\n\nstatus: a\n---\nbody\n"
        );
    }

    #[test]
    fn an_unterminated_frontmatter_is_not_changed() {
        assert_eq!(change(b"---\nkey: 1\nbody\n", "key", Some(&json!(2))), None);
    }
}
