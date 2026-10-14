//! The frontmatter of a note: the YAML block between two `---` lines at the start.

use serde_json::{Map, Number, Value};
use yaml_rust2::{Yaml, YamlLoader};

use crate::index::{FrontmatterLink, NoteIndex};
use crate::text::{nfc, tag_body_is_valid, wikilink_display};

/// The frontmatter key whose values are tags.
const TAGS_KEY: &str = "tags";

/// The frontmatter key whose values are aliases.
const ALIASES_KEY: &str = "aliases";

/// The YAML of the frontmatter and the byte offset where the body starts. `None` and `0` when the
/// note does not start with a frontmatter block.
pub(crate) fn split(text: &str) -> (Option<&str>, usize) {
    let Some(first_end) = line_end(text, 0) else {
        return (None, 0);
    };
    if text.get(..first_end).map(trim_newline) != Some("---") {
        return (None, 0);
    }
    let mut start = first_end;
    while let Some(end) = line_end(text, start) {
        if text.get(start..end).map(trim_newline) == Some("---") {
            return (text.get(first_end..start), end);
        }
        start = end;
    }
    (None, 0)
}

/// The offset after the line that starts at `start`, with its newline. `None` at the end.
fn line_end(text: &str, start: usize) -> Option<usize> {
    let rest = text.get(start..)?;
    if rest.is_empty() {
        return None;
    }
    Some(rest.find('\n').map_or(text.len(), |at| start + at + 1))
}

fn trim_newline(line: &str) -> &str {
    line.trim_end_matches(['\n', '\r'])
}

/// Read the frontmatter `yaml` into `index`: the properties, the tags of the key `tags`, the
/// aliases and the links in the values. A frontmatter that is not a YAML mapping gives nothing.
pub(crate) fn read(yaml: &str, index: &mut NoteIndex) {
    let Ok(documents) = YamlLoader::load_from_str(yaml) else {
        return;
    };
    let Some(Yaml::Hash(hash)) = documents.into_iter().next() else {
        return;
    };
    let mut properties = Map::new();
    for (key, value) in &hash {
        let key = key_text(key);
        if key == TAGS_KEY {
            index.tags.extend(frontmatter_tags(value));
        }
        if key == ALIASES_KEY {
            index.aliases.extend(strings(value));
        }
        collect_links(&key, value, &mut index.frontmatter_links);
        properties.insert(key, json(value));
    }
    index.frontmatter = Some(properties);
}

/// The text of a mapping key.
fn key_text(key: &Yaml) -> String {
    match key {
        Yaml::String(text) | Yaml::Real(text) => text.clone(),
        Yaml::Integer(number) => number.to_string(),
        Yaml::Boolean(flag) => flag.to_string(),
        _ => String::new(),
    }
}

/// A YAML value as JSON. Plain scalars follow the YAML 1.2 core schema.
fn json(value: &Yaml) -> Value {
    match value {
        Yaml::String(text) => Value::String(text.clone()),
        Yaml::Integer(number) => Value::Number(Number::from(*number)),
        Yaml::Real(text) => text
            .parse::<f64>()
            .ok()
            .and_then(Number::from_f64)
            .map_or_else(|| Value::String(text.clone()), Value::Number),
        Yaml::Boolean(flag) => Value::Bool(*flag),
        Yaml::Array(items) => Value::Array(items.iter().map(json).collect()),
        Yaml::Hash(hash) => Value::Object(
            hash.iter()
                .map(|(key, value)| (key_text(key), json(value)))
                .collect(),
        ),
        _ => Value::Null,
    }
}

/// The strings of a value that is one string or a list of strings.
fn strings(value: &Yaml) -> Vec<String> {
    match value {
        Yaml::String(text) => vec![text.clone()],
        Yaml::Array(items) => items
            .iter()
            .filter_map(|item| match item {
                Yaml::String(text) => Some(text.clone()),
                _ => None,
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// The tags of the key `tags`: each value that is a whole tag, with or without its `#`. A string
/// that holds more than one tag, such as `"a, b"`, is not a tag.
fn frontmatter_tags(value: &Yaml) -> Vec<String> {
    strings(value)
        .into_iter()
        .filter_map(|tag| {
            let body = tag.strip_prefix('#').unwrap_or(&tag);
            tag_body_is_valid(body).then(|| format!("#{body}"))
        })
        .collect()
}

/// The links in the values of `key`: each string value that is one wikilink. A list item has the
/// key `<key>.<position>`.
fn collect_links(key: &str, value: &Yaml, links: &mut Vec<FrontmatterLink>) {
    match value {
        Yaml::String(text) => {
            if let Some(target) = whole_wikilink(text) {
                let (target, alias) = target
                    .split_once('|')
                    .map_or((target, None), |(target, alias)| (target, Some(alias)));
                links.push(FrontmatterLink {
                    key: key.to_owned(),
                    link: nfc(target),
                    display_text: alias.map_or_else(|| wikilink_display(target), str::to_owned),
                });
            }
        }
        Yaml::Array(items) => {
            for (position, item) in items.iter().enumerate() {
                collect_links(&format!("{key}.{position}"), item, links);
            }
        }
        Yaml::Hash(hash) => {
            for (inner, item) in hash {
                collect_links(&format!("{key}.{}", key_text(inner)), item, links);
            }
        }
        _ => {}
    }
}

/// The target of a string that is exactly one `[[target]]`.
fn whole_wikilink(text: &str) -> Option<&str> {
    let inner = text.trim().strip_prefix("[[")?.strip_suffix("]]")?;
    (!inner.is_empty() && !inner.contains("[[") && !inner.contains("]]")).then_some(inner)
}
