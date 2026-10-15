//! The links that a rename or a move rewrites, with the rules of Obsidian's "Automatically update
//! internal links" (workspace change `add-notes-write-back`, spec `notes-link-rewrite`).
//!
//! The rules are those of the Obsidian app 1.14.4 (`fileManager.renameFile`), and the golden
//! fixtures that the real app recorded are the oracle (`services/notes/tests/link_rewrite_golden.rs`
//! in this repository):
//!
//! 1. Before the rename, each link and embed of each note whose path part is not empty is resolved
//!    to the list of files that it can name, the best first ([`Files::linkpath_dest`]).
//! 2. After the rename, each of those links is resolved again from the path of its note, which may
//!    have moved. When the list is empty or differs, the link is written again for the file that
//!    it named first, from the path of its note: the name alone when it names one file
//!    (`shortest`), or the path from the folder of the note (`relative`) or from the vault root
//!    (`absolute`), followed by the heading or block of the link ([`Files::link_text`]).
//! 3. A wikilink keeps its form, its `!` and its alias; a Markdown link keeps its text, its title
//!    and its angle brackets, and encodes each space of the new path as `%20`
//!    ([`written_again`]). A text or an alias that was the old name becomes the new name.
//!
//! Rule 2 also writes again a link to ANOTHER file when the rename made the name of that file
//! ambiguous: after a move to `Targets/Other/Epsilon.md`, `[[Epsilon]]`, which named
//! `Targets/Epsilon.md`, becomes `[[Targets/Epsilon]]` (fixture `move-into-name-collision`).
//!
//! Links in code, in HTML comments and in math are not links (the reader of [`crate::markdown`]
//! blanks them). Links in the frontmatter are not rewritten: Obsidian writes the whole frontmatter
//! again with its own YAML writer, which notes cannot copy byte for byte.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::ops::Range;

use crate::index::LinkForm;
use crate::markdown::{self, Reference};
use crate::text::{nfc, percent_decode};

/// How Obsidian writes the path of a link that it writes again: the vault setting
/// `newLinkFormat`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NewLinkFormat {
    /// `shortest`: the name alone when it names one file, else the path from the vault root.
    #[default]
    Shortest,
    /// `relative`: the path from the folder of the linking note.
    Relative,
    /// `absolute`: the path from the vault root.
    Absolute,
}

/// The texts of the Markdown notes that the rename of `from` to `to` changes, by their path after
/// the rename. `paths` holds every file of the vault before the rename, and `notes` the text of
/// each Markdown note by its path. All paths are relative to the vault root, with `/`, in NFC.
#[must_use]
pub fn rewrite_links(
    paths: &[String],
    notes: &BTreeMap<String, String>,
    from: &str,
    to: &str,
    format: NewLinkFormat,
) -> BTreeMap<String, String> {
    let moved = |path: &str| if path == from { to } else { path }.to_owned();
    let before = Files::new(paths.iter().cloned());
    // Obsidian takes the renamed file out of its name lookup and puts it in again at the end.
    let after = Files::new(
        paths
            .iter()
            .filter(|path| path.as_str() != from)
            .cloned()
            .chain(paths.iter().any(|path| path == from).then(|| to.to_owned())),
    );
    let mut rewritten = BTreeMap::new();
    for (source, text) in notes {
        let target_source = moved(source);
        let mut changes: Vec<(Range<usize>, String)> = Vec::new();
        for (range, link) in links(text) {
            let linkpath = link_path(&link.target);
            let named = before.linkpath_dest(linkpath, source);
            let Some(first) = named.first() else {
                continue;
            };
            let named_now = after.linkpath_dest(linkpath, &target_source);
            if !named_now.is_empty() && named_now == named {
                continue;
            }
            let wiki = link.form == LinkForm::Wiki;
            let new_link = after.link_text(&moved(first), &target_source, wiki, format)
                + subpath(&link.target);
            if new_link != link.target {
                changes.push((range, written_again(&link, &new_link)));
            }
        }
        if !changes.is_empty() {
            rewritten.insert(target_source, apply(text, changes));
        }
    }
    rewritten
}

/// One link of a note: the text from `[[` or `[` to its end, its form, and its target as Obsidian
/// reads it (the `link` of the metadata cache), with its heading or block.
#[derive(Debug)]
struct NoteLink {
    original: String,
    form: LinkForm,
    target: String,
}

/// The links of the body of `text` and where each one is.
fn links(text: &str) -> Vec<(Range<usize>, NoteLink)> {
    let (_, body_start) = crate::frontmatter::split(text);
    let body = text.get(body_start..).unwrap_or_default();
    markdown::references(body)
        .into_iter()
        .filter_map(|Reference { range, form }| {
            let original = body.get(range.clone())?.to_owned();
            let target = match form {
                LinkForm::Wiki => wikilink_target(&original),
                LinkForm::Markdown => {
                    markdown_parts(&original).map(|parts| markdown_target(parts.destination))?
                }
            };
            let range = range.start + body_start..range.end + body_start;
            Some((
                range,
                NoteLink {
                    original,
                    form,
                    target,
                },
            ))
        })
        .collect()
}

/// The target of a wikilink as Obsidian reads it: the text before the first `|` that is not the
/// first character, trimmed, without a last `\`, with each no-break space as a space, in NFC.
fn wikilink_target(original: &str) -> String {
    let inner = original
        .trim_start_matches('!')
        .strip_prefix("[[")
        .and_then(|rest| rest.strip_suffix("]]"))
        .unwrap_or_default();
    let target = match inner.find('|') {
        Some(at) if at > 0 => inner.get(..at).unwrap_or_default(),
        _ => inner,
    };
    let target = target.trim();
    let target = target.strip_suffix('\\').unwrap_or(target);
    nfc(target.replace('\u{a0}', " ").trim())
}

/// The target of a Markdown link as Obsidian reads it: without angle brackets, decoded, in NFC.
fn markdown_target(destination: &str) -> String {
    let destination = destination
        .strip_prefix('<')
        .and_then(|rest| rest.strip_suffix('>'))
        .map_or(destination, str::trim);
    nfc(&percent_decode(destination))
}

/// The path part of a link target: the text before the first `#`.
fn link_path(target: &str) -> &str {
    target.split_once('#').map_or(target, |(path, _)| path)
}

/// The heading or block part of a link target: the text from the first `#`, or empty.
fn subpath(target: &str) -> &str {
    target
        .find('#')
        .and_then(|at| target.get(at..))
        .unwrap_or("")
}

/// `text` with each range of `changes` replaced by its new text. The ranges do not overlap.
fn apply(text: &str, mut changes: Vec<(Range<usize>, String)>) -> String {
    changes.sort_by_key(|(range, _)| std::cmp::Reverse(range.start));
    let mut result = text.to_owned();
    for (range, new) in changes {
        result.replace_range(range, &new);
    }
    result
}

/// The files of a vault and the lookup of each name, as Obsidian's metadata cache holds them.
#[derive(Debug)]
struct Files {
    /// Each path, by the file name in lower case, in the order in which the files came.
    by_name: BTreeMap<String, Vec<String>>,
    /// Each path in lower case, to the path.
    by_path: BTreeMap<String, String>,
}

impl Files {
    fn new(paths: impl Iterator<Item = String>) -> Self {
        let mut by_name: BTreeMap<String, Vec<String>> = BTreeMap::new();
        let mut by_path = BTreeMap::new();
        for path in paths {
            by_name
                .entry(name_of(&path).to_lowercase())
                .or_default()
                .push(path.clone());
            by_path.insert(path.to_lowercase(), path);
        }
        Self { by_name, by_path }
    }

    /// The file whose path is `path`, or `path.md`.
    fn exact(&self, path: &str) -> Option<String> {
        let found = |path: &str| {
            self.by_path
                .get(&path.to_lowercase())
                .filter(|p| *p == path)
        };
        found(path)
            .or_else(|| found(&format!("{path}.md")))
            .cloned()
    }

    /// The files that the link path `linkpath` of the note `source` can name, the best first:
    /// Obsidian's `metadataCache.getLinkpathDest`.
    fn linkpath_dest(&self, linkpath: &str, source: &str) -> Vec<String> {
        if linkpath.is_empty() {
            return self.exact(source).into_iter().collect();
        }
        let lower = linkpath.to_lowercase();
        let with_extension = format!("{linkpath}.md").to_lowercase();
        let by_name = name_of(&lower)
            .contains('.')
            .then(|| self.by_name.get(name_of(&lower)))
            .flatten();
        let (mut wanted, candidates) = match by_name {
            Some(found) => (lower, found),
            None => match self.by_name.get(name_of(&with_extension)) {
                Some(found) => (with_extension, found),
                None => return Vec::new(),
            },
        };
        if name_of(&wanted) == wanted && candidates.len() == 1 {
            return candidates.clone();
        }
        let mut folder = parent_of(source).to_lowercase();
        if wanted.starts_with("./") || wanted.starts_with("../") {
            wanted = relative_to(&mut folder, &wanted);
            if let Some(found) = candidates.iter().find(|path| path.to_lowercase() == wanted) {
                return vec![found.clone()];
            }
        }
        let wanted = wanted.strip_prefix('/').unwrap_or(&wanted).to_owned();
        if let Some(found) = candidates.iter().find(|path| path.to_lowercase() == wanted) {
            return vec![found.clone()];
        }
        if linkpath.starts_with('/') {
            return Vec::new();
        }
        let (mut near, mut far): (Vec<String>, Vec<String>) = candidates
            .iter()
            .filter(|path| path.to_lowercase().ends_with(&wanted))
            .cloned()
            .partition(|path| path.to_lowercase().starts_with(&folder));
        near.sort_by_key(|path| js_length(path));
        far.sort_by_key(|path| js_length(path));
        near.extend(far);
        near
    }

    /// The path that Obsidian writes in a link from `source` to `file`: its
    /// `metadataCache.fileToLinktext`. A Markdown note loses its `.md` in a wikilink.
    fn link_text(&self, file: &str, source: &str, wiki: bool, format: NewLinkFormat) -> String {
        let omit_extension = wiki && extension_of(name_of(file)) == "md";
        let path = if omit_extension {
            without_extension(file)
        } else {
            file
        };
        match format {
            NewLinkFormat::Absolute => path.to_owned(),
            NewLinkFormat::Relative => relative_path(path, source),
            NewLinkFormat::Shortest => {
                let name = if omit_extension {
                    stem_of(file)
                } else {
                    name_of(file)
                };
                let named = self.linkpath_dest(link_path(name), source);
                if named.len() == 1 && named.first().is_some_and(|only| only == file) {
                    name.to_owned()
                } else {
                    path.to_owned()
                }
            }
        }
    }
}

/// `wanted`, which starts with `./` or `../`, as a path from the vault root for a note in
/// `folder`. Like Obsidian, it leaves `folder` with the trailing `/` it added or the folders it
/// went up.
fn relative_to(folder: &mut String, wanted: &str) -> String {
    let mut wanted = wanted
        .strip_prefix("./")
        .filter(|rest| rest.starts_with("../"))
        .unwrap_or(wanted);
    if let Some(rest) = wanted.strip_prefix("./") {
        if !folder.is_empty() {
            folder.push('/');
        }
        return format!("{folder}{rest}");
    }
    while let Some(rest) = wanted.strip_prefix("../") {
        wanted = rest;
        *folder = parent_of(folder).to_owned();
    }
    if !folder.is_empty() {
        folder.push('/');
    }
    format!("{folder}{wanted}")
}

/// `path` relative to the folder of `source`, as Obsidian writes it for `relative`.
fn relative_path(path: &str, source: &str) -> String {
    let mut up = String::new();
    let mut folder = parent_of(source);
    while !folder.is_empty() && folder != "/" && !path.starts_with(&format!("{folder}/")) {
        up.insert_str(0, "../");
        folder = parent_of(folder);
    }
    match path.strip_prefix(&format!("{folder}/")) {
        Some(rest) => up + rest,
        None => up + path,
    }
}

/// The text of the link `link` written again with the target `new_link`: Obsidian's link updater.
fn written_again(link: &NoteLink, new_link: &str) -> String {
    match link.form {
        LinkForm::Wiki => wikilink_again(&link.original, new_link),
        LinkForm::Markdown => markdown_again(link, new_link),
    }
}

fn wikilink_again(original: &str, new_link: &str) -> String {
    let prefix = if original.starts_with('!') {
        "![["
    } else {
        "[["
    };
    let inner = original
        .strip_prefix(prefix)
        .and_then(|rest| rest.strip_suffix("]]"))
        .unwrap_or_default();
    let Some((target, alias)) = inner.split_once('|').filter(|(_, alias)| !alias.is_empty()) else {
        return format!("{prefix}{new_link}]]");
    };
    let pipe = if original.contains("\\|") { "\\|" } else { "|" };
    let alias = if target.contains('/') && stem_of(target) == alias.trim() {
        stem_of(link_path(new_link))
    } else {
        alias
    };
    format!("{prefix}{new_link}{pipe}{alias}]]")
}

fn markdown_again(link: &NoteLink, new_link: &str) -> String {
    // `links` reads only a Markdown link whose parts match the pattern.
    let Some(parts) = markdown_parts(&link.original) else {
        return link.original.clone();
    };
    let destination = if parts.destination.starts_with('<') {
        format!("<{new_link}>")
    } else {
        encoded(new_link)
    };
    let shown = parts.text.replace(['\u{a0}', '\u{202f}'], " ");
    let shown = shown.trim();
    let old = link_path(&link.target);
    let text = if shown == display_name(old) {
        display_name(link_path(new_link)).to_owned()
    } else if shown.contains('/') && shown == without_md(old) {
        without_md(link_path(new_link)).to_owned()
    } else {
        parts.text.to_owned()
    };
    format!(
        "{}{text}{}{destination}{}{}",
        parts.open, parts.separator, parts.title, parts.close
    )
}

/// The parts of a Markdown link `[text](destination "title")`, as Obsidian's pattern reads them.
#[derive(Debug, Clone, Copy)]
struct MarkdownParts<'a> {
    /// `[` or `![`.
    open: &'a str,
    text: &'a str,
    /// `](` and the white space after it.
    separator: &'a str,
    destination: &'a str,
    /// The white space and the title after the destination, or empty.
    title: &'a str,
    /// The white space before the `)`, and the `)`.
    close: &'a str,
}

fn markdown_parts(original: &str) -> Option<MarkdownParts<'_>> {
    let open_length = if original.starts_with("![") { 2 } else { 1 };
    let (open, rest) = original.split_at_checked(open_length)?;
    if !open.ends_with('[') {
        return None;
    }
    let mut from = 0;
    while let Some(at) = rest
        .get(from..)
        .and_then(|tail| tail.find("]("))
        .map(|at| from + at)
    {
        let (text, after) = rest.split_at_checked(at)?;
        if let Some(parts) = markdown_tail(open, text, after) {
            return Some(parts);
        }
        from = at + 1;
    }
    None
}

/// The parts after the text, from `](` on, or `None` when they do not match the pattern.
fn markdown_tail<'a>(open: &'a str, text: &'a str, after: &'a str) -> Option<MarkdownParts<'a>> {
    let body = after.strip_prefix("](")?;
    let leading = body.len() - body.trim_start().len();
    let separator = after.get(..2 + leading)?;
    let body = body.get(leading..)?;
    let without_paren = body.strip_suffix(')')?;
    let core = without_paren.trim_end();
    let close = body.get(core.len()..)?;
    if core.is_empty() {
        return None;
    }
    let destination_end = if core.starts_with('<') {
        core.find('>')? + 1
    } else {
        core.find(char::is_whitespace).unwrap_or(core.len())
    };
    let (destination, title) = core.split_at_checked(destination_end)?;
    if !destination.starts_with('<') && destination.contains(['"', ' ']) {
        return None;
    }
    if !title.is_empty() && !is_title(title) {
        return None;
    }
    Some(MarkdownParts {
        open,
        text,
        separator,
        destination,
        title,
        close,
    })
}

/// Whether `title`, white space and a title, matches Obsidian's pattern of a link title.
fn is_title(title: &str) -> bool {
    let word = title.trim_start();
    if word.len() == title.len() {
        return false;
    }
    let quoted = |open: char, close: char| {
        word.strip_prefix(open)
            .and_then(|rest| rest.strip_suffix(close))
            .is_some_and(|inner| !inner.is_empty() && !inner.contains(close))
    };
    !word.contains(' ') || quoted('"', '"') || quoted('\'', '\'') || quoted('(', ')')
}

/// `link` with `\`, the control characters that Obsidian names and each space percent-encoded.
fn encoded(link: &str) -> String {
    let mut text = String::with_capacity(link.len());
    for character in link.chars() {
        if matches!(
            character,
            '\\' | ' ' | '\u{0}' | '\u{8}' | '\u{b}' | '\u{c}' | '\u{e}'..='\u{1f}'
        ) {
            let _ = write!(text, "%{:02X}", u32::from(character));
        } else {
            text.push(character);
        }
    }
    text
}

/// The text after the last `/` of `path`.
fn name_of(path: &str) -> &str {
    path.rsplit_once('/').map_or(path, |(_, name)| name)
}

/// The text before the last `/` of `path`, or empty.
fn parent_of(path: &str) -> &str {
    path.rsplit_once('/').map_or("", |(folder, _)| folder)
}

/// The position of the `.` of an extension in `text`: not the first or the last character.
fn extension_dot(text: &str) -> Option<usize> {
    text.rfind('.')
        .filter(|at| *at != 0 && *at + 1 != text.len())
}

/// The extension of `name` in lower case, or empty.
fn extension_of(name: &str) -> String {
    extension_dot(name)
        .and_then(|at| name.get(at + 1..))
        .unwrap_or_default()
        .to_lowercase()
}

/// `text` without its extension.
fn without_extension(text: &str) -> &str {
    extension_dot(text)
        .and_then(|at| text.get(..at))
        .unwrap_or(text)
}

/// The name of `path` without its extension.
fn stem_of(path: &str) -> &str {
    without_extension(name_of(path))
}

/// The name of `path` as Obsidian shows a note: without `.md`.
fn display_name(path: &str) -> &str {
    let name = name_of(path);
    if extension_of(name) == "md" {
        stem_of(name)
    } else {
        name
    }
}

/// `path` without `.md` when it names a note.
fn without_md(path: &str) -> &str {
    if extension_of(name_of(path)) == "md" {
        without_extension(path)
    } else {
        path
    }
}

/// The length of `text` in UTF-16 code units, as JavaScript counts it.
fn js_length(text: &str) -> usize {
    text.encode_utf16().count()
}
