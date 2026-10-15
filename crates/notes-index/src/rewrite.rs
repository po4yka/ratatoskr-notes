//! The links that a rename or a move rewrites, with the rules of Obsidian's "Automatically update
//! internal links" (workspace change `add-notes-write-back`, spec `notes-link-rewrite`).

use std::collections::BTreeMap;

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
/// each Markdown note by its path.
#[must_use]
pub fn rewrite_links(
    paths: &[String],
    notes: &BTreeMap<String, String>,
    from: &str,
    to: &str,
    format: NewLinkFormat,
) -> BTreeMap<String, String> {
    let _ = (paths, notes, from, to, format);
    BTreeMap::new()
}
