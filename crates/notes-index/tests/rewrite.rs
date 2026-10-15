//! The link rewrite of a link nested in the text of another (workspace change
//! `add-notes-write-back`, spec `notes-link-rewrite`, task 8.8).
//!
//! The wikilink reader also reads the text of a Markdown link, so `[x [[A]]](A.md)` holds two links
//! whose ranges overlap. Obsidian 1.14.4 writes both into the same bytes and corrupts the note
//! (`[x [[A]]](Bé.md))`). Notes rewrites the inner link inside the text of the outer one.

use std::collections::BTreeMap;
use std::panic::AssertUnwindSafe;

use ratatoskr_notes_index::{NewLinkFormat, rewrite_links};

/// The text of `Note.md` after `from` is renamed to `to` in a vault of `Note.md` with `text` and
/// the file `from`, or `None` when the rewrite panicked.
fn rewritten(text: &str, from: &str, to: &str) -> Option<String> {
    let paths = vec![from.to_owned(), "Note.md".to_owned()];
    let notes = BTreeMap::from([("Note.md".to_owned(), text.to_owned())]);
    std::panic::catch_unwind(AssertUnwindSafe(|| {
        rewrite_links(&paths, &notes, from, to, NewLinkFormat::Shortest)
    }))
    .ok()
    .map(|rewritten| {
        rewritten
            .get("Note.md")
            .cloned()
            .unwrap_or_else(|| text.to_owned())
    })
}

#[test]
fn a_wikilink_in_the_text_of_a_markdown_link_is_rewritten_inside_it() {
    assert_eq!(
        rewritten("x [\u{fc} [[A]] \u{fc}](A.md) y\n", "A.md", "B\u{e9}.md").as_deref(),
        Some("x [\u{fc} [[B\u{e9}]] \u{fc}](B\u{e9}.md) y\n"),
        "both links name the new path, and no other byte changes"
    );
}

#[test]
fn a_nested_link_before_a_multibyte_character_does_not_panic() {
    assert_eq!(
        rewritten("[x [[Ab]]](Ab.md)\u{e9}\n", "Ab.md", "B.md").as_deref(),
        Some("[x [[B]]](B.md)\u{e9}\n"),
        "the rewrite ends, and the text stays valid"
    );
}
