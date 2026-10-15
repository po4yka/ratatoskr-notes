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

#[test]
fn the_nested_link_is_rewritten_and_not_its_copy_in_code_a_comment_or_math() {
    for (before, after) in [
        (
            "[`[[A]]` [[A]]](A.md)\n",
            "[`[[A]]` [[B\u{e9}]]](B\u{e9}.md)\n",
        ),
        (
            "[<!-- [[A]] --> [[A]]](A.md)\n",
            "[<!-- [[A]] --> [[B\u{e9}]]](B\u{e9}.md)\n",
        ),
        (
            "[$[[A]]$ [[A]]](A.md)\n",
            "[$[[A]]$ [[B\u{e9}]]](B\u{e9}.md)\n",
        ),
    ] {
        assert_eq!(
            rewritten(before, "A.md", "B\u{e9}.md").as_deref(),
            Some(after),
            "the link is rewritten where it stands in {before:?}"
        );
    }
}

#[test]
fn two_nested_links_in_one_text_are_both_rewritten() {
    assert_eq!(
        rewritten("[x [[A]] y [[A]]](A.md)\n", "A.md", "B\u{e9}.md").as_deref(),
        Some("[x [[B\u{e9}]] y [[B\u{e9}]]](B\u{e9}.md)\n")
    );
}

#[test]
fn a_markdown_link_in_the_alias_of_a_wikilink_is_rewritten_where_it_stands() {
    assert_eq!(
        rewritten("[[A|[x](A.md)]]\n", "A.md", "B\u{e9}.md").as_deref(),
        Some("[[B\u{e9}|[x](B\u{e9}.md)]]\n"),
        "the alias moves with the new target, and its link is rewritten"
    );
    assert_eq!(
        rewritten("[[A|`[x](A.md)`[x](A.md)]]\n", "A.md", "ABCDEFGHIJK.md").as_deref(),
        Some("[[ABCDEFGHIJK|`[x](A.md)`[x](ABCDEFGHIJK.md)]]\n"),
        "the copy in the code span keeps its text"
    );
}

/// The pieces of a random note: each with its text before the rename of `A.md` to `Bé.md`, and
/// after it. A piece that holds no link to `A.md` is the same after.
const PIECES: [(&str, &str); 18] = [
    ("[[A]]", "[[B\u{e9}]]"),
    ("![[A]]", "![[B\u{e9}]]"),
    ("[[A#Part|the part]]", "[[B\u{e9}#Part|the part]]"),
    ("[x](A.md)", "[x](B\u{e9}.md)"),
    ("![img](A.md)", "![img](B\u{e9}.md)"),
    ("[[A|[x](A.md)]]", "[[B\u{e9}|[x](B\u{e9}.md)]]"),
    ("[y [[A]]](A.md)", "[y [[B\u{e9}]]](B\u{e9}.md)"),
    ("[`[[A]]` [[A]]](A.md)", "[`[[A]]` [[B\u{e9}]]](B\u{e9}.md)"),
    (
        "[[A|`[x](A.md)`[x](A.md)]]",
        "[[B\u{e9}|`[x](A.md)`[x](B\u{e9}.md)]]",
    ),
    ("`[[A]]`", "`[[A]]`"),
    ("<!-- [[A]] -->", "<!-- [[A]] -->"),
    ("$[[A]]$", "$[[A]]$"),
    ("[[Other]]", "[[Other]]"),
    (
        "\u{fc}ber \u{e9}t\u{e9} \u{1f600}",
        "\u{fc}ber \u{e9}t\u{e9} \u{1f600}",
    ),
    ("\r\n", "\r\n"),
    ("\n", "\n"),
    (" ", " "),
    ("plain text", "plain text"),
];

/// A small deterministic generator, so that a failure is found again from its seed.
struct Lcg(u64);

impl Lcg {
    fn below(&mut self, bound: usize) -> usize {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        usize::try_from(self.0 >> 33).unwrap_or(0) % bound
    }
}

#[test]
fn every_link_of_a_random_note_is_rewritten_and_no_other_byte_changes() {
    for seed in 1..=500_u64 {
        let mut random = Lcg(seed);
        let (mut before, mut after) = (String::new(), String::new());
        for _ in 0..=random.below(12) {
            let (old, new) = PIECES[random.below(PIECES.len())];
            // A space between two pieces keeps each one a piece of its own.
            before.push_str(old);
            before.push(' ');
            after.push_str(new);
            after.push(' ');
        }
        before.push('\n');
        after.push('\n');
        let paths = vec![
            "A.md".to_owned(),
            "Other.md".to_owned(),
            "Note.md".to_owned(),
        ];
        let notes = BTreeMap::from([("Note.md".to_owned(), before.clone())]);
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            rewrite_links(
                &paths,
                &notes,
                "A.md",
                "B\u{e9}.md",
                NewLinkFormat::Shortest,
            )
        }));
        let got = result.ok().map(|rewritten| {
            rewritten
                .get("Note.md")
                .cloned()
                .unwrap_or_else(|| before.clone())
        });
        assert_eq!(
            got.as_deref(),
            Some(after.as_str()),
            "seed {seed}: {before:?}"
        );
    }
}
