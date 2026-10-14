//! The index of a Markdown note in an Obsidian vault, and the resolution of its links with the
//! rules of the Obsidian engine (workspace design D8).
//!
//! The crate does no input or output. [`parse`] reads the text of one note and returns its
//! [`NoteIndex`]: the frontmatter, the tags, the headings, the block ids, the aliases, and every
//! link and embed as the note writes it. [`Vault`] holds the paths of every file of one vault, and
//! [`LinkGraph`] resolves the links of every note against them and gives the backlinks of each
//! file.
//!
//! The oracle is the golden output of the real Obsidian engine under
//! `tests/fixtures/obsidian-<version>/`. A new Obsidian version adds a new directory there.

mod index;
mod resolve;

pub use crate::index::{FrontmatterLink, Heading, Link, LinkForm, NoteIndex, parse};
pub use crate::resolve::{LinkCounts, LinkGraph, Vault};
