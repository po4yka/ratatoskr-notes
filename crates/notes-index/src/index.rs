//! What the index of one note holds.

use serde_json::{Map, Value};

/// The index of one Markdown note.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct NoteIndex {
    /// The frontmatter as JSON, or `None` when the note has none.
    pub frontmatter: Option<Map<String, Value>>,
    /// The tags of the frontmatter key `tags`, then the tags of the text, each with its `#`, in
    /// the order of the note. Tags are not normalised.
    pub tags: Vec<String>,
    /// The headings, in the order of the note.
    pub headings: Vec<Heading>,
    /// The block ids, without the `^`, in the order of the note.
    pub blocks: Vec<String>,
    /// The values of the frontmatter key `aliases`. An alias never resolves a link.
    pub aliases: Vec<String>,
    /// The links of the text, in the order of the note.
    pub links: Vec<Link>,
    /// The embeds of the text, in the order of the note.
    pub embeds: Vec<Link>,
    /// The links in the values of the frontmatter.
    pub frontmatter_links: Vec<FrontmatterLink>,
}

/// One heading.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Heading {
    /// 1 to 6.
    pub level: u8,
    /// The text of the heading as the note writes it.
    pub heading: String,
}

/// How a link is written.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LinkForm {
    /// `[[target]]`, `[[target|alias]]`, `![[target]]`.
    Wiki,
    /// `[text](target)`, `![text](target)`.
    Markdown,
}

/// One link or embed of the text.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct Link {
    /// The target with its subpath, in NFC: `Beta#Section One`, `../Beta.md`.
    pub link: String,
    /// The text that Obsidian shows for the link.
    pub display_text: String,
    /// How the link is written.
    pub form: LinkForm,
}

impl Link {
    /// The part of the target before the first `#`: the file that the link names.
    #[must_use]
    pub fn linkpath(&self) -> &str {
        self.link
            .split_once('#')
            .map_or(self.link.as_str(), |(path, _)| path)
    }

    /// The part of the target from the first `#`: a heading or a block. Empty when there is none.
    #[must_use]
    pub fn subpath(&self) -> &str {
        self.link
            .find('#')
            .map_or("", |at| self.link.get(at..).unwrap_or(""))
    }
}

/// One link in a value of the frontmatter.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct FrontmatterLink {
    /// The key of the value: `related`, or `links.0` for the first item of the list `links`.
    pub key: String,
    /// The target with its subpath, in NFC.
    pub link: String,
    /// The text that Obsidian shows for the link.
    pub display_text: String,
}

/// The index of the note `text`.
#[must_use]
pub fn parse(text: &str) -> NoteIndex {
    let _ = text;
    NoteIndex::default()
}
