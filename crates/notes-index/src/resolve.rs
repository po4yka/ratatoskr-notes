//! The resolution of links with the rules of the Obsidian engine.

use std::collections::BTreeMap;

use crate::index::{LinkForm, NoteIndex};

/// The number of links from one note to each target.
pub type LinkCounts = BTreeMap<String, u32>;

/// The paths of every file of one vault, in NFC.
#[derive(Debug, Clone, Default)]
pub struct Vault {
    paths: Vec<String>,
}

impl Vault {
    /// The vault of `paths`. Each path is relative to the vault root, in NFC.
    #[must_use]
    pub fn new(paths: impl IntoIterator<Item = String>) -> Self {
        Self {
            paths: paths.into_iter().collect(),
        }
    }

    /// The file that the link `linkpath` of the note `source` names, or `None` when no file
    /// matches.
    #[must_use]
    pub fn resolve(&self, source: &str, linkpath: &str, form: LinkForm) -> Option<&str> {
        let _ = (source, linkpath, form, &self.paths);
        None
    }
}

/// The resolved and the unresolved links of every note of one vault.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LinkGraph {
    /// By source note: the number of links to each file.
    pub resolved: BTreeMap<String, LinkCounts>,
    /// By source note: the number of links to each target that names no file.
    pub unresolved: BTreeMap<String, LinkCounts>,
}

impl LinkGraph {
    /// Resolve the links, embeds and frontmatter links of each note in `notes` against `vault`.
    #[must_use]
    pub fn build<'a>(
        vault: &Vault,
        notes: impl IntoIterator<Item = (&'a str, &'a NoteIndex)>,
    ) -> Self {
        let _ = (vault, notes.into_iter().count());
        Self::default()
    }

    /// The notes that link to `target`, with the number of links of each.
    #[must_use]
    pub fn backlinks(&self, target: &str) -> LinkCounts {
        self.resolved
            .iter()
            .filter_map(|(source, counts)| counts.get(target).map(|count| (source.clone(), *count)))
            .collect()
    }
}
