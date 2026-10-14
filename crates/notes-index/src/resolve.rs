//! The resolution of links with the rules of the Obsidian engine (workspace design D8).
//!
//! A link names a file by its `linkpath`, the part of the target before the first `#`. An empty
//! linkpath names the linking note itself. Otherwise, with case ignored, the first rule that
//! matches wins:
//!
//! 1. a Markdown link whose target does not start with `/` names the path relative to the folder
//!    of the linking note; a target that starts with `./` or `../` does so for a wikilink too, and a
//!    target that starts with `/` names the path from the vault root;
//! 2. the exact vault path, with or without its `.md` extension;
//! 3. a file of that name in the linking note's own folder;
//! 4. the only file whose path ends with the linkpath (`[[2024/Alpha]]` names
//!    `Archive/2024/Alpha.md`);
//! 5. the shortest path among those files.
//!
//! An alias never resolves a link: the vault holds paths only.

use std::collections::BTreeMap;

use crate::index::{LinkForm, NoteIndex};
use crate::text::nfc;

/// The number of links from one note to each target.
pub type LinkCounts = BTreeMap<String, u32>;

/// The extension of a Markdown note, which a link may leave out.
const MARKDOWN_EXTENSION: &str = ".md";

/// One file of a vault and its keys for the comparison that ignores case.
#[derive(Debug, Clone)]
struct Entry {
    path: String,
    /// The path in lower case.
    key: String,
    /// The path in lower case without `.md`, for a Markdown note.
    key_without_extension: Option<String>,
}

impl Entry {
    fn new(path: String) -> Self {
        let key = path.to_lowercase();
        let key_without_extension = key.strip_suffix(MARKDOWN_EXTENSION).map(str::to_owned);
        Self {
            path,
            key,
            key_without_extension,
        }
    }

    fn is(&self, wanted: &str) -> bool {
        self.key == wanted || self.key_without_extension.as_deref() == Some(wanted)
    }

    fn ends_with(&self, wanted: &str) -> bool {
        let suffix = format!("/{wanted}");
        self.key.ends_with(&suffix)
            || self
                .key_without_extension
                .as_deref()
                .is_some_and(|key| key.ends_with(&suffix))
    }
}

/// The paths of every file of one vault, in NFC.
#[derive(Debug, Clone, Default)]
pub struct Vault {
    entries: Vec<Entry>,
}

impl Vault {
    /// The vault of `paths`. Each path is relative to the vault root; it is put in NFC.
    #[must_use]
    pub fn new(paths: impl IntoIterator<Item = String>) -> Self {
        Self {
            entries: paths
                .into_iter()
                .map(|path| Entry::new(nfc(&path)))
                .collect(),
        }
    }

    /// The file that the link `linkpath` of the note `source` names, or `None` when no file
    /// matches.
    #[must_use]
    pub fn resolve(&self, source: &str, linkpath: &str, form: LinkForm) -> Option<&str> {
        let linkpath = nfc(linkpath);
        if linkpath.is_empty() {
            return self.exact(&source.to_lowercase());
        }
        let folder = source.rsplit_once('/').map_or("", |(folder, _)| folder);
        if let Some(absolute) = linkpath.strip_prefix('/') {
            return self.exact(&absolute.to_lowercase());
        }
        let relative = linkpath.starts_with("./") || linkpath.starts_with("../");
        if relative || form == LinkForm::Markdown {
            let joined = join(folder, &linkpath);
            if let Some(found) = joined.and_then(|joined| self.exact(&joined.to_lowercase())) {
                return Some(found);
            }
            if relative {
                return None;
            }
        }
        let wanted = linkpath.to_lowercase();
        if let Some(found) = self.exact(&wanted) {
            return Some(found);
        }
        if !folder.is_empty()
            && let Some(found) = self.exact(&format!("{}/{wanted}", folder.to_lowercase()))
        {
            return Some(found);
        }
        self.entries
            .iter()
            .filter(|entry| entry.ends_with(&wanted))
            .min_by(|a, b| {
                (a.path.chars().count(), &a.path).cmp(&(b.path.chars().count(), &b.path))
            })
            .map(|entry| entry.path.as_str())
    }

    /// The file whose path is `wanted`, in lower case, with or without `.md`. A file with the
    /// whole name wins over a note named without its extension.
    fn exact(&self, wanted: &str) -> Option<&str> {
        self.entries
            .iter()
            .find(|entry| entry.key == wanted)
            .or_else(|| self.entries.iter().find(|entry| entry.is(wanted)))
            .map(|entry| entry.path.as_str())
    }
}

/// `target` relative to `folder`, with each `.` and `..` segment applied. `None` when `..` leaves
/// the vault.
fn join(folder: &str, target: &str) -> Option<String> {
    let mut segments: Vec<&str> = folder.split('/').filter(|s| !s.is_empty()).collect();
    for segment in target.split('/') {
        match segment {
            "" | "." => {}
            ".." => {
                segments.pop()?;
            }
            name => segments.push(name),
        }
    }
    Some(segments.join("/"))
}

/// The resolved and the unresolved links of every note of one vault.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LinkGraph {
    /// By source note: the number of links to each file.
    pub resolved: BTreeMap<String, LinkCounts>,
    /// By source note: the number of links to each linkpath that names no file.
    pub unresolved: BTreeMap<String, LinkCounts>,
}

impl LinkGraph {
    /// Resolve the links, embeds and frontmatter links of each note in `notes` against `vault`.
    #[must_use]
    pub fn build<'a>(
        vault: &Vault,
        notes: impl IntoIterator<Item = (&'a str, &'a NoteIndex)>,
    ) -> Self {
        let mut graph = Self::default();
        for (source, index) in notes {
            let source = nfc(source);
            let targets = index
                .links
                .iter()
                .chain(&index.embeds)
                .map(|link| (link.linkpath(), link.form))
                .chain(index.frontmatter_links.iter().map(|link| {
                    let linkpath = link.link.split('#').next().unwrap_or_default();
                    (linkpath, LinkForm::Wiki)
                }));
            let mut resolved = LinkCounts::new();
            let mut unresolved = LinkCounts::new();
            for (linkpath, form) in targets {
                match vault.resolve(&source, linkpath, form) {
                    Some(path) => *resolved.entry(path.to_owned()).or_default() += 1,
                    None => *unresolved.entry(linkpath.to_owned()).or_default() += 1,
                }
            }
            graph.resolved.insert(source.clone(), resolved);
            graph.unresolved.insert(source, unresolved);
        }
        graph
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
