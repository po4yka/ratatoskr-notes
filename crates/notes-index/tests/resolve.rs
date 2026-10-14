//! Link resolution with the rules of the Obsidian engine (workspace spec `notes-file-index`).

use ratatoskr_notes_index::{LinkForm, LinkGraph, Vault, parse};

#[test]
fn same_folder_then_shortest_path_wins() {
    let vault =
        Vault::new(["A/Note.md", "B/C/Note.md", "Root.md", "B/C/Other.md"].map(str::to_owned));

    assert_eq!(
        vault.resolve("Root.md", "Note", LinkForm::Wiki),
        Some("A/Note.md"),
        "from the root, the shortest path among equal names wins"
    );
    assert_eq!(
        vault.resolve("B/C/Other.md", "Note", LinkForm::Wiki),
        Some("B/C/Note.md"),
        "a file of that name in the linking file's own folder wins"
    );

    let root = parse("[[Note]]");
    let other = parse("[[Note]]");
    let graph = LinkGraph::build(&vault, [("Root.md", &root), ("B/C/Other.md", &other)]);
    assert_eq!(
        graph.backlinks("A/Note.md").into_iter().collect::<Vec<_>>(),
        [("Root.md".to_owned(), 1)]
    );
    assert_eq!(
        graph
            .backlinks("B/C/Note.md")
            .into_iter()
            .collect::<Vec<_>>(),
        [("B/C/Other.md".to_owned(), 1)]
    );
}
