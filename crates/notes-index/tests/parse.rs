//! The parser of one note (workspace spec `notes-file-index`, task 7.2).

use ratatoskr_notes_index::{LinkForm, parse};

#[test]
fn every_link_form_is_indexed() {
    let index = parse(
        "Plain [[Name]], aliased [[Name|Shown]], heading [[Name#Part]], block [[Name#^b1]].\n\
         ![[Picture.png]]\n\
         A Markdown link [text](Folder/Other%20Note.md).\n",
    );

    let links: Vec<(&str, &str, &str, LinkForm)> = index
        .links
        .iter()
        .map(|link| {
            (
                link.linkpath(),
                link.subpath(),
                link.display_text.as_str(),
                link.form,
            )
        })
        .collect();
    assert_eq!(
        links,
        [
            ("Name", "", "Name", LinkForm::Wiki),
            ("Name", "", "Shown", LinkForm::Wiki),
            ("Name", "#Part", "Name > Part", LinkForm::Wiki),
            ("Name", "#^b1", "Name > ^b1", LinkForm::Wiki),
            ("Folder/Other Note.md", "", "text", LinkForm::Markdown),
        ]
    );
    let embeds: Vec<(&str, LinkForm)> = index
        .embeds
        .iter()
        .map(|embed| (embed.link.as_str(), embed.form))
        .collect();
    assert_eq!(embeds, [("Picture.png", LinkForm::Wiki)]);
}

#[test]
fn comment_links_count_and_code_links_do_not() {
    let index = parse(
        "%% see [[Secret]] and #hidden %%\n\
         \n\
         ```\n\
         [[Code]] #code\n\
         ```\n\
         \n\
         `[[Span]]` and <!-- [[Html]] --> after.\n",
    );

    let links: Vec<&str> = index.links.iter().map(|link| link.link.as_str()).collect();
    assert_eq!(links, ["Secret"]);
    assert_eq!(index.tags, ["#hidden"]);
}
