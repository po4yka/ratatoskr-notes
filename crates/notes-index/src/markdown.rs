//! The body of a note: `CommonMark` with the Obsidian extensions (workspace design D8).
//!
//! A `CommonMark` parser finds the code (spans, fenced, tilde and indented blocks), the HTML
//! comments and the `$` math, and the reader blanks them out of a copy of the body. The Markdown
//! links, the images and the headings also come from the parser. The wikilinks, the embeds, the
//! tags and the block ids are read from the copy, so nothing in code, in an HTML comment or in
//! math is indexed. A `%% comment %%` is not blanked: Obsidian indexes the links and the tags in
//! it.

use std::ops::Range;

use pulldown_cmark::{Event, LinkType, Options, Parser, Tag, TagEnd};

use crate::index::{Heading, Link, LinkForm, NoteIndex};
use crate::text::{
    has_scheme, is_tag_char, nfc, percent_decode, tag_body_is_valid, wikilink_display,
};

/// One link or embed and where it is in the body.
struct Found {
    at: usize,
    end: usize,
    embed: bool,
    /// Whether a Markdown link is inline, `[text](target)`, and not a reference or a shortcut.
    inline: bool,
    link: Link,
}

/// Where one link or embed of a body is: the text from `[[` or `[` (with the `!` of an embed) to
/// its end, and its form.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Reference {
    pub(crate) range: Range<usize>,
    pub(crate) form: LinkForm,
}

/// A Markdown link or image whose text the reader still collects.
struct Open {
    at: usize,
    end: usize,
    embed: bool,
    inline: bool,
    target: Option<String>,
    text: String,
}

/// Read the body of a note into `index`: links, embeds, tags, headings and block ids.
pub(crate) fn read(body: &str, index: &mut NoteIndex) {
    let (found, visible, mut link_ranges) = find_links(body, index);
    for found in found {
        if found.embed {
            index.embeds.push(found.link);
        } else {
            index.links.push(found.link);
        }
    }

    link_ranges.extend(urls(&visible));
    read_tags(&blanked(&visible, &link_ranges), &mut index.tags);
    read_blocks(&visible, &mut index.blocks);
}

/// Where each wikilink, embed and inline Markdown link of `body` is, outside code, in the order of
/// the body. A reference-style Markdown link is left out: it names no target in its own text.
pub(crate) fn references(body: &str) -> Vec<Reference> {
    let (found, _, _) = find_links(body, &mut NoteIndex::default());
    found
        .into_iter()
        .filter(|found| found.link.form == LinkForm::Wiki || found.inline)
        .map(|found| Reference {
            range: found.at..found.end,
            form: found.link.form,
        })
        .collect()
}

/// The links and embeds of `body` in its order, the body with code, HTML comments and math blanked
/// out, and the ranges of the links. The headings go to `index`.
fn find_links(body: &str, index: &mut NoteIndex) -> (Vec<Found>, String, Vec<Range<usize>>) {
    let mut blank: Vec<Range<usize>> = Vec::new();
    let mut found: Vec<Found> = Vec::new();
    let mut link_ranges: Vec<Range<usize>> = Vec::new();
    read_events(body, index, &mut blank, &mut found, &mut link_ranges);
    let visible = blanked(body, &blank);
    read_wikilinks(&visible, &mut found, &mut link_ranges);
    found.sort_by_key(|found| found.at);
    (found, visible, link_ranges)
}

/// The events of the `CommonMark` parser: what to blank, the Markdown links and images, and the
/// headings.
fn read_events(
    body: &str,
    index: &mut NoteIndex,
    blank: &mut Vec<Range<usize>>,
    found: &mut Vec<Found>,
    link_ranges: &mut Vec<Range<usize>>,
) {
    let mut open: Option<Open> = None;
    for (event, range) in Parser::new_ext(body, Options::ENABLE_MATH).into_offset_iter() {
        match event {
            Event::Start(Tag::CodeBlock(_)) | Event::Code(_) => {
                if let (Some(open), Event::Code(code)) = (open.as_mut(), &event) {
                    open.text.push_str(code);
                }
                blank.push(range);
            }
            Event::InlineMath(_) | Event::DisplayMath(_) => blank.push(range),
            Event::Start(Tag::HtmlBlock) | Event::InlineHtml(_) | Event::Html(_) => {
                if body
                    .get(range.clone())
                    .is_some_and(|html| html.trim_start().starts_with("<!--"))
                {
                    blank.push(range);
                }
            }
            Event::Start(Tag::Heading { level, .. }) => {
                if let Some(heading) = body.get(range).map(heading_text) {
                    index.headings.push(Heading {
                        level: level as u8,
                        heading,
                    });
                }
            }
            Event::Start(
                Tag::Link {
                    link_type,
                    ref dest_url,
                    ..
                }
                | Tag::Image {
                    link_type,
                    ref dest_url,
                    ..
                },
            ) => {
                let embed = matches!(event, Event::Start(Tag::Image { .. }));
                let target = local_target(link_type, dest_url);
                if target.is_some() {
                    link_ranges.push(range.clone());
                }
                open = Some(Open {
                    at: range.start,
                    end: range.end,
                    embed,
                    inline: link_type == LinkType::Inline,
                    target,
                    text: String::new(),
                });
            }
            Event::Text(text) => {
                if let Some(open) = open.as_mut() {
                    open.text.push_str(&text);
                }
            }
            Event::End(TagEnd::Link | TagEnd::Image) => {
                if let Some(Open {
                    at,
                    end,
                    embed,
                    inline,
                    target: Some(target),
                    text,
                }) = open.take()
                {
                    found.push(Found {
                        at,
                        end,
                        embed,
                        inline,
                        link: Link {
                            link: target,
                            display_text: text,
                            form: LinkForm::Markdown,
                        },
                    });
                }
            }
            _ => {}
        }
    }
}

/// The target of a Markdown link or image that names a file of the vault, decoded and in NFC.
/// `None` for a URL with a scheme, an e-mail address or an empty target.
fn local_target(link_type: LinkType, destination: &str) -> Option<String> {
    if matches!(link_type, LinkType::Autolink | LinkType::Email) {
        return None;
    }
    let destination = destination.trim();
    if destination.is_empty() || has_scheme(destination) {
        return None;
    }
    Some(nfc(&percent_decode(destination)))
}

/// The text of a heading as the note writes it, without its markers.
fn heading_text(source: &str) -> String {
    let line = source.lines().next().unwrap_or_default().trim();
    let Some(after) = line.strip_prefix('#') else {
        return line.to_owned();
    };
    let content = after.trim_start_matches('#').trim();
    let closing = content.trim_end_matches('#');
    if closing.len() < content.len() && (closing.is_empty() || closing.ends_with([' ', '\t'])) {
        closing.trim().to_owned()
    } else {
        content.to_owned()
    }
}

/// A copy of `text` with each range in `ranges` replaced by spaces. Each range starts and ends on
/// a character boundary.
fn blanked(text: &str, ranges: &[Range<usize>]) -> String {
    let mut bytes = text.as_bytes().to_vec();
    for range in ranges {
        if let Some(part) = bytes.get_mut(range.clone()) {
            part.fill(b' ');
        }
    }
    String::from_utf8(bytes).unwrap_or_else(|_| text.to_owned())
}

/// The wikilinks and wikilink embeds of `text`. A wikilink ends on its own line.
fn read_wikilinks(text: &str, found: &mut Vec<Found>, link_ranges: &mut Vec<Range<usize>>) {
    let mut at = 0;
    while let Some(open) = text
        .get(at..)
        .and_then(|rest| rest.find("[["))
        .map(|o| at + o)
    {
        let inner_start = open + 2;
        let rest = text.get(inner_start..).unwrap_or_default();
        let line = rest.split('\n').next().unwrap_or_default();
        let Some(close) = line.find("]]") else {
            at = inner_start;
            continue;
        };
        let inner = line.get(..close).unwrap_or_default();
        if inner.contains("[[") {
            at = open + 1;
            continue;
        }
        let end = inner_start + close + 2;
        if !inner.is_empty() {
            let embed = open > 0 && text.as_bytes().get(open - 1) == Some(&b'!');
            let start = if embed { open - 1 } else { open };
            found.push(Found {
                at: start,
                end,
                embed,
                inline: false,
                link: wikilink(inner),
            });
            link_ranges.push(start..end);
        }
        at = end;
    }
}

/// The link of the inside of `[[...]]`: the target before the first `|`, and the alias after it.
/// The target is put in NFC; the text that Obsidian shows keeps the form that the note writes.
fn wikilink(inner: &str) -> Link {
    let (target, alias) = inner
        .split_once('|')
        .map_or((inner, None), |(target, alias)| (target, Some(alias)));
    Link {
        link: nfc(target),
        display_text: alias.map_or_else(|| wikilink_display(target), str::to_owned),
        form: LinkForm::Wiki,
    }
}

/// The ranges of the bare URLs in `text`, such as `https://example.com/#anchor`, whose `#` is
/// not a tag.
fn urls(text: &str) -> Vec<Range<usize>> {
    let bytes = text.as_bytes();
    let mut ranges = Vec::new();
    let mut from = 0;
    while let Some(marker) = text
        .get(from..)
        .and_then(|rest| rest.find("://"))
        .map(|m| from + m)
    {
        let mut start = marker;
        while start > 0
            && bytes
                .get(start - 1)
                .is_some_and(|b| b.is_ascii_alphanumeric() || matches!(b, b'+' | b'.' | b'-'))
        {
            start -= 1;
        }
        let mut end = marker + 3;
        while bytes
            .get(end)
            .is_some_and(|b| !b.is_ascii_whitespace() && !matches!(b, b'<' | b'>' | b'"'))
        {
            end += 1;
        }
        if start < marker {
            ranges.push(start..end);
        }
        from = end;
    }
    ranges
}

/// The tags of `text`, each with its `#`, in order. A `#` right after another `#` starts no tag.
fn read_tags(text: &str, tags: &mut Vec<String>) {
    let mut previous: Option<char> = None;
    let mut characters = text.char_indices();
    while let Some((at, character)) = characters.next() {
        if character == '#' && previous != Some('#') {
            let body: String = text
                .get(at + 1..)
                .unwrap_or_default()
                .chars()
                .take_while(|c| is_tag_char(*c))
                .collect();
            if tag_body_is_valid(&body) {
                for _ in 0..body.chars().count() {
                    previous = characters.next().map(|(_, c)| c);
                }
                tags.push(format!("#{body}"));
                continue;
            }
        }
        previous = Some(character);
    }
}

/// The block ids of `text`: a last word `^id` of a line, where `id` holds letters, digits and
/// `-`.
fn read_blocks(text: &str, blocks: &mut Vec<String>) {
    for line in text.lines() {
        let last = line.trim_end().rsplit(char::is_whitespace).next();
        if let Some(id) = last.and_then(|word| word.strip_prefix('^'))
            && !id.is_empty()
            && id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')
        {
            blocks.push(id.to_owned());
        }
    }
}
