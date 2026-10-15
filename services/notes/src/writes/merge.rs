//! The three-way merge proposal of a text conflict (workspace design D2).
//!
//! A line-based diff3: the changes of the current revision and of the writer's content against the
//! base, each as hunks of base lines. Hunks of the two sides that overlap or touch form one region.
//! A region that only one side changed takes that side; a region that both sides changed the same
//! way takes it once; any other region is an overlapping hunk, and the proposal carries both sides
//! between conflict markers. Notes never applies a proposal on its own.

use std::ops::Range;
use std::time::{Duration, Instant};

use similar::{Algorithm, DiffTag, capture_diff_slices_deadline};

/// The longest time one diff may take. After it the diff is less minimal, never wrong.
const DIFF_DEADLINE: Duration = Duration::from_secs(2);

/// The merged text and whether it holds overlapping hunks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Merged {
    /// The proposed text.
    pub(super) text: String,
    /// Whether both sides changed one region differently.
    pub(super) overlapping: bool,
}

/// One change of a side: the base lines it replaces and its own lines.
#[derive(Debug, Clone)]
struct Hunk<'a> {
    base: Range<usize>,
    lines: &'a [&'a str],
}

/// The three-way merge of `current` and `writer` from `base`.
pub(super) fn merge(base: &str, current: &str, writer: &str) -> Merged {
    let base: Vec<&str> = base.split_inclusive('\n').collect();
    let current: Vec<&str> = current.split_inclusive('\n').collect();
    let writer: Vec<&str> = writer.split_inclusive('\n').collect();
    let ours = hunks(&base, &current);
    let theirs = hunks(&base, &writer);

    let mut text = String::new();
    let mut overlapping = false;
    let mut at = 0;
    let (mut i, mut j) = (0, 0);
    while i < ours.len() || j < theirs.len() {
        // The region starts with the earlier hunk and grows while a hunk of either side touches it.
        let start = match (ours.get(i), theirs.get(j)) {
            (Some(a), Some(b)) => a.base.start.min(b.base.start),
            (Some(a), None) => a.base.start,
            (None, Some(b)) => b.base.start,
            (None, None) => break,
        };
        let mut end = start;
        let (first_ours, first_theirs) = (i, j);
        loop {
            if let Some(hunk) = ours.get(i).filter(|hunk| hunk.base.start <= end) {
                end = end.max(hunk.base.end);
                i += 1;
            } else if let Some(hunk) = theirs.get(j).filter(|hunk| hunk.base.start <= end) {
                end = end.max(hunk.base.end);
                j += 1;
            } else {
                break;
            }
        }
        push_lines(&mut text, base.get(at..start).unwrap_or_default());
        let region = start..end;
        let ours_region = ours.get(first_ours..i).unwrap_or_default();
        let theirs_region = theirs.get(first_theirs..j).unwrap_or_default();
        let ours_text = side(&base, &region, ours_region);
        let theirs_text = side(&base, &region, theirs_region);
        if theirs_region.is_empty() || ours_text == theirs_text {
            text.push_str(&ours_text);
        } else if ours_region.is_empty() {
            text.push_str(&theirs_text);
        } else {
            overlapping = true;
            text.push_str("<<<<<<< current\n");
            push_ended(&mut text, &ours_text);
            text.push_str("=======\n");
            push_ended(&mut text, &theirs_text);
            text.push_str(">>>>>>> writer\n");
        }
        at = end;
    }
    push_lines(&mut text, base.get(at..).unwrap_or_default());
    Merged { text, overlapping }
}

/// The changes from `base` to `side`, in the order of the base.
fn hunks<'a>(base: &[&str], side: &'a [&'a str]) -> Vec<Hunk<'a>> {
    let deadline = Instant::now().checked_add(DIFF_DEADLINE);
    capture_diff_slices_deadline(Algorithm::Myers, base, side, deadline)
        .into_iter()
        .filter(|operation| operation.tag() != DiffTag::Equal)
        .map(|operation| Hunk {
            base: operation.old_range(),
            lines: side.get(operation.new_range()).unwrap_or_default(),
        })
        .collect()
}

/// The text of `region` of the base with the `changes` of one side.
fn side(base: &[&str], region: &Range<usize>, changes: &[Hunk<'_>]) -> String {
    let mut text = String::new();
    let mut at = region.start;
    for hunk in changes {
        push_lines(&mut text, base.get(at..hunk.base.start).unwrap_or_default());
        push_lines(&mut text, hunk.lines);
        at = hunk.base.end;
    }
    push_lines(&mut text, base.get(at..region.end).unwrap_or_default());
    text
}

fn push_lines(text: &mut String, lines: &[&str]) {
    for line in lines {
        text.push_str(line);
    }
}

/// Push `side`, and end it with a line break before a conflict marker.
fn push_ended(text: &mut String, side: &str) {
    text.push_str(side);
    if !side.is_empty() && !side.ends_with('\n') {
        text.push('\n');
    }
}

#[cfg(test)]
mod tests {
    use super::{Merged, merge};

    #[test]
    fn one_region_changed_twice_overlaps() {
        let merged = merge(
            "a\nb\nc\n",
            "a\nB from the vault\nc\n",
            "a\nB from the writer\nc\n",
        );

        assert_eq!(
            merged,
            Merged {
                text: "a\n<<<<<<< current\nB from the vault\n=======\nB from the writer\n\
                       >>>>>>> writer\nc\n"
                    .to_owned(),
                overlapping: true,
            }
        );
    }

    #[test]
    fn the_same_change_on_both_sides_is_taken_once() {
        let merged = merge("a\nb\nc\n", "a\nB\nc\n", "a\nB\nc\nd\n");

        assert_eq!(
            merged,
            Merged {
                text: "a\nB\nc\nd\n".to_owned(),
                overlapping: false,
            }
        );
    }
}
