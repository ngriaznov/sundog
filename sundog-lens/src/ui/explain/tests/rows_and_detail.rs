//! The answers table and the detail block of the selected node.

use super::*;

#[test]
fn a_result_lists_every_node_with_owns_local_source_and_probes() {
    let model = model();
    let mut app = opened(&model, "k1");
    let request = ask(&mut app, &model);
    answered(&mut app, &model, &request, |_| {});
    let rows = shown(&app, &model, 116, 40);
    let header = find(&rows, "NODE");
    for column in ["OWNS", "LOCAL", "READ", "SERVES", "SOURCE", "PROBES"] {
        assert!(header.contains(column), "{column} in {header}");
    }
    let located = locate(&model, Some("it"), &KeySpec::parse("k1").unwrap()).unwrap();
    let owners: Vec<_> = located.owners.iter().map(|o| o.slot.to_string()).collect();
    let first = owners[0].as_str();
    let start = rows.iter().position(|row| row == header).unwrap();
    for (offset, label) in ["n1", "n2", "n3", "n4", "n5"].into_iter().enumerate() {
        let row = &rows[start + 1 + offset];
        assert!(row.contains(&format!(" {label} ")), "{row}");
        if owners.iter().any(|owner| owner == label) {
            assert!(row.contains("yes"), "{row}");
            assert!(row.contains("live · expires in 38.1 s"), "{row}");
            assert!(row.contains("this node hit"), "{row}");
            assert!(row.contains("serves"), "{row}");
            assert!(row.contains(" hit "), "{row}");
            let other = owners.iter().find(|owner| *owner != label).unwrap();
            assert!(row.contains(&format!("{other} held")), "{row}");
        } else {
            assert!(row.contains("no "), "{row}");
            assert!(row.contains("absent"), "{row}");
            assert!(row.contains("not an owner"), "{row}");
            assert!(row.contains(&format!("{first} hit")), "{row}");
            assert!(
                row.contains(&format!("{} held · {} held", owners[0], owners[1])),
                "{row}"
            );
        }
    }
}

#[test]
fn agreeing_views_show_the_check_and_the_lens_hash() {
    let model = model();
    let mut app = opened(&model, "k1");
    let request = ask(&mut app, &model);
    answered(&mut app, &model, &request, |_| {});
    let rows = shown(&app, &model, 116, 40);
    assert!(
        has(
            &rows,
            "✔ 5 nodes agree, the lens computes the same · asked 3.0 s ago"
        ),
        "{rows:?}"
    );
    let digest = model.ownership("it").unwrap();
    assert!(has(&rows, &eventlog::view_hash(digest.view_hash)));
    let title = drawn(&app, &model, 140, 40);
    assert!(
        has(&title, "Explain a read · it · computed · asked"),
        "{title:?}"
    );
}

#[test]
fn disagreeing_views_show_who_holds_which() {
    let model = model();
    let mut app = opened(&model, "k1");
    let request = ask(&mut app, &model);
    answered(&mut app, &model, &request, |answers| {
        if let Outcome::Read(reading) = &mut answers.nodes[3].outcome {
            reading.distributed.as_mut().unwrap().view = "71c0a2f4e8b3195d".to_owned();
        }
    });
    let rows = shown(&app, &model, 116, 40);
    let digest = model.ownership("it").unwrap();
    let lens = eventlog::view_hash(digest.view_hash);
    let line = find(&rows, "↻ n1");
    assert!(
        line.starts_with(&format!(
            "↻ n1 n2 n3 n5 hold {lens}, n4 holds 71c0a2f4 · asked"
        )),
        "{line}"
    );
    assert!(!has(&rows, "agree"), "{rows:?}");
    // Every node on one view that is not the lens's: the nodes differ
    // from the lens only when the placement differs; the view moved when
    // none holds the lens's.
    let mut app = opened(&model, "k1");
    let request = ask(&mut app, &model);
    answered(&mut app, &model, &request, |answers| {
        if let Outcome::Read(reading) = &mut answers.nodes[2].outcome {
            reading.distributed.as_mut().unwrap().owners.reverse();
        }
    });
    let rows = shown(&app, &model, 116, 40);
    let line = find(&rows, "differs from the lens");
    assert!(
        line.starts_with(&format!(
            "↻ n3 differs from the lens, which holds view {lens} · asked"
        )),
        "{line}"
    );
    // Two nodes that differ read in the plural.
    let mut app = opened(&model, "k1");
    let request = ask(&mut app, &model);
    answered(&mut app, &model, &request, |answers| {
        for answer in &mut answers.nodes[1..3] {
            if let Outcome::Read(reading) = &mut answer.outcome {
                reading.distributed.as_mut().unwrap().owners.reverse();
            }
        }
    });
    let rows = shown(&app, &model, 116, 40);
    let line = find(&rows, "differ from the lens");
    assert!(
        line.starts_with(&format!(
            "↻ n2 n3 differ from the lens, which holds view {lens} · asked"
        )),
        "{line}"
    );
}

#[test]
fn one_node_that_agrees_reads_in_the_singular() {
    let model = model();
    let mut app = opened(&model, "k1");
    let request = ask(&mut app, &model);
    answered(&mut app, &model, &request, |answers| {
        answers.nodes.truncate(1);
    });
    let rows = shown(&app, &model, 116, 40);
    assert!(
        has(&rows, "✔ 1 node agrees, the lens computes the same · asked"),
        "{rows:?}"
    );
    assert!(!has(&rows, "1 nodes"), "{rows:?}");
}

#[test]
fn a_failed_node_keeps_its_row_with_the_reason() {
    let model = model();
    let mut app = opened(&model, "k1");
    let request = ask(&mut app, &model);
    answered(&mut app, &model, &request, |answers| {
        answers.nodes[1].outcome = Outcome::Failed("no answer (connection refused)".to_owned());
        answers.nodes[3].outcome = Outcome::Failed(crate::ask::PREDATES.to_owned());
    });
    let rows = shown(&app, &model, 116, 40);
    let header = rows.iter().position(|row| row.contains("NODE")).unwrap();
    assert!(
        rows[header + 2].starts_with("  n2")
            && rows[header + 2].contains("no answer (connection refused)"),
        "{}",
        rows[header + 2]
    );
    assert!(
        rows[header + 4].starts_with("  n4")
            && rows[header + 4].contains("rebuild sundog-testnode"),
        "{}",
        rows[header + 4]
    );
    // The three that answered still agree, and the failures take no part
    // in the comparison.
    assert!(has(&rows, "✔ 3 nodes agree"), "{rows:?}");
    assert_eq!(rows[header + 1].matches("n1").count(), 1);
}

#[test]
fn the_detail_block_follows_the_selected_node() {
    let model = model();
    let mut app = opened(&model, "k1");
    let request = ask(&mut app, &model);
    answered(&mut app, &model, &request, |_| {});
    let located = locate(&model, Some("it"), &KeySpec::parse("k1").unwrap()).unwrap();
    let first = located.owners[0].slot.to_string();
    let rows = shown(&app, &model, 116, 40);
    assert!(has(&rows, "n1 in detail"), "{rows:?}");
    assert!(find(&rows, "record  ").contains("absent"));
    assert!(find(&rows, "marks   ").contains("does not own the part · none"));
    assert!(find(&rows, "reads   ").contains("own copy: not an owner · serves peers: miss"));
    assert!(
        find(&rows, "source  ").contains(&format!("the first owner to answer is {first}: a hit"))
    );
    let probes = find(&rows, "probes  ");
    assert!(
        probes.contains(&format!("{first} held · 1760054300112.3@{first}")),
        "{probes}"
    );
    assert!(
        probes.contains("expires in 38.1 s · a read gets a value"),
        "{probes}"
    );
    // Down moves the one node selection; the detail follows.
    press(&mut app, &model, KeyCode::Down);
    press(&mut app, &model, KeyCode::Down);
    let rows = shown(&app, &model, 116, 40);
    assert!(has(&rows, "n3 in detail"), "{rows:?}");
    assert!(!has(&rows, "n1 in detail"));
    let selected = rows
        .iter()
        .find(|row| row.starts_with("▌ n3") || row.contains("▌ n3"));
    assert!(selected.is_some(), "{rows:?}");
    if first == "n3" {
        assert!(find(&rows, "record  ").contains("live 1760054300112.3@n3 · expires in 38.1 s"));
        assert!(find(&rows, "marks   ").contains("owns the part · none"));
    }
    // A failed node's detail is its reason.
    answered_failure(&mut app, &model);
    let rows = shown(&app, &model, 116, 40);
    assert!(has(&rows, "n3 in detail"));
    assert!(
        find(&rows, "reply   ").contains("no answer within 3 s"),
        "{rows:?}"
    );
}

/// Delivers a second request's answers in which n3 stayed silent.
fn answered_failure(app: &mut App, model: &Model) {
    let request = ask(app, model);
    answered(app, model, &request, |answers| {
        answers.nodes[2].outcome = Outcome::Failed("no answer within 3 s".to_owned());
    });
}

/// The reading of the `at`th answer, which must have one.
fn reading(answers: &mut Explained, at: usize) -> &mut Reading {
    match &mut answers.nodes[at].outcome {
        Outcome::Read(reading) => reading,
        Outcome::Failed(reason) => panic!("node {at} failed: {reason}"),
    }
}

/// What the answers of [`odd_answers`] name.
struct Odd {
    /// The slot labels of the key's first and second owner.
    first: String,
    second: String,
    /// The lens's view, 16 digits.
    view: String,
    /// The version every record carries, with its node named by label.
    version: String,
}

/// An overlay whose answers hold every kind of record, source and probe
/// the fixture cluster leaves out: n1 a tombstone with no owner to answer,
/// an owner out of reach, an owner on another view and a view of its own
/// that moved; n2 an entry gone idle on a part it gives up, beside owners
/// that miss and decline; n3 a spilled entry that never expires; n4 an
/// entry whose expiry passed.
fn odd_answers(model: &Model) -> (App, Odd) {
    use crate::explained::{Lapse, Probe, Why};

    let located = locate(model, Some("it"), &KeySpec::parse("k1").unwrap()).unwrap();
    let (first, second) = (
        located.owners[0].node.to_string(),
        located.owners[1].node.to_string(),
    );
    let stamp = format!("1760054300112.3@{first}");
    let expired = Some(fixture::AT_MS - 4_200);
    let other_view = "71c0a2f4e8b3195d";
    let mut app = opened(model, "k1");
    let request = ask(&mut app, model);
    answered(&mut app, model, &request, |answers| {
        let n1 = reading(answers, 0);
        n1.local = Local::Tombstone {
            version: stamp.clone(),
        };
        n1.source = Source::Unavailable;
        let dist = n1.distributed.as_mut().unwrap();
        dist.view_moved_to = Some(other_view.to_owned());
        dist.probes = vec![
            Probe {
                node: first.clone(),
                answer: Answer::Unreached {
                    why: Why::Io("ConnectionRefused".to_owned()),
                },
            },
            Probe {
                node: second.clone(),
                answer: Answer::StaleView {
                    responder_view: other_view.to_owned(),
                },
            },
        ];
        let n2 = reading(answers, 1);
        n2.local = Local::Lapsed {
            version: stamp.clone(),
            expires_at_ms: expired,
            cause: Lapse::Idle,
        };
        let dist = n2.distributed.as_mut().unwrap();
        dist.residency.releasing_ms = Some(4_200);
        dist.residency.cold_marked = true;
        dist.probes = vec![
            Probe {
                node: first.clone(),
                answer: Answer::Miss,
            },
            Probe {
                node: second.clone(),
                answer: Answer::Declined,
            },
        ];
        reading(answers, 2).local = Local::Live {
            version: stamp.clone(),
            expires_at_ms: None,
            spilled: true,
        };
        reading(answers, 3).local = Local::Lapsed {
            version: stamp.clone(),
            expires_at_ms: expired,
            cause: Lapse::Expired,
        };
    });
    let odd = Odd {
        first: located.owners[0].slot.to_string(),
        second: located.owners[1].slot.to_string(),
        view: format!("{:016x}", located.view_hash),
        version: format!("1760054300112.3@{}", located.owners[0].slot),
    };
    (app, odd)
}

#[test]
fn the_table_names_each_kind_of_record_and_source() {
    let model = model();
    let (app, _) = odd_answers(&model);
    let rows = shown(&app, &model, 116, 60);
    let table = |label: &str| {
        rows.iter()
            .find(|row| {
                row.starts_with(&format!("  {label} ")) || row.starts_with(&format!("▌ {label} "))
            })
            .unwrap_or_else(|| panic!("no row for {label} in {rows:?}"))
    };
    assert!(table("n1").contains("tombstone"), "{}", table("n1"));
    assert!(table("n1").contains("unavailable"), "{}", table("n1"));
    assert!(table("n2").contains("lapsed idle"), "{}", table("n2"));
    assert!(
        table("n3").contains("spilled · never expires"),
        "{}",
        table("n3")
    );
    assert!(table("n4").contains("lapsed expired"), "{}", table("n4"));
}

#[test]
fn the_detail_block_gives_each_record_mark_and_probe_its_words() {
    let model = model();
    let (mut app, odd) = odd_answers(&model);
    let Odd {
        first,
        second,
        view,
        version,
    } = odd;
    let detail = |app: &App, label: &str| {
        let rows = shown(app, &model, 116, 60);
        assert!(has(&rows, &format!("{label} in detail")), "{rows:?}");
        rows
    };
    let rows = detail(&app, "n1");
    assert!(find(&rows, "record  ").ends_with(&format!("tombstone {version}")));
    assert!(find(&rows, "source  ").ends_with("no owner answers, so the fetch fails"));
    assert!(
        find(&rows, "view    ").ends_with(&format!(
            "{view} · moved to 71c0a2f4e8b3195d during the call"
        )),
        "{rows:?}"
    );
    assert!(
        find(&rows, "probes  ")
            .ends_with(&format!("{first} unreached (io error (ConnectionRefused))")),
        "{rows:?}"
    );
    assert!(
        has(
            &rows,
            &format!("{second} stale view · its view is 71c0a2f4e8b3195d")
        ),
        "{rows:?}"
    );

    press(&mut app, &model, KeyCode::Down);
    let rows = detail(&app, "n2");
    assert!(
        find(&rows, "record  ").ends_with(&format!("lapsed (idle) {version} · expired 4.2 s ago")),
        "{rows:?}"
    );
    assert!(
        find(&rows, "marks   ").ends_with("does not own the part · releasing 4.2 s, cold"),
        "{rows:?}"
    );
    assert!(
        find(&rows, "probes  ").ends_with(&format!("{first} miss")),
        "{rows:?}"
    );
    assert!(has(&rows, &format!("{second} declined")), "{rows:?}");

    press(&mut app, &model, KeyCode::Down);
    let rows = detail(&app, "n3");
    assert!(
        find(&rows, "record  ").ends_with(&format!(
            "live {version} · never expires · in the spill tier"
        )),
        "{rows:?}"
    );

    press(&mut app, &model, KeyCode::Down);
    let rows = detail(&app, "n4");
    assert!(
        find(&rows, "record  ")
            .ends_with(&format!("lapsed (expired) {version} · expired 4.2 s ago")),
        "{rows:?}"
    );
}

#[test]
fn a_replicated_reading_has_no_owner_columns() {
    let model = model();
    let mut app = opened(&model, "k1");
    let request = ask(&mut app, &model);
    answered(&mut app, &model, &request, |answers| {
        for answer in &mut answers.nodes {
            if let Outcome::Read(reading) = &mut answer.outcome {
                reading.distributed = None;
                reading.mode = "replicated".to_owned();
                reading.source = Source::Local { hit: true };
            }
        }
    });
    let rows = shown(&app, &model, 116, 40);
    let header = find(&rows, "NODE");
    assert!(
        header.contains("LOCAL") && header.contains("SOURCE"),
        "{header}"
    );
    for gone in ["OWNS", "READ", "SERVES", "PROBES"] {
        assert!(!header.contains(gone), "{gone} in {header}");
    }
    assert!(
        has(&rows, "no node gave a Distributed reading to compare"),
        "{rows:?}"
    );
    let detail = rows.join("\n");
    assert!(
        !detail.contains("marks   ") && !detail.contains("probes  "),
        "{detail}"
    );
}

#[test]
fn columns_drop_in_order_at_100_and_80_columns() {
    let model = model();
    let mut app = opened(&model, "k1");
    let request = ask(&mut app, &model);
    answered(&mut app, &model, &request, |_| {});
    let columns = |width: usize| -> Vec<&'static str> {
        let rows = shown(&app, &model, width, 40);
        let header = find(&rows, "NODE").to_owned();
        [
            "NODE", "OWNS", "LOCAL", "READ", "SERVES", "SOURCE", "PROBES",
        ]
        .into_iter()
        .filter(|column| header.contains(column))
        .collect()
    };
    assert_eq!(
        columns(116),
        [
            "NODE", "OWNS", "LOCAL", "READ", "SERVES", "SOURCE", "PROBES"
        ]
    );
    // The 100-column popup: PROBES goes first.
    assert_eq!(
        columns(92),
        ["NODE", "OWNS", "LOCAL", "READ", "SERVES", "SOURCE"]
    );
    // The 80-column popup: SERVES goes next.
    assert_eq!(columns(72), ["NODE", "OWNS", "LOCAL", "READ", "SOURCE"]);
    assert_eq!(columns(60), ["NODE", "OWNS", "LOCAL", "SOURCE"]);
    // Drawn into the bodies of the three smaller screens.
    for (width, height, absent) in [(100, 27, "PROBES"), (80, 21, "SERVES")] {
        let rows = drawn(&app, &model, width, height);
        assert!(has(&rows, "NODE"), "{rows:?}");
        assert!(!has(&rows, absent), "{absent} in {rows:?}");
    }
}

#[test]
fn rows_are_cut_with_a_count_and_the_detail_goes_first() {
    let model = model();
    let mut app = opened(&model, "k1");
    let request = ask(&mut app, &model);
    answered(&mut app, &model, &request, |_| {});
    let full = shown(&app, &model, 116, 40);
    assert!(has(&full, "in detail"));
    let head = full.iter().position(|row| row.contains("NODE")).unwrap();
    let with_rows = head + 1 + 5;
    // Room for the rows and the key hints but not the detail.
    let rows = shown(&app, &model, 116, with_rows + 2);
    assert!(!has(&rows, "in detail"), "{rows:?}");
    assert!(!has(&rows, "… +"), "{rows:?}");
    assert!(
        rows.iter()
            .any(|row| row.starts_with("  n5") || row.starts_with("▌ n5")),
        "the fifth node's table row, not the owner line that names it: {rows:?}"
    );
    assert_eq!(rows.len(), with_rows + 2);
    // Two rows fewer: the last rows give way to a count.
    let rows = shown(&app, &model, 116, with_rows);
    assert!(has(&rows, "… +3 nodes"), "{rows:?}");
    assert!(has(&rows, "n1") && has(&rows, "n2"), "{rows:?}");
    assert!(!rows.iter().any(|row| row.starts_with("  n3")), "{rows:?}");
    assert_eq!(rows.len(), with_rows);
    assert!(rows.last().unwrap().starts_with("Enter ask"));
    // The rows that stay follow the selection: four Downs select n5, and
    // the cut keeps its row in view.
    for _ in 0..4 {
        press(&mut app, &model, KeyCode::Down);
    }
    let rows = shown(&app, &model, 116, with_rows);
    assert!(has(&rows, "… +3 nodes"), "{rows:?}");
    assert!(rows.iter().any(|row| row.starts_with("▌ n5")), "{rows:?}");
    assert!(
        !rows
            .iter()
            .any(|row| row.starts_with("  n1") || row.starts_with("▌ n1")),
        "{rows:?}"
    );
    assert_eq!(rows.len(), with_rows);
    // Up walks the window back to the first rows.
    for _ in 0..4 {
        press(&mut app, &model, KeyCode::Up);
    }
    let rows = shown(&app, &model, 116, with_rows);
    assert!(rows.iter().any(|row| row.starts_with("▌ n1")), "{rows:?}");
    assert!(rows.iter().any(|row| row.starts_with("  n2")), "{rows:?}");
    assert!(!rows.iter().any(|row| row.starts_with("  n5")), "{rows:?}");
    // The same in the body of the smallest screen with the demo's
    // caption row, where the detail block never fits.
    let rows = drawn(&app, &model, 80, 20);
    assert!(has(&rows, "▌ n1") && has(&rows, "… +2 nodes"), "{rows:?}");
    for _ in 0..4 {
        press(&mut app, &model, KeyCode::Down);
    }
    let rows = drawn(&app, &model, 80, 20);
    assert!(has(&rows, "▌ n5") && has(&rows, "… +2 nodes"), "{rows:?}");
    for _ in 0..4 {
        press(&mut app, &model, KeyCode::Up);
    }
    // Every height down to nothing yields at most that many lines with
    // the key hints last.
    for height in 0..full.len() + 3 {
        let rows = shown(&app, &model, 116, height);
        assert!(rows.len() <= height.max(1), "{height}: {}", rows.len());
        assert!(rows.last().unwrap().contains("Esc close"), "{height}");
    }
}
