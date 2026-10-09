//! The read decisions a `Mode::Distributed` fetch makes, written once:
//! what this node's own copy of a key's part makes of a read
//! ([`local_verdict`]), and what this node answers a peer's `Fetch`
//! ([`fetch_serve_verdict`]). `Cache`'s owner loop, `Shard::rearm_local`
//! and the cluster's fetch responder decide through them, each from one
//! [`Residency`] read before the record is.

use crate::explain::{LocalRead, Residency, ServeVerdict};

/// Whether a part's records are served to peers: owned, or mid disown
/// grace. Pure; unit tested directly.
pub(crate) const fn resident(owns: bool, releasing: bool) -> bool {
    owns || releasing
}

/// What this node's copy makes of a read of a key in a part with
/// `residency`, `live` when the engine holds a live entry the read returns.
/// Not owning comes first, then distrust, which beats a hit, then the
/// entry, then a cold part's miss. Pure; unit tested directly.
pub(crate) const fn local_verdict(residency: &Residency, live: bool) -> LocalRead {
    if !residency.owns {
        LocalRead::NotOwner
    } else if residency.distrusted() {
        LocalRead::Distrusted
    } else if live {
        LocalRead::Hit
    } else if residency.cold() {
        LocalRead::ColdMiss
    } else {
        LocalRead::Miss
    }
}

/// Whether a read in a part with `residency` consults the local copy at
/// all: the [`local_verdict`] of a hit answers. Pure; unit tested directly.
pub(crate) const fn reads_local(residency: &Residency) -> bool {
    local_verdict(residency, true).answers()
}

/// [`fetch_serve_verdict`]'s inputs, each read before the record is.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[allow(
    clippy::struct_excessive_bools,
    reason = "independent inputs to one verdict; its tests exhaust their product"
)]
pub(crate) struct ServeInputs {
    /// The part is unverified or stale.
    pub(crate) distrusted: bool,
    /// The responder holds a record for the key it serves, live or a
    /// tombstone, in a resident part.
    pub(crate) served: bool,
    /// The asker's view hash equals the responder's.
    pub(crate) views_match: bool,
    /// The part is cold.
    pub(crate) cold: bool,
}

/// What this node answers a peer's `Fetch`: distrust declines even a held
/// record, a held record is served whatever the views say, a view
/// mismatch then sends the asker back for a fresher view, and only a warm
/// part's miss is definitive. Pure; unit tested directly.
pub(crate) const fn fetch_serve_verdict(inputs: ServeInputs) -> ServeVerdict {
    if inputs.distrusted {
        ServeVerdict::DeclineDistrusted
    } else if inputs.served {
        ServeVerdict::Serve
    } else if !inputs.views_match {
        ServeVerdict::Stale
    } else if inputs.cold {
        ServeVerdict::DeclineCold
    } else {
        ServeVerdict::Miss
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::time::Duration;

    use super::*;

    /// The read and serve rules as `fcac7a0` wrote them inline, transcribed
    /// literally so the extracted verdicts are checked against the code
    /// they replace. Not to be edited to match a later change.
    #[allow(
        clippy::fn_params_excessive_bools,
        reason = "each parameter is one of the inline code's conditions"
    )]
    mod legacy {
        /// `cache.rs:1699-1707` (`owner_answer`'s two local branches) and
        /// `store/mod.rs:3381-3407` (`rearm_local`): the local copy answers
        /// iff the part is owned and trusted and the read hit or the part
        /// is warm.
        pub(super) fn answers_locally(
            owns: bool,
            unverified: bool,
            live: bool,
            cold: bool,
        ) -> bool {
            owns && !unverified && (live || !cold)
        }

        /// What `ClusterRequestHandler::fetch` (`cluster.rs:1089-1107`)
        /// returned, past its no-shard and not-distributed checks.
        #[derive(Debug, PartialEq, Eq)]
        pub(super) enum Reply {
            Unavailable,
            FoundSome,
            Stale,
            FoundNone,
        }

        pub(super) fn handler(
            unverified: bool,
            held: bool,
            views_match: bool,
            cold: bool,
        ) -> Reply {
            if unverified {
                return Reply::Unavailable;
            }
            if held {
                return Reply::FoundSome;
            }
            if !views_match {
                return Reply::Stale;
            }
            if cold {
                return Reply::Unavailable;
            }
            Reply::FoundNone
        }

        /// `ShardOps::is_cold_part` (`store/mod.rs:5070-5079`).
        pub(super) fn is_cold(cold_marked: bool, unsettled: bool, owns: bool) -> bool {
            cold_marked || (unsettled && owns)
        }

        /// `ResidencySet::is_unverified` (`ownership.rs:945-947`).
        pub(super) fn is_unverified(unverified: bool, stale: bool) -> bool {
            unverified || stale
        }

        /// `Shard::is_resident_part` (`store/mod.rs:2672-2675`).
        pub(super) fn is_resident(owns: bool, releasing: bool) -> bool {
            owns || releasing
        }
    }

    /// Every [`Residency`]: each of the five marks on and off, with and
    /// without a disown grace running.
    fn every_residency() -> Vec<Residency> {
        let mut all = Vec::new();
        for bits in 0u8..32 {
            for releasing_for in [None, Some(Duration::from_millis(5))] {
                all.push(Residency {
                    owns: bits & 1 != 0,
                    releasing_for,
                    cold_marked: bits & 2 != 0,
                    unsettled: bits & 4 != 0,
                    unverified: bits & 8 != 0,
                    stale: bits & 16 != 0,
                });
            }
        }
        all
    }

    #[test]
    fn residency_derivations_match_the_pre_extraction_definitions() {
        for r in every_residency() {
            assert_eq!(
                r.cold(),
                legacy::is_cold(r.cold_marked, r.unsettled, r.owns),
                "{r:?}"
            );
            assert_eq!(r.implicitly_cold(), r.cold() && !r.cold_marked, "{r:?}");
            assert_eq!(
                r.distrusted(),
                legacy::is_unverified(r.unverified, r.stale),
                "{r:?}"
            );
            assert_eq!(
                r.resident(),
                legacy::is_resident(r.owns, r.releasing_for.is_some()),
                "{r:?}"
            );
        }
        for owns in [false, true] {
            assert_eq!(
                Residency::new(owns),
                Residency {
                    owns,
                    releasing_for: None,
                    cold_marked: false,
                    unsettled: false,
                    unverified: false,
                    stale: false,
                }
            );
        }
    }

    #[test]
    fn local_verdict_matches_the_pre_extraction_read_rule() {
        let mut seen = HashSet::new();
        for r in every_residency() {
            for live in [false, true] {
                let verdict = local_verdict(&r, live);
                seen.insert(format!("{verdict:?}"));
                assert_eq!(
                    verdict.answers(),
                    legacy::answers_locally(r.owns, r.distrusted(), live, r.cold()),
                    "{r:?} live={live}: {verdict:?}"
                );
                let expected = if !r.owns {
                    LocalRead::NotOwner
                } else if r.distrusted() {
                    LocalRead::Distrusted
                } else if live {
                    LocalRead::Hit
                } else if r.cold() {
                    LocalRead::ColdMiss
                } else {
                    LocalRead::Miss
                };
                assert_eq!(verdict, expected, "{r:?} live={live}");
                assert_eq!(
                    local_verdict(
                        &Residency {
                            releasing_for: None,
                            ..r
                        },
                        live
                    ),
                    verdict,
                    "a disown grace changes no local verdict"
                );
            }
        }
        assert_eq!(seen.len(), 5, "every verdict occurs: {seen:?}");
    }

    #[test]
    fn reads_local_is_the_gate_before_a_read() {
        for r in every_residency() {
            assert_eq!(reads_local(&r), r.owns && !r.distrusted(), "{r:?}");
            if !reads_local(&r) {
                assert!(
                    !local_verdict(&r, true).answers() && !local_verdict(&r, false).answers(),
                    "a part the gate skips never answers locally: {r:?}"
                );
            }
        }
    }

    /// Every [`ServeInputs`].
    fn every_serve_input() -> Vec<ServeInputs> {
        (0u8..16)
            .map(|bits| ServeInputs {
                distrusted: bits & 1 != 0,
                served: bits & 2 != 0,
                views_match: bits & 4 != 0,
                cold: bits & 8 != 0,
            })
            .collect()
    }

    /// The reply `ClusterRequestHandler::fetch` sends for `verdict`.
    fn reply_of(verdict: ServeVerdict) -> legacy::Reply {
        match verdict {
            ServeVerdict::Serve => legacy::Reply::FoundSome,
            ServeVerdict::Stale => legacy::Reply::Stale,
            ServeVerdict::Miss => legacy::Reply::FoundNone,
            ServeVerdict::DeclineDistrusted | ServeVerdict::DeclineCold => {
                legacy::Reply::Unavailable
            }
        }
    }

    #[test]
    fn fetch_serve_verdict_matches_the_pre_extraction_handler() {
        let mut seen = HashSet::new();
        for inputs in every_serve_input() {
            let verdict = fetch_serve_verdict(inputs);
            seen.insert(format!("{verdict:?}"));
            assert_eq!(
                reply_of(verdict),
                legacy::handler(
                    inputs.distrusted,
                    inputs.served,
                    inputs.views_match,
                    inputs.cold
                ),
                "{inputs:?}: {verdict:?}"
            );
        }
        assert_eq!(seen.len(), 5, "every verdict occurs: {seen:?}");
    }

    #[test]
    fn the_read_decisions_differ_where_the_code_differs() {
        let cold_owned = Residency {
            cold_marked: true,
            ..Residency::new(true)
        };
        assert_eq!(
            local_verdict(&cold_owned, false),
            LocalRead::ColdMiss,
            "a held tombstone is no live entry: a cold owned part asks the owners"
        );
        assert_eq!(
            fetch_serve_verdict(ServeInputs {
                distrusted: false,
                served: true,
                views_match: true,
                cold: true,
            }),
            ServeVerdict::Serve,
            "while the responder serves the tombstone it holds there"
        );

        let releasing = Residency {
            releasing_for: Some(Duration::from_secs(1)),
            ..Residency::new(false)
        };
        assert_eq!(local_verdict(&releasing, true), LocalRead::NotOwner);
        assert!(
            releasing.resident(),
            "the reader needs ownership; the responder serves a releasing part's records"
        );

        assert_eq!(
            fetch_serve_verdict(ServeInputs {
                distrusted: false,
                served: true,
                views_match: false,
                cold: false,
            }),
            ServeVerdict::Serve,
            "only the responder compares views, and a held record beats a mismatch"
        );
        assert_eq!(
            fetch_serve_verdict(ServeInputs {
                distrusted: false,
                served: false,
                views_match: true,
                cold: false,
            }),
            ServeVerdict::Miss,
            "the responder's miss takes an equal view hash for an equal view"
        );

        let distrusted = Residency {
            stale: true,
            ..Residency::new(true)
        };
        assert_eq!(local_verdict(&distrusted, true), LocalRead::Distrusted);
        assert_eq!(
            fetch_serve_verdict(ServeInputs {
                distrusted: true,
                served: true,
                views_match: true,
                cold: false,
            }),
            ServeVerdict::DeclineDistrusted,
            "distrust beats a hit on both sides"
        );
    }

    #[test]
    fn resident_is_owns_or_releasing() {
        for owns in [false, true] {
            for releasing in [false, true] {
                assert_eq!(
                    resident(owns, releasing),
                    legacy::is_resident(owns, releasing)
                );
            }
        }
    }

    #[test]
    fn answers_is_true_only_for_the_answering_variants() {
        assert!(LocalRead::Hit.answers());
        assert!(LocalRead::Miss.answers());
        assert!(!LocalRead::NotOwner.answers());
        assert!(!LocalRead::Distrusted.answers());
        assert!(!LocalRead::ColdMiss.answers());
    }
}
