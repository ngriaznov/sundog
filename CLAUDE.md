# sundog conventions

- Every new bit of code ships with test coverage. A new public method gets a
  test at every layer it is exposed on (`Shard` and `Cache`), a new background
  routine gets its decision logic split into a pure function with a unit test,
  and a new metric gets its value pinned in the exporter test. Before reporting
  a feature done, check each new symbol by name for a test that exercises it.
- Run every local lane before pushing: `cargo fmt --all --check`, the three
  clippy lanes (`--workspace`, `--features sim`, `--features tls,prometheus`)
  with `-D warnings -W clippy::pedantic`, `cargo test --workspace`, the sim
  suite, the tls+prometheus suite, `RUSTDOCFLAGS="-D warnings" cargo doc`, and
  the rightsize container suite. Repeat timing-sensitive new tests ten times. CI
  is the end gate; dispatch it on the branch and wait for green.
- Run `cargo semver-checks --baseline-version <last release> --release-type
  minor` before a minor release; only intentional, changelog-listed breaks may
  remain.
- Reads are TTL-blind: only writes take a per-entry TTL. No read method accepts
  a TTL.
- Deleted or expired entries never resurrect.
- Commits are authored by Nikita Griaznov
  (`17167893+ngriaznov@users.noreply.github.com`), unsigned, with no co-author
  trailers and no model identifiers in any pushed artifact.
- Delegated work runs on Sonnet. Work on a feature branch; merges to `main`,
  releases, and yanks wait for an explicit go.
- Prose is greenfield and describes the code as it is: present tense, no
  wind-ups or hedges.
- Container tests use the `rightsize` crate only; no Docker CLI, compose, or
  bollard in the repo. The test node is the `sundog-testnode` binary.
- Publishing goes through GitHub Actions (`release.yml`, `tag-release.yml`,
  `yank.yml`); the session git proxy cannot push tags. After a release run,
  verify the run's conclusion, not only the crates.io max version.
