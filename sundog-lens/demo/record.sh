#!/usr/bin/env bash
# Records the sundog-lens tour into target/lens-video/. Linux only.
# Needs tmux and asciinema. If a run is interrupted, stop the fleet with
#   pkill -f 'sundog-testnode lens-demo'
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
OUT=target/lens-video
rm -rf "$OUT"
mkdir -p "$OUT"
cargo build --release -p sundog-testnode --features prometheus
cargo build --release -p sundog-lens
LENS="$PWD/target/release/sundog-lens"
NODE="$PWD/target/release/sundog-testnode"

# 1. Smoke: the tour passes headless, within budget, before it is filmed.
start=$(date +%s)
"$LENS" demo --scenario tour --headless --testnode "$NODE" --logs "$OUT/smoke" | tee "$OUT/smoke.log"
elapsed=$(($(date +%s) - start))
((elapsed <= 95)) || {
  echo "tour took ${elapsed}s; shorten its pauses" >&2
  exit 1
}

# 2. Record in a fixed 140x40 pty with truecolor.
tmux kill-session -t lensrec 2>/dev/null || true
tmux new-session -d -s lensrec -x 140 -y 40 \
  "env TERM=xterm-256color COLORTERM=truecolor asciinema rec --overwrite --quiet \
     --cols 140 --rows 40 --title 'sundog-lens tour' \
     -c '$LENS demo --scenario tour --testnode $NODE --logs $OUT/nodes --marks $OUT/marks.txt --color truecolor' \
     $OUT/tour.cast; tmux wait-for -S lensrec-done"
tmux wait-for lensrec-done
test -s "$OUT/tour.cast"
echo "recorded $OUT/tour.cast"
