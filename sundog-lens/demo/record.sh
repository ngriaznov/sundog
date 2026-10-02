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
     -c '$LENS demo --scenario tour --testnode $NODE --logs $OUT/nodes --log $OUT/tour.log --marks $OUT/marks.txt --color truecolor; echo \$? > $OUT/tour.status' \
     $OUT/tour.cast; tmux wait-for -S lensrec-done"
tmux wait-for lensrec-done
test -s "$OUT/tour.cast"

# 3. The filmed take passes too: the demo exits 0 and its log holds no failed
# key, failed step or timed-out await.
[[ "$(cat "$OUT/tour.status" 2>/dev/null)" == 0 ]] || {
  echo "the filmed tour exited with status $(cat "$OUT/tour.status" 2>/dev/null || echo unknown)" >&2
  exit 1
}
if grep -q ' WARN \| ERROR ' "$OUT/tour.log" 2>/dev/null; then
  echo "the filmed tour logged a failure; see $OUT/tour.log" >&2
  exit 1
fi
# A take over the cast budget is rejected here, before it is rendered; slow
# awaits lengthen a take, so record again.
(($(stat -c%s "$OUT/tour.cast") <= 3000000)) || {
  echo "the take is over 3,000,000 bytes; record again" >&2
  exit 1
}
echo "recorded $OUT/tour.cast"
