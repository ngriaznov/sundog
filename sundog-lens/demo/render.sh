#!/usr/bin/env bash
# Publishes the recorded tour: copies the cast to assets/ and renders the
# README GIF from it with render.py. Needs: pip install pyte pillow imageio-ffmpeg
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
cp target/lens-video/tour.cast assets/sundog-lens.cast
python3 sundog-lens/demo/render.py assets/sundog-lens.cast assets/sundog-lens.gif --fps "${GIF_FPS:-10}"
ls -l assets/sundog-lens.cast assets/sundog-lens.gif

# The README budgets: lower GIF_FPS (or shorten the tour's pauses) if over.
(($(stat -c%s assets/sundog-lens.cast) <= 3000000)) || {
  echo 'cast over 3,000,000 bytes' >&2
  exit 1
}
(($(stat -c%s assets/sundog-lens.gif) <= 8000000)) || {
  echo 'GIF over 8,000,000 bytes; lower GIF_FPS' >&2
  exit 1
}
