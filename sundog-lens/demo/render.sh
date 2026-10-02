#!/usr/bin/env bash
# Publishes the recorded tour: copies the cast to assets/ and renders the
# README GIF from it with render.py. Needs: pip install pyte pillow imageio-ffmpeg
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
cp target/lens-video/tour.cast assets/sundog-lens.cast
python3 sundog-lens/demo/render.py assets/sundog-lens.cast assets/sundog-lens.gif --fps "${GIF_FPS:-10}"
ls -l assets/sundog-lens.cast assets/sundog-lens.gif
