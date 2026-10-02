#!/usr/bin/env python3
"""Render an asciicast to a GIF for the README.

The cast is replayed with pyte, each screen is drawn with Pillow at a fixed
cell size and the frames go to ffmpeg, which builds one exact palette and maps
every pixel to it with no dithering and rectangle diffs, so a cell that is
blank in the terminal is blank in the GIF.

    pip install pyte pillow imageio-ffmpeg
    render.py IN.cast OUT.gif [--fps 10] [--font-size 13] [--idle 2] [--hold 4]
"""

import argparse
import json
import subprocess
import sys
import tempfile
from pathlib import Path

import imageio_ffmpeg
import pyte
from PIL import Image, ImageDraw, ImageFont

FONTS = "/usr/share/fonts/truetype/dejavu/"
BG, FG = "#12110f", "#ece6d9"
# The theme: ANSI colors by pyte's names. Truecolor and 256-color cells arrive
# from pyte as hex strings and are used as they are.
NAMES = dict(zip(
    ["black", "red", "green", "brown", "blue", "magenta", "cyan", "white"],
    ["12110f", "ff6b6b", "8fd16a", "f2b544", "6cb6ff", "c792ea", "6cc4e8", "ece6d9"],
))
NAMES.update(zip(
    ["brightblack", "brightred", "brightgreen", "brightbrown", "brightblue",
     "brightmagenta", "brightcyan", "brightwhite"],
    ["7d776b", "ff8a8a", "a8e08a", "ffd580", "89ddff", "e2a8ff", "9ee0f0", "ffffff"],
))


def color(name, default):
    """A pyte color (name or hex) as a Pillow color."""
    if name == "default":
        return default
    return "#" + NAMES.get(name, name)


def read_cast(path, idle):
    """The cast's size and its output events as (seconds, text), with every
    gap longer than `idle` seconds cut down to `idle`."""
    with open(path, encoding="utf-8") as f:
        header = json.loads(f.readline())
        relative = header.get("version") == 3
        size = header["term"] if relative else header
        cols, rows = size.get("cols", size.get("width")), size.get("rows", size.get("height"))
        events, t, shift, last = [], 0.0, 0.0, 0.0
        for line in f:
            at, kind, data = json.loads(line)
            t = t + at if relative else at
            if kind != "o":
                continue
            if "\x1b[?1049l" in data:  # the app left its screen; the rest is shell noise
                break
            if t - last > idle:
                shift += t - last - idle
            last = t
            events.append((t - shift, data))
    return (cols, rows), events


class Painter:
    """Draws a pyte screen to an RGB image at a fixed cell size."""

    def __init__(self, size):
        self.regular = ImageFont.truetype(FONTS + "DejaVuSansMono.ttf", size)
        self.bold = ImageFont.truetype(FONTS + "DejaVuSansMono-Bold.ttf", size)
        self.wide = ImageFont.truetype(FONTS + "DejaVuSans.ttf", size)
        self.cw = round(self.regular.getlength("M"))
        ascent, descent = self.regular.getmetrics()
        self.ch = ascent + descent + 2
        self.base = ascent + 1
        # What the mono font draws for a character it lacks.
        self.notdef = self.ink(self.regular, "\U0010ffff")
        self.has = {}

    def ink(self, font, ch):
        """The pixels the font draws for `ch`."""
        img = Image.new("L", (self.cw * 2, self.ch * 2))
        ImageDraw.Draw(img).text((0, self.base), ch, fill=255, font=font, anchor="ls")
        return img.tobytes()

    def font(self, ch, bold):
        if ch not in self.has:
            self.has[ch] = self.ink(self.regular, ch) != self.notdef
        if self.has[ch]:
            return self.bold if bold else self.regular
        return self.wide

    def paint(self, screen):
        img = Image.new("RGB", (screen.columns * self.cw, screen.lines * self.ch), BG)
        draw = ImageDraw.Draw(img)
        for y in range(screen.lines):
            row = screen.buffer[y]
            top = y * self.ch
            cells = []
            for x in range(screen.columns):
                c = row[x]
                fg, bg = color(c.fg, FG), color(c.bg, BG)
                if c.reverse:
                    fg, bg = bg, fg
                cells.append((c.data, fg, bg, c.bold))
                if bg != BG:
                    draw.rectangle([x * self.cw, top, (x + 1) * self.cw - 1, top + self.ch - 1], fill=bg)
            for x, (ch, fg, _, bold) in enumerate(cells):
                if ch.strip() == "":
                    continue
                font = self.font(ch, bold)
                left = x * self.cw
                if font is self.wide:  # centre a proportional fallback glyph in its cell
                    left += (self.cw - font.getlength(ch)) / 2
                draw.text((left, top + self.base), ch, fill=fg, font=font, anchor="ls")
        return img


def frames(events, grid, painter, fps, hold, changed_only):
    """Yield one RGB frame per 1/fps second. The screen is painted only when it
    changed; an unchanged tick repeats the held frame (or is skipped with
    `changed_only`). The last frame is held for `hold` more seconds."""
    screen = pyte.Screen(*grid)
    stream = pyte.Stream(screen)
    end = events[-1][0] + hold
    held, key, i = None, None, 0
    for tick in range(int(end * fps) + 1):
        t = tick / fps
        while i < len(events) and events[i][0] <= t:
            stream.feed(events[i][1])
            i += 1
        now = tuple(tuple(tuple(screen.buffer[y][x]) for x in range(screen.columns)) for y in range(screen.lines))
        if now != key:
            key, held = now, painter.paint(screen).tobytes()
        elif changed_only:
            continue
        yield held


def ffmpeg(args, frame_iter, size, fps):
    cmd = [imageio_ffmpeg.get_ffmpeg_exe(), "-v", "error", "-y", "-f", "rawvideo", "-pix_fmt", "rgb24",
           "-s", "%dx%d" % size, "-framerate", str(fps), "-i", "-", *args]
    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE)
    for frame in frame_iter:
        proc.stdin.write(frame)
    proc.stdin.close()
    if proc.wait():
        sys.exit("ffmpeg failed")


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("cast")
    ap.add_argument("gif")
    ap.add_argument("--fps", type=int, default=10)
    ap.add_argument("--font-size", type=int, default=13)
    ap.add_argument("--idle", type=float, default=2.0, help="longest silence kept, in seconds")
    ap.add_argument("--hold", type=float, default=4.0, help="seconds the last frame is held")
    a = ap.parse_args()

    grid, events = read_cast(a.cast, a.idle)
    painter = Painter(a.font_size)
    size = (grid[0] * painter.cw, grid[1] * painter.ch)
    with tempfile.TemporaryDirectory() as tmp:
        palette = Path(tmp) / "palette.png"
        # Pass 1: one exact palette over every distinct frame.
        ffmpeg(["-vf", "palettegen=stats_mode=full", "-update", "1", str(palette)],
               frames(events, grid, painter, a.fps, a.hold, True), size, a.fps)
        # Pass 2: every frame mapped to it with no dithering.
        ffmpeg(["-i", str(palette), "-lavfi", "paletteuse=dither=none:diff_mode=rectangle",
                "-loop", "0", a.gif],
               frames(events, grid, painter, a.fps, a.hold, False), size, a.fps)


if __name__ == "__main__":
    main()
