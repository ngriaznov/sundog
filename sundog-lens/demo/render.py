#!/usr/bin/env python3
"""Render an asciicast to a GIF for the README.

The cast is replayed with pyte, each screen is drawn with Pillow at a fixed
cell size and the frames go to ffmpeg, which maps every pixel to a palette with
no dithering and rectangle diffs, so a cell that is blank in the terminal is
blank in the GIF. The palette is built here, not by palettegen, which averages
solid cell colors with their neighbors: the flat colors (cell backgrounds, text
colors, lines) are kept exactly and the anti-aliasing shades take the slots
that remain, so the lens's blended colors survive. Box-drawing and block
glyphs are drawn as lines and rectangles that fill their cell, so borders and
bars are solid, and braille dots are square pixels in the flat text color.

    pip install pyte pillow imageio-ffmpeg
    render.py IN.cast OUT.gif [--fps 10] [--font-size 13] [--idle 2] [--hold 4]
"""

import argparse
import collections
import json
import subprocess
import sys
import tempfile
from pathlib import Path

import imageio_ffmpeg
import pyte
from PIL import Image, ImageColor, ImageDraw, ImageFont

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


# Lines and blocks, drawn geometrically. Each glyph is a list of (x0, y0, x1, y1)
# fractions of the cell, 0 to 1 from the top left; "heavy" lines are two pixels.
LIGHT, HEAVY = 1, 2
BLOCKS = {
    "\u2580": [(0, 0, 1, .5)], "\u2584": [(0, .5, 1, 1)], "\u2588": [(0, 0, 1, 1)],
    "\u258c": [(0, 0, .5, 1)], "\u2590": [(.5, 0, 1, 1)],
}
# Corners are a line pair joined by a quarter circle: (horizontal side, vertical side).
CORNERS = {"\u256d": ("r", "b"), "\u256e": ("l", "b"), "\u2570": ("r", "t"), "\u256f": ("l", "t")}
LINES = {  # glyph: (orientation, weight, extent along the cell, dash count)
    "\u2502": ("v", LIGHT, (0, 1), 1), "\u2500": ("h", LIGHT, (0, 1), 1),
    "\u2501": ("h", HEAVY, (0, 1), 1), "\u2578": ("h", HEAVY, (0, .5), 1),
    "\u250a": ("v", LIGHT, (0, 1), 4), "\u2504": ("h", LIGHT, (0, 1), 3),
}


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
        self.ch = (ascent + descent + 3) // 2 * 2  # even, so halves match
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

    def shape(self, draw, glyph, x, top, fg):
        """Draw a box, block or braille glyph geometrically; False for any other."""
        left, cw, ch = x * self.cw, self.cw, self.ch
        mid_x, mid_y = left + cw // 2, top + ch // 2
        if "\u2800" <= glyph <= "\u28ff":  # braille: a 2x2 pixel square per set dot
            dots = ord(glyph) - 0x2800
            for bit in range(8):
                if dots >> bit & 1:
                    col, row = (bit // 3, bit % 3) if bit < 6 else (bit - 6, 3)
                    px, py = left + cw // 8 + col * (cw // 2), top + ch // 10 + row * (ch // 5)
                    draw.rectangle([px, py, px + 1, py + 1], fill=fg)
        elif glyph in BLOCKS:
            for x0, y0, x1, y1 in BLOCKS[glyph]:
                draw.rectangle([left + round(x0 * cw), top + round(y0 * ch),
                                left + round(x1 * cw) - 1, top + round(y1 * ch) - 1], fill=fg)
        elif glyph in LINES:
            way, weight, (a, b), dashes = LINES[glyph]
            for i in range(dashes):  # dashes fill 2/3 of each span
                lo, hi = (i + (0 if dashes == 1 else .15)) / dashes, (i + (1 if dashes == 1 else .85)) / dashes
                lo, hi = a + (b - a) * lo, a + (b - a) * hi
                if way == "h":
                    y = mid_y - (weight - 1)
                    draw.rectangle([left + round(lo * cw), y, left + round(hi * cw) - 1, mid_y], fill=fg)
                else:
                    draw.rectangle([mid_x, top + round(lo * ch), mid_x, top + round(hi * ch) - 1], fill=fg)
        elif glyph in CORNERS:
            across, down = CORNERS[glyph]
            r = cw // 2
            cx = mid_x + r if across == "r" else mid_x - r
            cy = mid_y + r if down == "b" else mid_y - r
            box = [cx - r, cy - r, cx + r, cy + r]
            start = {("r", "b"): 180, ("l", "b"): 270, ("r", "t"): 90, ("l", "t"): 0}[(across, down)]
            draw.arc(box, start, start + 90, fill=fg)
            draw.line([cx, mid_y, left + cw - 1, mid_y] if across == "r" else [left, mid_y, cx, mid_y], fill=fg)
            draw.line([mid_x, cy, mid_x, top + ch - 1] if down == "b" else [mid_x, top, mid_x, cy], fill=fg)
        else:
            return False
        return True

    def paint(self, screen):
        img = Image.new("RGB", (screen.columns * self.cw, screen.lines * self.ch), BG)
        draw = ImageDraw.Draw(img)
        self.flat = {ImageColor.getrgb(BG)}  # the flat colors of this frame
        for y in range(screen.lines):
            row = screen.buffer[y]
            top = y * self.ch
            cells = []
            for x in range(screen.columns):
                c = row[x]
                fg, bg = color(c.fg, FG), color(c.bg, BG)
                if c.reverse:
                    fg, bg = bg, fg
                cells.append((c.data, fg, c.bold, c.underscore))
                self.flat.add(ImageColor.getrgb(bg))
                if c.data.strip():
                    self.flat.add(ImageColor.getrgb(fg))
                if bg != BG:
                    draw.rectangle([x * self.cw, top, (x + 1) * self.cw - 1, top + self.ch - 1], fill=bg)
            for x, (ch, fg, bold, underline) in enumerate(cells):
                if ch.strip() == "":
                    continue
                if underline:  # one pixel between the baseline and the descent
                    line = top + self.base + 1
                    draw.line([x * self.cw, line, (x + 1) * self.cw - 1, line], fill=fg)
                if self.shape(draw, ch, x, top, fg):
                    continue
                font = self.font(ch, bold)
                left = x * self.cw
                if font is self.wide:  # centre a proportional fallback glyph in its cell
                    left += (self.cw - font.getlength(ch)) / 2
                draw.text((left, top + self.base), ch, fill=fg, font=font, anchor="ls")
        return img


def frames(events, grid, painter, fps, hold):
    """Yield (image, painted) once per 1/fps second, starting at the first tick
    whose screen has content. The screen is painted only when it changed, and
    `painted` says so; an unchanged tick repeats the held image. The last frame
    is held for `hold` more seconds."""
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
        painted = now != key
        if painted:
            if not any(c.data.strip() for row in screen.buffer.values() for c in row.values()):
                continue
            key, held = now, painter.paint(screen)
        if held is not None:
            yield held, painted


FLAT_COLORS = 215  # the slots for flat colors; the other slots are shades


def palette(events, grid, painter, fps, hold):
    """Pass 1: one palette for the whole GIF (a GIF stores only its changes
    against a single palette). The flat colors that most frames use are kept
    exactly; the few that a single frame shows, such as the steps of a fade,
    map to their nearest neighbor. The remaining slots hold the most common
    anti-aliasing shades."""
    flats, shades = collections.Counter(), collections.Counter()
    for image, painted in frames(events, grid, painter, fps, hold):
        flats.update(painter.flat)
        if painted:
            shades.update({c: n for n, c in image.getcolors(1 << 24)})
    colors = [c for c, _ in flats.most_common(FLAT_COLORS)]
    colors += [c for c, _ in shades.most_common() if c not in flats][: 255 - len(colors)]
    # The last entry is transparent: paletteuse paints it where a frame repeats
    # the one before, so the GIF stores only what changed.
    pixels = (colors + colors[:1] * 255)[:255]
    return b"".join(bytes((*rgb, 255)) for rgb in pixels) + bytes(4)


def ffmpeg(frame_iter, palette_file, size, fps, gif):
    exe = imageio_ffmpeg.get_ffmpeg_exe()
    cmd = [exe, "-v", "error", "-y",
           "-f", "rawvideo", "-pix_fmt", "rgb24", "-s", "%dx%d" % size, "-framerate", str(fps), "-i", "-",
           "-f", "rawvideo", "-pix_fmt", "rgba", "-s", "16x16", "-i", str(palette_file),
           "-lavfi", "[0:v][1:v]paletteuse=dither=none:diff_mode=rectangle", "-loop", "0", gif]
    proc = subprocess.Popen(cmd, stdin=subprocess.PIPE)
    for image, _ in frame_iter:
        proc.stdin.write(image.tobytes())
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
        pal = Path(tmp) / "palette.rgba"  # one 16x16 image
        pal.write_bytes(palette(events, grid, painter, a.fps, a.hold))
        ffmpeg(frames(events, grid, painter, a.fps, a.hold), pal, size, a.fps, a.gif)


if __name__ == "__main__":
    main()
