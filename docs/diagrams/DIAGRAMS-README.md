# kahshe — animated diagrams

Two looping SVGs, 7-second cycle, 760×360.

| File | Shows |
|---|---|
| `scene-scan.svg` | Engine plans against the catalog, then reads all ten files. The index slot is drawn empty and labelled `no index`, so the pair reads as a before/after |
| `scene-kahshe.svg` | Kahshe reads the index out of object storage, prunes eight, engine reads two |
| `preview.html` | Both, side by side, animating |

## Using them

Inline the SVG in your HTML to get the animation. As `<img src="scene-scan.svg">` the
animation still runs — the CSS is inside the file and there is no external font dependency,
because every label is outlined paths.

**The static state is the finished state.** Any renderer that ignores CSS animation — a PDF
export, some README pipelines, a thumbnailer — shows the completed diagram rather than an
empty frame. Nothing is hidden in the base markup.

`prefers-reduced-motion: reduce` stops the loop and leaves that same completed state.

## The sequence in scene-kahshe

Engine → kahshe → catalog → back to kahshe → **kahshe reads the index from object storage** →
eight files dim, two go amber → pruned plan returns to the engine → engine reads two data files.

The index sits inside the object storage panel rather than inside the kahshe node, because
that is where it actually lives: a sidecar in the same bucket, read during planning, before
any data file is touched. It carries min/max stats, bloom filters, and n-gram postings —
the pruning tier and the text tier in one sidecar. Nothing is read from the data grid until after the pruning happens —
the timing enforces that, not just the labels.

## The kahshe node

It is the only **light surface** in the frame: the brand's primary lockup — ink letters, teal
brackets — sitting on its own pale card. No glow, no border — a light surface on a dark ground
already has all the edge it needs.

The reason it is drawn this way rather than filled with an accent: it is distinguished by
**value, not hue**. Teal already means structure and amber already means result, so borrowing
either to say "this is the product" would blunt a colour that is carrying meaning elsewhere.
Inverting to light says *new element* without spending anything from the palette — and it puts
the logo on the surface it was designed for.

## Planning costs the same

Both scenes hit object storage at **2.05s**. The extra hops in `scene-kahshe` — engine to
kahshe, kahshe to catalog, the index read, the pruning — all fit inside the same planning
window, because that is metadata-scale work. Drawing the index lookup as additional elapsed
time would claim kahshe makes planning slower, which is the opposite of the argument.

The phase bar at the bottom of each scene makes this checkable: the grey `plan` segment is
pixel-identical in both, and only the teal `read` segment differs — 3.36s against 0.79s.

## Timing model

Every animated element runs `7s linear infinite` with **no `animation-delay` anywhere**. Each
element's start time is baked into its own keyframe percentages instead, so all of them share
one clock and reset together on the loop.

This matters: with per-element delays, each element gets its own phase-shifted 7s cycle, and
elements from one pass overlap the start of the next. It looks like independent intervals
rather than a sequence, because that is exactly what it is.

## The read arrow

One arrow, moving. It draws to a file, holds ~340 ms, disappears, and redraws to the next —
ten stops in `scene-scan` (1.75s to 5.15s of the cycle), two in `scene-kahshe`. The windows
are timed so no two are ever on screen together; the effect is a single arrow working down
the row rather than a fan of simultaneous reads, which is what a scan actually looks like.

Visited files stay lit, so the damage accumulates while the arrow moves on.

The arrow is teal because a read is structure. Only the files that matched carry amber.

**Static fallback:** with no animation, all the arrows show at once. That still reads
correctly — ten reads versus two — so nothing is lost in a PDF or a thumbnailer.

## Colour discipline

Teal for structure, the planning arrows, the read path, and every component box including
kahshe's. Amber only for the two matched files, the arrow carrying the pruned plan back to
the engine, and the resulting count — results, never identity. Pruned files drop to 32% opacity rather than changing hue — absence is not a state
that earns colour.

## Editing

Regenerate rather than hand-editing paths — the labels are outlined Plex, not live text.
Numbers, node labels, and the tile count are all near the top of the generator.

## Note

SVGs delivered through chat carry an embedded C2PA manifest (~7.7 KB). Strip with:

    sed -i 's/<metadata>.*<\/metadata>//; s/ xmlns:c2pa="[^"]*"//' *.svg
