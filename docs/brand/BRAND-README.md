# Kahshe brand assets — v1.1

Scheme: **Dual**. Teal carries identity and structure. Amber means a match. Nothing else gets colour.

## Contents

| File | What it is |
|---|---|
| `kahshe-brand-document.html` | The full standard — mark, type, colour discipline, applications |
| `tokens.css` | CSS custom properties |
| `tokens.json` | Same tokens for design tools and build pipelines |
| `logo-wordmark.svg` | Primary lockup, light backgrounds |
| `logo-wordmark-reversed.svg` | For dark backgrounds |
| `logo-wordmark-mono.svg` | Single colour (ink) |
| `logo-icon.svg` | Square icon, 120px |
| `logo-wordmark-indexed.svg` | With subscript zero |
| `logo-icon-teal.svg` | Icon on teal fill |
| `favicon.svg` | 32px — bare `k`, brackets dropped |
| `favicon-16.png` / `favicon-32.png` | Raster fallbacks |
| `apple-touch-icon-180.png` / `icon-512.png` | Touch and store icons |

## About the SVGs

All logo SVGs are **outlined paths**, generated from IBM Plex metrics — no live text, no font
dependency at render time. Spacing is balanced by **white area, not by measured gap** — the two are not the same thing.
`k` meets the opening bracket with a flat vertical stem, while `e` is x-height only and sits
between the closing bracket's arms, so equal gaps read as unequal space. Measured ink gaps are
therefore deliberately asymmetric:

| junction | ink gap | average white |
|---|---|---|
| `[` → `k` | 0.2163em | 21.45px @52pt |
| `e` → `]` | 0.1177em | 21.45px @52pt |
| icon `[` → `k` | 0.150em | symmetrical |
| icon `k` → `]` | 0.150em | symmetrical |

The two wordmark gaps are unequal on purpose — do not "correct" them to match. The icon is symmetrical by choice: inside a cropped tile the brackets read as a frame, and even gaps hold better than optical ones. The word carries
−0.03em tracking.

PNG raster fallbacks are included for favicons and touch icons.

Fonts: IBM Plex Sans and IBM Plex Mono, SIL Open Font License 1.1 — logo use permitted.
Ship the OFL licence file alongside any asset that embeds them as live text.

## The one rule that matters

If something is coloured, something is true about it.

- **Teal** — brackets, links, focus, active state, the tool's own voice in CLI output.
- **Amber** — a hit count, a matched row, the file that was actually read. Never a heading, button, or logo.
- **Neutrals** — everything else, without exception.

Never both on one element. If it's both identity and result, it's a result: amber wins.
A third colour means you've found a new state — name it before you colour it.
Errors take red from the system palette, so amber keeps meaning "found".

## Naming and attribution

Write "Kahshe for Apache Iceberg". Never "Kahshe Iceberg" or "Iceberg Kahshe", and never
place the Iceberg logo in or beside the mark.

> Apache, Apache Iceberg, and Iceberg are trademarks of the Apache Software Foundation.
