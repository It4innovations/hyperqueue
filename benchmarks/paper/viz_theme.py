"""Shared Altair theme for the evaluation figures.

One theme so the figures read as a system rather than five unrelated charts. The palette is the
validated default from the data-viz reference: slot order is the colour-vision-deficiency safety
mechanism, not decoration, so series are assigned slots in order and never cycled.

Validated with the reference validator: lightness band, chroma floor, CVD separation (worst
adjacent 9.1 protan) and normal-vision separation (worst adjacent 19.6) all PASS. On the white
surface three slots -- aqua (2.8:1), magenta (2.7:1), yellow (2.2:1) -- sit below 3:1, which
triggers the **relief rule**: every multi-series chart carries direct labels as well as colour,
so identity is never hue alone.

Tuned for a printed paper, which adds constraints a screen figure does not have:

* **White surface, not off-white.** Paper is already white; an off-white panel prints as a
  visible grey rectangle floating on the page.
* **Greyscale survivability.** Measured luminance gaps between adjacent palette slots are as
  small as 1.7 points (aqua/magenta) and 2.5 (green/blue) -- indistinguishable on a mono printer
  or a photocopy. Colour alone therefore cannot carry identity in print, so line charts add
  `DASHES` as a third channel on top of the direct labels.
* **Text sized to the page, not to the screen.** `paper.tex` is 10pt `article`; figures are
  authored at `TEXT_W_PT` so they are included at `width=\textwidth` with no rescaling, and
  8pt figure text stays 8pt on paper instead of shrinking to 6.
* **A humanist sans for figure text.** Lato, not the paper's Computer Modern. Figure labels are
  read in glances rather than in sentences, and at 8pt a sans with open apertures and a large
  x-height stays legible where a Didone serif's thin strokes start to break up in toner. It also
  marks figure text as figure text, which is the usual convention in a serif-set paper.
* **Darker secondary ink.** Small grey text that is legible on a backlit screen goes weak in
  toner; muted ink is held at ~7:1 rather than the ~3.5:1 a screen theme can afford.

There is no dark variant to keep in step.
"""

from __future__ import annotations

import altair as alt

#: Categorical slots, in the fixed order that passes the CVD gates. Never reorder, never cycle:
#: a ninth series folds into "other" or becomes a facet.
SERIES = [
    "#2a78d6",  # 1 blue
    "#eb6834",  # 2 orange
    "#1baf7a",  # 3 aqua
    "#eda100",  # 4 yellow
    "#e87ba4",  # 5 magenta
    "#008300",  # 6 green
    "#4a3aa7",  # 7 violet
    "#e34948",  # 8 red
]

#: Pure white: the figure sits on the page, not on a visible panel.
SURFACE = "#ffffff"
INK = "#000000"
INK_SECONDARY = "#333333"
#: ~7:1 on white. A screen theme would use something near 3.5:1; toner eats that.
INK_MUTED = "#5c5c5c"
#: Neutral, not warm: a warm grid looks dirty next to black text on white paper.
GRID = "#e8e8e8"
AXIS = "#b0b0b0"

#: Reserved status colours -- never reused as a series.
CRITICAL = "#d03b3b"

#: Lato: a humanist sans, deliberately *not* the paper's Computer Modern. Figure text is scanned,
#: not read, and at 8pt Lato's open apertures and large x-height survive toner where Computer
#: Modern's hairlines thin out. Lato is also narrow for its x-height, which matters here because
#: several figures are width-bound by their labels rather than by their plot areas.
#:
#: The **first** name must resolve on the machine that renders. vl-convert runs its own font scan
#: instead of deferring to fontconfig, and a name it cannot resolve -- "LM Roman 10", "Lato Light",
#: anything but a plain installed family -- does not fall back: it emits a PDF with **no text at
#: all**, silently, at a fifth of the file size. Any change here must be checked with `pdffonts`,
#: which should list a subsetted Regular *and* Bold.
FONT = "Lato, Noto Sans, DejaVu Sans, sans-serif"

#: Dash patterns, in slot order, as the greyscale channel for line charts. Slot 0 is solid so the
#: primary series stays clean; the rest separate by *rhythm*, which survives a photocopy.
DASHES = [
    [1, 0],       # 1 solid
    [6, 2],       # 2 long dash
    [2, 2],       # 3 dot
    [8, 2, 2, 2],  # 4 dash-dot
    [4, 2],       # 5 medium dash
    [1, 2],       # 6 fine dot
    [10, 3],      # 7 very long dash
    [6, 2, 1, 2],  # 8 dash-dot fine
]

#: `\textwidth` of `paper.tex` in PostScript points: 2.5cm margins on the smaller of a4 (16.0cm
#: -> 453pt) and letter (16.6cm -> 470pt). Authoring at the smaller means `width=\textwidth`
#: never *shrinks* the figure, so authored point sizes are the printed point sizes.
#: vl-convert maps 1px -> 1pt in PDF, so these numbers are directly comparable to the page.
TEXT_W_PT = 450

#: Panel plot height. Width is deliberately not a constant: Vega sizes the *plot area*, while
#: axis titles, labels, legends and direct-label overhang all sit outside it, so the width that
#: lands a figure on TEXT_W_PT depends on its panel count and its labels. `fit_and_save()`
#: measures it per figure instead of guessing.
PANEL_H = 190
#: Fallback for `theme.view`, which needs some number for a chart that sets no width of its own.
PANEL_W = 300


@alt.theme.register("hq_eval", enable=True)
def _theme() -> alt.theme.ThemeConfig:
    return {
        "config": {
            "background": SURFACE,
            "font": FONT,
            "view": {"stroke": "transparent", "continuousWidth": PANEL_W, "continuousHeight": PANEL_H},
            "axis": {
                "labelFont": FONT,
                "titleFont": FONT,
                "labelColor": INK_MUTED,
                "titleColor": INK_SECONDARY,
                "labelFontSize": 8,
                "titleFontSize": 8,
                "titleFontWeight": "normal",
                "gridColor": GRID,
                "gridWidth": 0.6,
                "domainColor": AXIS,
                "domainWidth": 0.8,
                "tickColor": AXIS,
                "tickWidth": 0.8,
                "labelFlush": True,
            },
            "legend": {
                "labelFont": FONT,
                "titleFont": FONT,
                "labelColor": INK_SECONDARY,
                "titleColor": INK_SECONDARY,
                "labelFontSize": 8,
                "titleFontSize": 8,
                "titleFontWeight": "normal",
                "symbolStrokeWidth": 1.6,
                "symbolSize": 70,
            },
            "title": {
                "font": FONT,
                "fontSize": 9,
                "fontWeight": "bold",
                "color": INK,
                "anchor": "start",
                "offset": 6,
                "subtitleFont": FONT,
                "subtitleFontSize": 8,
                "subtitleColor": INK_SECONDARY,
            },
            # 2pt rules look bold on a screen and blunt on paper; 1.4pt holds a clean edge at
            # print resolution without disappearing.
            "line": {"strokeWidth": 1.4},
            "point": {"size": 40, "filled": True},
            "text": {"font": FONT, "fontSize": 8},
            "range": {"category": SERIES},
        }
    }


def save(chart: alt.TopLevelMixin, out_stem, formats=(".png", ".pdf")) -> None:
    r"""Write a chart to each format, at the stem given. Prints what it wrote, as the matplotlib
    scripts did, so the sweep runbooks in the READMEs keep working unchanged.

    The PDF is the artifact for the paper -- vector, with text left as embedded subsetted text
    rather than outlines, so it stays selectable and scales cleanly. The PNG is a preview, at
    300 ppi so it is still usable if a venue insists on raster.

    Also reports the finished page size in points, which is the number that decides whether
    `\includegraphics` will rescale the figure and drag the type sizes with it.
    """
    for suffix in formats:
        path = out_stem.with_suffix(suffix)
        chart.save(str(path), ppi=300)
        note = ""
        if suffix == ".pdf":
            w, h = _pdf_page_pt(path)
            if w:
                scale = TEXT_W_PT / w
                fit = "fits \\textwidth" if 0.97 <= scale <= 1.06 else f"would rescale x{scale:.2f}"
                note = f"  [{w:.0f} x {h:.0f} pt, {fit}]"
        print(f"wrote {path}{note}")


def _pdf_page_pt(path):
    """MediaBox of a one-page PDF, in points. vl-convert writes 1px -> 1pt."""
    import re

    try:
        data = path.read_bytes()
    except OSError:
        return None, None
    m = re.search(rb"/MediaBox\s*\[\s*([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)", data)
    if not m:
        return None, None
    x0, y0, x1, y1 = (float(v) for v in m.groups())
    return x1 - x0, y1 - y0


def fit_and_save(build, out_stem, *, target=None, start=140, bounds=(26, 520), rounds=5, **kw) -> None:
    r"""Render `build(panel_width)` at the panel width that makes the finished page `target` wide.

    Panel width is not figure width: axis titles, labels, legends, direct-label overhang and --
    the one that catches people -- a panel title wider than its own panel all add to it. Above a
    floor the page grows point for point with the panel, so a fixed-point walk converges in two
    or three renders; below that floor the page stops responding entirely, and the walk detects
    it and stops rather than pinning the panel to a bound.

    This matters because a figure `\includegraphics` has to rescale drags every type size with
    it: a figure that comes out 940pt wide inside a 450pt text block prints its 8pt labels at
    under 4pt. Panels are also sized per figure rather than per script, since the same script
    emits figures with different panel counts for different scenarios.
    """
    import tempfile
    from pathlib import Path

    target = target or TEXT_W_PT
    lo, hi = bounds

    def page_width(w):
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as handle:
            probe = Path(handle.name)
        try:
            build(w).save(str(probe))
            return _pdf_page_pt(probe)[0]
        finally:
            probe.unlink(missing_ok=True)

    # Two probes first: the page moves by one point per point of panel width *per panel*, so the
    # slope has to be measured rather than assumed -- stepping by the raw error overshoots by the
    # panel count and oscillates.
    seen = [(w, page_width(w)) for w in (start, max(lo, start // 2))]
    seen = [(w, page) for w, page in seen if page]
    for _ in range(max(0, rounds - 2)):
        if not seen or any(abs(page - target) <= 3 for _, page in seen):
            break
        (w1, p1), (w2, p2) = sorted(seen)[:2] if len(seen) == 2 else (sorted(seen)[-2], sorted(seen)[-1])
        slope = (p2 - p1) / (w2 - w1) if w2 != w1 else 0.0
        if slope <= 0.05:
            # No response *between these two probes* does not mean no response anywhere: a legend
            # or title puts a floor under the page, and below it the panel width does nothing.
            # Push above the floor before concluding the figure cannot be sized.
            widest = max(w for w, _ in seen)
            if widest >= hi:
                break
            nxt = min(hi, widest * 2)
        else:
            nxt = max(lo, min(hi, round((target - p2) / slope + w2)))
        if any(nxt == w for w, _ in seen):
            break
        page = page_width(nxt)
        if not page:
            break
        seen.append((nxt, page))

    # Prefer the widest panel that still fits; if nothing fits, the narrowest page there is.
    fits = [(w, page) for w, page in seen if page <= target * 1.03]
    best = max(fits)[0] if fits else (min(seen, key=lambda wp: wp[1])[0] if seen else start)

    save(build(best), out_stem, **kw)
    page, _ = _pdf_page_pt(out_stem.with_suffix(".pdf"))
    if page and page > target * 1.03:
        # Narrowing the panels further will not help: something that does not scale with panel
        # width is setting the figure's size, nearly always a panel title or a one-row legend.
        print(
            f"  note: {out_stem.name} is {page:.0f}pt against a {target:.0f}pt text block "
            f"(x{target / page:.2f}); a title or legend is wider than its panel -- "
            f"shorten it, or wrap the panels with vt.grid()"
        )


GRID_PER_ROW = 3


def grid(panels, *, per_row: int = GRID_PER_ROW, spacing: int = 26, row_spacing: int = 20):
    """Lay panels out in rows of at most `per_row`, as one chart.

    A row of small multiples cannot be made to fit by narrowing the panels past the width of
    their own titles -- five 60pt titles is a 590pt figure whatever the plot areas do. Wrapping
    is the only thing that recovers the text width, and it also keeps each panel wide enough to
    read.
    """
    import altair as alt

    if len(panels) <= per_row:
        return alt.hconcat(*panels, spacing=spacing)
    rows = [panels[i : i + per_row] for i in range(0, len(panels), per_row)]
    return alt.vconcat(*(alt.hconcat(*row, spacing=spacing) for row in rows), spacing=row_spacing)


def dash_scale(field: str, domain, *, legend=None) -> alt.StrokeDash:
    """Dash patterns keyed to the same field (and same domain order) as colour.

    The greyscale channel. Adjacent palette slots differ by as little as 1.7 luminance points,
    so on a mono printer or a photocopy the lines merge; the dash rhythm still separates them.
    Pass the identical `domain` used for colour so slot N always gets dash N.
    """
    return alt.StrokeDash(
        f"{field}:N",
        scale=alt.Scale(domain=list(domain), range=[DASHES[i] for i in range(len(domain))]),
        legend=legend,
    )


def direct_labels(base: alt.Chart, x: str, y: str, series: str, *, dx: int = 6, align: str = "left"):
    """Label each series at its last point.

    Required, not cosmetic: three of the five slots fall below 3:1 contrast on the light surface,
    so the palette's relief rule obliges a second channel besides hue.
    """
    return (
        base.mark_text(align=align, dx=dx, fontSize=8, fontWeight="bold")
        .transform_window(rank="rank()", sort=[alt.SortField(x, order="descending")], groupby=[series])
        .transform_filter(alt.datum.rank == 1)
        .encode(text=alt.Text(f"{series}:N"), color=alt.Color(f"{series}:N", legend=None))
    )
