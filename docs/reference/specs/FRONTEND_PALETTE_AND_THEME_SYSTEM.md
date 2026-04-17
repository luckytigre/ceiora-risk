# Frontend Palette And Theme System

Date: 2026-04-16
Owner: Codex
Status: Active guidance for frontend palette rollout; current working study lives in `frontend/src/app/palette-preview/page.tsx`

## Purpose

Capture the palette process, the decisions made during the palette study, and the rules that should govern how palette and theme choices translate across the Ceiora frontend.

This document is not a loose design note. It is the active reference for:

- how the frontend should feel
- what the palette is trying to achieve
- how palette roles should map to app surfaces
- what kinds of visual behavior are in-bounds versus out-of-bounds
- how to roll the theme into the app without reintroducing visual drift

## Context

Ceiora is not a generic SaaS dashboard and it should not look like one.

The product lives between:

- technical risk tooling
- dense analytical tables and charts
- more editorial public-facing framing on the intro surface

That means the palette cannot optimize for a single mood such as:

- hard institutional coldness
- warm lifestyle softness
- flashy trading-terminal chroma
- playful startup polish

The target is quieter and harder to fake:

- restrained
- slightly warm, but never distinctly warm
- serious, but not clinical
- modern, but not trendy
- airy between sections, compact within technical surfaces
- high taste without looking “designed at”

## Process Summary

The palette was developed through iterative inspection on:

- the actual intro page
- a local palette study page at `frontend/src/app/palette-preview/page.tsx`
- concrete mockups of tables, factor charts, dropdowns, buttons, detail panels, and modal surfaces

The process deliberately moved away from abstract swatches and toward realistic app fragments because the important question was not “do the colors look nice?” but “do they behave correctly in the actual product?”

The study repeatedly tested and refined:

- intro card whites
- app shell/background darks
- light-mode substrate and light-mode signal contrast
- neo-dot background treatment
- display versus body text temperature
- signal colors
- family accents
- analytics-emphasis colors that currently reuse family values
- chart behavior
- method/projection annotations
- control geometry
- density and spacing rules
- light-mode tooltip, dropdown, editorial card, table hover, and modal surfaces

## Goals

### Primary Goals

- Make the product feel calm, expensive, and credible.
- Keep the app slightly lifelike, not sterile.
- Preserve a small trace of warmth across the full product without allowing the UI to read as creamy or sepia.
- Keep technical surfaces dense and efficient.
- Keep public/editorial moments brighter and more composed without splitting into a different brand language.
- Standardize component behavior so the app does not drift into multiple micro-styles.

### Non-Goals

- A fully warm editorial palette across the whole app.
- Rounded “friendly SaaS” controls.
- Pill-heavy menus, toggles, or segmented controls as the default interaction pattern.
- Broad card fills as the default substrate for app content.
- Reusing signal colors as general-purpose branding colors.
- Treating every chart series as equally decorative or equally “pretty.”

## High-Level Decisions

### 1. The app should be slightly warm, but never obviously warm

Warmth belongs in the system only as an undertone.

Implication:

- whites can lean a half-step warm
- the dark shell can lean a half-step warm or neutral
- nothing should read beige, cream-heavy, or sepia

### 2. The intro page and the app must feel related, not identical

The intro page can carry more editorial paper contrast.
The app itself should remain lighter, more architectural, and more background-led.

Implication:

- editorial “paper” surfaces are valid on public-facing sections
- dense app views should not inherit broad paper slabs everywhere

### 3. Most app content should render directly on the background

Ceiora should use cards judiciously.

Default structure should come from:

- spacing
- hairlines
- alignment
- typographic hierarchy

Not from:

- broad opaque panels
- stacked mini-cards
- heavy component-library boxes

### 4. Technical regions should be compact

The app should breathe between sections, but a factor chart, table, readout, or settings fragment should become much denser once the user enters it.

Implication:

- keep section rhythm open
- keep internal analytical rows tight
- avoid wasting vertical space inside charts and tables

### 5. Controls should be quiet and lightweight

The UI should feel authored, not over-styled.

Preferred control system:

- text actions
- rectangular actions
- line-based tabs
- square or lightly architectural dropdowns

Rejected direction:

- pill buttons
- capsule tabs
- bubbly segmented controls
- glow-heavy “ready” states

### 6. Signals, family colors, and method colors must stay distinct

These are separate semantic systems.

- signal colors encode positive/negative/warning/neutral state
- family colors encode model family identity
- analytics-emphasis colors encode category or sleeve emphasis inside technical UI
- method colors encode methodological provenance

They must not collapse into one generic accent system.

Important implementation note:

- separate semantic identities should exist even when two identities currently share the same underlying color value
- this allows the app to keep the current look while preserving the ability to retune family and analytics emphasis independently later

### 7. Projection methods are secondary overlays, not peers to core exposures

Secondary projection loadings should read as subordinate analytical additions.

Implication:

- use restrained neutral flank extensions
- avoid loud or unrelated colors
- distinguish methods by subtle contrast hierarchy, not theatrical styling

### 8. Light mode is a sibling system, not a dark-mode inversion

The light theme should preserve the same Ceiora rules while changing the substrate:

- warm-neutral mineral paper rather than bright white
- darker ink hierarchy than dark mode requires
- the same compact technical density
- the same quiet controls and sparse surface usage

The light theme should not become:

- a generic bright SaaS skin
- a flat inversion of dark mode
- a heavily textured editorial paper field

## Aesthetic Direction

The intended Ceiora direction is:

- institutional restraint with a trace of editorial intelligence

Not:

- severe institutional austerity
- decorative editorial luxury

In practice this means:

- clean serif use only in high-value moments
- quieter sans/body/label usage
- reduced annotation voice
- less all-caps + tracked metadata language
- fewer ornamental marks and framing devices

## Current Palette Principles

The exact values may continue to move slightly, but the functional palette should obey these roles.

### Canvas / Shell

Use a graphite dark that is slightly cool-neutral with only a trace of warmth.

The shell should:

- support white and near-white text
- support the neo-dot field without turning blue
- avoid muddy brown warmth

### Light Canvas / Shell

Use a pale mineral field that remains slightly warm-neutral.

The light substrate should:

- avoid sterile white
- avoid visibly creamy ivory
- remain cleaner and more restrained than the editorial paper surface
- read as a working field, not a decorative background

The light substrate should stay simpler than the dark shell.
Do not add decorative texture just because the darker shell carries the neo-dot field.

### Editorial Surface

Use a restrained paper white that is bright and modern.

Editorial paper should:

- feel cleaner than cream
- remain slightly warm compared with pure neutral white
- be used sparingly

In light mode:

- the editorial paper surface should still be warmer and slightly richer than the base light substrate
- this is how the distinction between `working field` and `narrative surface` survives

### Ink / Text

Text should separate into tiers:

- display white
- primary body ink
- secondary ink
- muted/folio ink

Rules:

- display text may be barely warm
- body text should remain cleaner and more neutral
- utility labels should not overuse mono or overtracked all-caps treatment

### Signal Colors

Positive and negative should be:

- slightly more vibrant than fully dusty institutional colors
- not so warm that they read muddy
- not so vivid that they feel like retail-trading UI

Signals should feel:

- calm
- legible
- slightly energized

### Family Colors

Family colors may have more personality than signals, but should still remain disciplined.

They should:

- distinguish `cUSE`, `cPAR`, and `cMAC`
- never become default call-to-action color
- appear mainly where model-family identity matters

Naming note:

- `cPAR` means `parsimonious and actional regression`
- references to cPAR as a `parallel` family describe its architectural relationship to cUSE, not the acronym expansion

### Method Colors

Method colors should stay limited and intentional.

Rules:

- `Core` can remain neutral
- projection methods can share a disciplined highlight family when appropriate
- in charts, projection loadings should default to neutral gray hierarchy rather than loud accent color

For projection rails specifically:

- `Fundamental Projection` should read slightly stronger than `Returns Projection`
- the distinction should survive even for tiny flank additions
- pattern-dependent encodings such as dotted rails are not reliable for very short additions

### Chart Colors

Charts should prioritize interpretability over prettiness.

Rules:

- do not make every series equally “beautiful”
- maintain clear separation between long and short
- reserve projection/read-through methods as subordinate marks
- allow one or two drier utility colors rather than over-harmonizing everything

For long/short factor charts:

- long and short should split cleanly from a visible zero axis
- chart rows should be compact and one-line where possible
- projection methods should add at the flanks incrementally, not compete as a third primary series

## What Was Explicitly Rejected

The palette process ruled out the following directions:

- overly creamy whites
- obviously warm dark backgrounds
- bright-white or clinical light-mode substrates
- cool-blue panel substrates as the dominant app surface
- overuse of rounded corners and pill controls
- decorative symbols such as `§`, `№`, or Roman numeral framing
- excessive mono/utility annotation language
- large filled swaths that obscure the neo-dot background without adding real structure
- projection overlays that rely on dotted treatment when the segment may be too small to resolve visually
- loud yellow projection rails competing with core long/short exposures
- decorative texture in light mode that makes the substrate feel ornamental

## Layout And Surface Rules

### Global Rule

Most app content should live directly on the background.

### Use Filled Surfaces For

- true modal elevation
- detail/drawer focus
- editorial public moments
- rare high-contrast grouping where spacing and rules are insufficient

### Avoid Filled Surfaces For

- generic tables
- routine dashboard summaries
- top-level app sections
- every chart container by default

In light mode, this rule becomes even more important.

Because the substrate is already bright:

- broad white or off-white slabs add very little structure
- overusing them makes the app feel heavier and more generic, not more premium

### Internal Density Rule

Outside technical surfaces:

- permit breath
- keep margins open

Inside technical surfaces:

- tighten rows
- shorten inter-item spacing
- remove duplicate separators
- prefer one-row analytical structures when possible

## Controls And Motion

### Preferred Controls

- plain text action
- rectangular action
- line-based tab
- architectural dropdown
- quiet typeahead

### Motion Direction

Motion should be minimal and useful.

Use:

- brief color transitions
- subtle line shifts
- slight translation where it helps affordance

Avoid:

- bounce
- glow theatrics
- soft springiness
- oversized hover transformations

The motion standards can remain shared across dark and light modes.
The surface, border, and contrast values change; the movement language should not.

## How The Palette Should Function In The Repo

Palette choices should be implemented semantically, not as one-off hex substitutions.

The frontend should expose semantic token groups such as:

- `canvas`
- `ink`
- `surface`
- `signal`
- `analytics`
- `method`
- `family`
- `chart`
- `interaction`

### Semantic Identity Rule

Separate semantic identities should exist for:

- family identity
- analytics emphasis
- signal state
- method provenance

These identities may temporarily reuse the same literal values, but they should not be implemented as one shared token by default.

### Required Rule

Components should consume semantic roles, not direct literals.

Examples:

- tables consume `ink`, `signal`, `method`, `rule`
- stats and analytical emphasis consume `analytics`, not `family`
- factor charts consume `signal`, `chart`, `method`
- nav and controls consume `interaction`, `ink`, `rule`
- editorial landing surfaces consume `surface-editorial`

This prevents visual drift and keeps future retuning tractable.

## Translation Rules Across Pages, Features, And Content

### Intro / Public Landing

The public surface may carry:

- more paper contrast
- more editorial composition
- slightly warmer highlights

But it must still belong to the same shell and ink family as the app.

### Main App Shell

The authenticated app should bias toward:

- background-led layout
- quiet controls
- dense analytical surfaces
- minimal card use

### Tables

All tables should share one base style language:

- compact header rhythm
- quiet rules
- dense numeric cadence
- subtle hover
- restrained method labeling
- optional alternating rows only when they remain extremely subtle

Do not invent different table aesthetics per feature.

### Charts

Charts should:

- remain compact
- avoid excess annotation
- use consistent signal and long/short color behavior
- keep secondary methods subordinate

Tooltip/crosshair surfaces should:

- get their own light and dark surface treatments
- stay compact
- never rely on dark-mode contrast values in light mode

### Detail Panels / Drawers

These can take a slightly more elevated surface treatment than the base app, but should still feel architectural rather than soft.

### Modals

Modals are one of the few places where a true elevated surface is appropriate.

They should:

- feel deliberate
- not look like frosted, rounded SaaS trays
- maintain compact hierarchy

### Forms And Settings

Settings and form controls should be quiet and compact.

In particular:

- remove pill-like segmented controls over time
- avoid rounded choice cards as the default form idiom
- keep inputs/dropdowns visually related to the rest of the shell

### Light-Mode Surface Counterparts

The following surfaces require explicit light-mode counterparts and should not be reused blindly from dark mode:

- tooltip / hover readout
- dropdown / popover
- editorial card
- modal / elevated panel
- table hover / selected state

The geometry and motion can remain shared.
The substrate, border, and text contrast values must be adjusted for light ground.

## Rollout Guidance

When translating this work into the real app, do not do a random page-by-page color sweep.

Use phases:

1. app shell and text tokens
2. nav, menu, dropdown, button primitives
3. tables and dense data surfaces
4. charts, tooltips, and projection treatments
5. detail panels, modals, and form controls
6. light-mode sibling tokens and counterpart surfaces
7. public/editorial pages

### Implementation Rule

Each phase should:

- remove hardcoded component literals where possible
- replace them with semantic tokens
- converge duplicate component styles rather than adding new variants

## Relationship To The Local Palette Study

The active palette study currently lives at:

- `frontend/src/app/palette-preview/page.tsx`

That file is a working artifact, not the long-term system of record.

Its purpose is to:

- pressure-test palette behavior
- validate chart/table/control density
- refine motion and interaction standards
- compare proposed styling against realistic Ceiora fragments
- validate dark and light sibling systems before global rollout

Once real app tokenization is underway, the semantic token layer in the frontend should become authoritative, while the preview remains a diagnostic and exploration surface.

## Practical Acceptance Criteria

The palette/theme rollout is succeeding when the app:

- feels slightly warm but not warm
- feels calmer and more expensive than a generic SaaS dashboard
- uses fewer broad cards
- stays compact inside analytical surfaces
- keeps controls quiet
- reduces annotation noise
- preserves strong signal/family/method separation
- preserves separate semantic identities even when some values are intentionally reused
- gives light mode its own disciplined counterpart surfaces rather than a naive inversion
- makes public/editorial pages and dense app views feel like one brand, not two

If a later change makes the app feel:

- creamy
- overly blue
- too pill-based
- too card-heavy
- too annotated
- too decorative

then that change is likely violating this spec even if the individual color values are technically consistent.
