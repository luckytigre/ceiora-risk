# LSEG Spot Checks

Date: 2026-03-16  
Mode: Read-only  
Seed: `20260316`

## Method

- Sample size: `20` securities
- Date sample:
  - `2016-02-29`
  - `2018-04-30`
  - `2021-02-26`
  - `2023-10-31`
  - `2026-02-27`
- Fields checked:
  - ticker
  - isin
  - exchange
  - country
  - sector
  - business sector
  - price
  - market cap
  - name
- DB source: local source archive
- LSEG source: fresh read-only point-in-time pulls via the vendored LSEG client
- Raw comparison artifact: `/tmp/barra_lseg_spot_checks.json`

## Sample

- `AAPL.OQ`
- `BABA.N`
- `VTRU.P`
- `NBL.O^J20`
- `ACCL.OQ`
- `GOOGL.OQ`
- `TSM.N`
- `BKR.O`
- `DELL.N`
- `WTW.O`
- `DG.N`
- `VRTX.OQ`
- `LECO.OQ`
- `RRR.OQ`
- `NUVL.OQ`
- `WYNN.OQ`
- `VOYA.N`
- `STR.N`
- `GBX.N`
- `MC.N`

The sample mixes mega-cap, mid-cap, smaller names, ADR / non-US listings in scope, and a few legacy / edge names.

## Summary

- Total security/date pairs: `100`
- Complete matches: `83`
- Pairs with any mismatch: `17`

Field-level match counts:

| Field | Match | Mismatch |
| --- | ---: | ---: |
| ticker | 85 | 15 |
| isin | 93 | 7 |
| exchange | 100 | 0 |
| country | 100 | 0 |
| sector | 100 | 0 |
| business sector | 100 | 0 |
| price | 95 | 5 |
| market cap | 100 | 0 |
| name | 100 | 0 |

## Interpretation

Confirmed:

- Exchange, country, sector, business sector, market cap, and name matched cleanly across the sample.
- The spot checks do not point to a broad current classification failure.
- The mismatches cluster in legacy / corporate-action-heavy names rather than in ordinary current large-cap equities.

Likely lineage / static-identifier mismatches:

- `VTRU.P`: historical LSEG ticker often null while the DB retains a static ticker; older ISIN mismatch also appears.
- `STR.N`: older ticker / ISIN drift across corporate history.
- `BKR.O`, `DELL.N`: historical ISIN mismatch likely reflects corporate-action lineage, not an obviously wrong current record.

Confirmed price-history misses in the DB:

- `NBL.O^J20`
  - DB missing prices on `2021-02-26`, `2023-10-31`, `2026-02-27`
  - LSEG still returns `8.46`
- `VTRU.P`
  - DB missing price on `2026-02-27`
  - LSEG returns `9.07`
- `STR.N`
  - DB missing price on `2026-02-27`
  - LSEG returns `18.12`

These misses line up with the broader intermittent price-history anomaly seen in the local source archive.

## Materiality

High:

- Missing prices for live or recently live names can distort eligibility, row counts, and served loadings coverage.

Medium:

- Historical ticker / ISIN drift in legacy names is noisy but expected to some extent in a static security master. It matters mainly if those fields are used as hard join keys instead of informational attributes.

Low:

- Classification / exchange / country mismatches were not observed in this sample.

## Bottom Line

The LSEG spot checks do not show a broad failure of price, market-cap, exchange, or sector ingestion. The real issue is narrower:

- intermittent price-history holes for a subset of names
- lineage-heavy ticker / ISIN drift in older or legacy listings

That is consistent with the broader database findings rather than contradictory to them.
