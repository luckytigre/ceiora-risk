# Glossary

One noun per concept. The nouns are part of the product: a user who sees the same
object called four things assumes they are four things.

Referenced in review. When adding a surface, use these terms in UI copy, `aria-label`s,
error messages and empty states.

## Objects

| Term | Means | Do not use |
| --- | --- | --- |
| **position** | One holding of one instrument in one account. The unit a user sees in a table row. | holding, row, leg, line |
| **thesis** | A user-defined grouping of positions. Does not change the underlying holdings. | bucket, group, basket, section |
| **allocation** | The signed share quantity of a position assigned to a thesis. A position may be split across several. | assignment, split, share |
| **account** | The book a position sits in. | portfolio, book |
| **factor** | A risk factor in the active model. | driver, exposure (that is the *value*, not the thing) |
| **exposure** | A position's or portfolio's loading on a factor. | sensitivity (that is a distinct measure), beta |

Notes on the two that are easy to get wrong:

- **position vs allocation.** A position is what is held. An allocation is how much
  of it a thesis claims. Deleting a thesis removes allocations, never positions —
  say so in the copy, because users reasonably fear otherwise.
- **exposure vs sensitivity.** Both appear in the factor tables and they are not
  synonyms. Do not use one to avoid repeating the other.

## Copy conventions

- Ellipsis is `…` (U+2026), never `...`. 28 vs 25 uses coexisted, sometimes in
  adjacent files.
- Minus is `−` (U+2212) in rendered values, not a hyphen. `signedQuantity()` in
  InlineThesisRows.tsx is the reference implementation.
- Empty states name the thing and the reason: "No positions are loaded." Avoid
  double negatives — "No unintended single-factor dominance unless by design"
  takes three reads.
- A control says what happens: "Delete thesis", not "OK". A confirmation dialog's
  destructive button repeats the verb, so the user can act on the button alone.
