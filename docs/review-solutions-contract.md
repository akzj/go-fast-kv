# Expert Review Contract: Solution Scoring

## Scoring Dimensions (0-100 each, weighted)

| Dimension | Weight | Description |
|-----------|--------|-------------|
| Completeness | 25% | All 11 design docs integrated; no missing sections |
| Consistency | 25% | Matches finalized decisions (VAddr=16B, Inline≤48B, B-link-tree, WAL+checkpoint) |
| Implementability | 25% | Clear enough for implementation; no ambiguity |
| Risk | 25% | Low risk of failure; edge cases addressed |

## Finalized Decisions (Ground Truth)

- **VAddr**: 16 bytes (SegmentID[8] + Offset[8])
- **Inline threshold**: ≤48 bytes
- **Storage**: Append-only
- **Index**: B-link-tree (4KB pages)
- **Recovery**: WAL + checkpoint
- **Concurrency**: Single-writer, multi-reader
- **Page Cache**: OS Page Cache (not custom) — rated 64/100

## Required Sections Check

Every solution MUST cover:
1. VAddr format
2. B-link-tree structure
3. Page Manager
4. External Value Store
5. API Layer
6. Concurrency
7. Recovery (WAL + checkpoint)
8. Compaction
9. Cache strategy

## Output Format

```markdown
## Solution-A Score: XX/100

### Completeness (XX/25)
### Consistency (XX/25) 
### Implementability (XX/25)
### Risk (XX/25)

**Rationale**: ...

---

## Solution-B Score: XX/100
...

---

## Solution-C Score: XX/100
...

---

## Recommendation

[Best solution and reasoning]
```

## Known Traps to Flag

| Trap | Indicator | Penalty |
|------|-----------|---------|
| Wrong VAddr size | 8 or 12 bytes instead of 16 | -20 points |
| Wrong inline threshold | ≠48 bytes | -10 points |
| Custom cache vs OS Page Cache | Dense Array custom buffer | -15 points if inconsistent |
| Missing WAL | No WAL section | -20 points |
| Missing B-link-tree details | No node format discussion | -15 points |
