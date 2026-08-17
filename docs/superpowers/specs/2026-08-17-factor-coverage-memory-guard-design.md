# Factor Coverage Memory Guard

## Problem

Opening `/factor` automatically requests `full_range=true` coverage. When an index
pool or parameterized factor is selected, the backend cannot use the lightweight
manifest path and scans the entire `factor_values` Parquet lake. The current lake
contains roughly 269,926 files, and the de-duplication query can grow the backend
Python process to multiple gigabytes of memory.

## Design

The Factor Value Store page will request coverage only for its selected date range
during automatic refresh. The API will also protect callers that send
`full_range=true` with a resolved symbol universe or factor parameters by falling
back to the same bounded date-range query. Unbounded full-range coverage remains
available only when the request has no universe or parameter filters, allowing the
existing manifest-index fast path to serve it when available.

## Verification

- Add a backend regression test for the full-range fallback decision.
- Build the frontend.
- Restart the prod services and verify `/health` on ports 8800 and 8810 plus the
  frontend on port 3511.
- Re-open `/factor` and sample backend memory to confirm no continuing unbounded
  growth.
