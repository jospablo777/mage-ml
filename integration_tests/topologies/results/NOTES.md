# Notes on the results

One JSON line per run, in `<experiment>-<session>.jsonl`, with the session's metadata in
`<session>.meta.json` (provenance.py). `incidents.json` lists conditions that changed
during a session; `scripts/study/analyze.py` applies it.

- Session 20261012T094950Z-337718 (ml_scoring): the R environment check changed at
  10:15:49 UTC, during the session (see incidents.json). Its R runs before that time are
  the "before" measurement of the check; R scorers were run again after the change.
- Session 20261012T085115Z-32468c (faults) predates the fix of the status of a compiled
  chain's last block (1210d19d3); session 20261012T094438Z-da12bd ran after it. The
  analysis reports the latest session.
- Session 20261012T142209Z-9e82df (journal) stopped after its first runs: switching the
  journal mode found the database locked by the test's own open session (fixed in the
  test). The analysis reports the latest journal session.
