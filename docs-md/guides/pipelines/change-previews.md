# Change previews

*See what a change to a pipeline does to its data before it ships.*

A change preview runs the pipeline's current code on the inputs of its last completed run
and compares the outputs with that run's: rows added, removed and changed, per block and
per column, before the change ships. Nothing reaches a destination.

Open a pipeline's **Change previews** page and choose **Preview the current code**, or run:

```bash
mage preview my_project orders_daily --key score_orders=order_id
```

```text
Preview of orders_daily against run 41: done
  load_orders     unchanged
  score_orders    previewed: its code changed (transformers/score_orders.py)
                    output_0: 0 added, 2 removed, 7 changed (score 7), 1 unchanged, by order_id
                      rows: 10 before, 8 now
  export_scores   not run: it reads score_orders, which changed; it writes to a destination; previews never run exporters
```

## What runs

| Block | In a preview |
| --- | --- |
| Its code or settings changed since the run | Runs again on the run's stored inputs |
| After a changed block | Runs again, on the changed block's new output |
| Neither | Not run; its stored output is the input of the blocks after it |
| Exporter, sensor | Never runs: it writes to or waits on the outside |
| SQL block | Never runs: it runs in the warehouse |
| Dynamic block | Never runs |
| After a changed block that did not run | Not run, as its input would be the old one; when its own code changed it runs, on the run's stored output of that block, and says so |

- The run to compare with is the pipeline's last completed run (`--run` names another).
  Its record says what code it ran (see [run records](run-records.md)); a
  changed shared file (project code or settings) makes every block run again.
- The blocks run in a partition of the preview's own; the run's outputs are linked, not
  copied, and stay as they were. A loader whose code changed reads its source now, not as
  the run did.
- A failing block shows its error line; the blocks after it do not run.

## How outputs compare

Stored DataFrames compare in Rust (Polars), about 100 ms for a million rows:

- **Schema:** columns added, removed, and columns whose type changed.
- **Rows by key:** added, removed, changed and unchanged, how many rows changed in each
  column, and sample keys of each. The key is the block's `preview_key` (a column or a
  list, in the block's configuration), `--key block=column,column`, or a column `id` in
  both outputs. A key that is not unique says so.
- **Without a key:** the rows only in one of the two, compared as sets of rows.
- Floats within a relative 1e-9 count as equal. Other outputs (numbers, lists, objects)
  compare as equal or different.

The last 10 previews of a pipeline are kept, with their outputs.
