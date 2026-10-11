# dbt connection profiles

[Video](https://www.youtube.com/embed/rb_E2Nfm8Ak)

MageML looks for a file named `profiles.yml` located in your dbt project root
folder.

For example, if your project is named `demo_project` and your dbt project
name is `demo`, then your `profiles.yml` file for that dbt project should be
located in `demo_project/dbt/demo/profiles.yml`.

If you have a 2nd dbt project (e.g. named `example`), the connection profiles
for that project should be located in `demo_project/dbt/example/profiles.yml`.

For more information on how to configure your connection profiles, read
[dbt’s documentation](https://docs.getdbt.com/docs/get-started/connection-profiles).

You may also [interpolate variables](variable-interpolation.md) in your connection profiles.

---

## How connection profiles are used

When you run a [single dbt model](run-single-model.md) or
[selected dbt models](run-selected-model.md), the block has a field labeled
**Target**: the dbt profile target. Its value is the connection profile target that
MageML uses when running the dbt block.

MageML passes that value to dbt with the `--target` flag. In a pipeline run, a dbt
model block runs `dbt build` (`dbt run` when its tests are disabled).
