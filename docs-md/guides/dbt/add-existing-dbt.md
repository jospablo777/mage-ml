# Add an existing dbt project to MageML

For example, if your project is named `demo_project` and your existing dbt
project is named `demo`, add your dbt project in the project under the
following path:

```
demo_project/dbt/demo/
```

You can have multiple dbt projects in a single project under the folder
named `dbt/` at the root location of your project.

For example:

```
demo_project/
|   dbt/
|   |   demo/
|   |   |   ...
|   |   project_2/
|   |   |   ...
```

---

## Required structure

### Connection profiles

For MageML to properly run dbt commands, you must have a `profiles.yml` file at
the root of each of your dbt projects. For example:

```
demo_project/
|   dbt/
|   |   demo/
|   |   |   profiles.yml
|   |   project_2/
|   |   |   profiles.yml
```

For more information on how to configure dbt connection profiles, read this
[documentation](connection-profiles.md).
