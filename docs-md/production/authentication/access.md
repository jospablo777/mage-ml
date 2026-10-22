# Who may do what

*The lowest role each API action needs, from the policies themselves.*

With user authentication on, each request through the API is checked by its resource's
policy. This table comes from those policies, as recorded in `contracts/policy_matrix.json`,
and a test keeps it current. Each cell is the lowest role an action needs; the roles above it
may do it too:

- **Anyone**: no sign-in (the sign-in page reads these).
- **Signed in**: any signed-in user, one with no role too.
- **Viewer**, **Editor**, **Admin**, **Owner**: the roles of
  [Roles and permissions](overview.md#roles-and-permissions).
- **No one**: the action does not exist for that resource.

A `*` marks an action whose role changes with `DISABLE_NOTEBOOK_EDIT_ACCESS` (1 turns notebook
and pipeline edits off, 2 pipeline edits): editors usually lose it. Policies also check each
attribute read or written and each query parameter; an action allowed here can still refuse
a field. A pipeline's own permissions (roles on one pipeline) narrow these further.

| Resource | List | Detail | Create | Update | Delete |
| --- | --- | --- | --- | --- | --- |
| `autocomplete_items` | Viewer | No one | No one | No one | No one |
| `backfills` | Viewer | Viewer | Editor | Editor | Editor |
| `block_layout_items` | No one | Viewer | No one | No one | No one |
| `block_outputs` | No one | Viewer | No one | No one | No one |
| `block_runs` | Viewer | No one | No one | No one | No one |
| `block_templates` | Viewer | Viewer | No one | No one | No one |
| `blocks` | Viewer | Viewer | Editor * | Editor * | Editor * |
| `browser_items` | Viewer | Viewer | Editor * | Editor * | Editor * |
| `cache_items` | Viewer | Viewer | No one | Viewer | No one |
| `client_pages` | Viewer | Viewer | Viewer | Viewer | No one |
| `clusters` | No one | Viewer | No one | Editor | No one |
| `code_executions` | No one | No one | Editor * | No one | No one |
| `column_atlas_queries` | No one | No one | Viewer | No one | No one |
| `command_center_items` | No one | No one | Viewer | No one | No one |
| `compute_clusters` | Viewer | Viewer | Editor | Editor | Editor |
| `compute_connections` | Viewer | No one | No one | Editor | No one |
| `compute_services` | Viewer | Viewer | No one | No one | No one |
| `configuration_options` | Viewer | No one | No one | No one | No one |
| `custom_designs` | Anyone, Viewer, Editor, Admin, Owner | Anyone, Viewer, Editor, Admin, Owner | No one | No one | No one |
| `custom_templates` | Viewer | Viewer | Editor | Editor | Editor |
| `data_contracts` | Viewer | Viewer | Editor | No one | No one |
| `data_providers` | Viewer | No one | No one | No one | No one |
| `downloads` | No one | No one | Viewer | No one | No one |
| `event_rules` | No one | Viewer | No one | No one | No one |
| `execution_frameworks` | Viewer | Viewer | No one | No one | No one |
| `execution_states` | Viewer | No one | No one | No one | No one |
| `extension_options` | Viewer | Viewer | No one | No one | No one |
| `file_contents` | No one | Viewer | No one | Editor * | No one |
| `file_versions` | Viewer | No one | No one | No one | No one |
| `files` | Viewer | No one | Editor * | Editor * | Editor * |
| `folders` | No one | No one | Editor * | Editor * | Editor * |
| `fusion_verifications` | No one | Viewer | Editor | No one | Editor |
| `git_branches` | Viewer | Viewer | Editor | Editor | No one |
| `git_custom_branches` | Viewer | Viewer | Editor | Editor | No one |
| `git_files` | No one | Viewer | No one | No one | No one |
| `global_data_products` | Viewer | Viewer | Editor | Editor | Editor |
| `global_hooks` | Admin | Admin | Admin | Admin | Admin |
| `integration_destinations` | Viewer | No one | Viewer | No one | No one |
| `integration_source_streams` | No one | No one | No one | Viewer | No one |
| `integration_sources` | Viewer | Viewer | Editor | Editor | No one |
| `interactions` | Viewer | Viewer | Editor | Editor | Editor |
| `kernel_processes` | Viewer | Viewer | No one | No one | Editor * |
| `kernels` | Viewer | No one | No one | Editor * | No one |
| `language_setups` | No one | Editor | No one | No one | No one |
| `logs` | Viewer | No one | No one | No one | No one |
| `model_releases` | No one | Viewer | Editor | No one | No one |
| `monitor_reports` | No one | Viewer | No one | No one | No one |
| `monitor_stats` | No one | Viewer | No one | No one | No one |
| `monitors` | Viewer | Viewer | No one | Editor | No one |
| `oauth_access_tokens` | Viewer | No one | No one | No one | No one |
| `oauths` | Anyone, Viewer, Editor, Admin, Owner | Anyone, Viewer, Editor, Admin, Owner | Viewer | Viewer | No one |
| `outputs` | Viewer | Viewer | Viewer | No one | No one |
| `page_block_layouts` | No one | Viewer | No one | Editor | No one |
| `permissions` | Viewer | Viewer | Admin | Admin | Admin |
| `pipeline_compilations` | No one | Viewer | Editor | No one | No one |
| `pipeline_documentations` | No one | Viewer | No one | Editor | No one |
| `pipeline_execution_frameworks` | Viewer | Viewer | Editor * | Editor * | Editor * |
| `pipeline_interactions` | No one | Viewer | No one | Editor | No one |
| `pipeline_runs` | Viewer | Viewer | Editor | Editor | Editor |
| `pipeline_schedules` | Viewer | Viewer | Editor | Editor | Editor |
| `pipeline_service_builds` | Editor | Editor | Editor | No one | No one |
| `pipeline_service_exports` | Editor | Editor | Editor | No one | No one |
| `pipeline_services` | Viewer | Viewer | Editor | Editor | Editor |
| `pipeline_triggers` | Viewer | No one | Editor | No one | No one |
| `pipelines` | Viewer | Viewer | Editor * | Editor | Editor * |
| `projects` | Anyone, Viewer, Editor, Admin, Owner | No one | Editor | Viewer | No one |
| `pull_requests` | Viewer | Viewer | Editor | No one | No one |
| `python_workers` | Viewer | Viewer | Editor | No one | No one |
| `role_permissions` | No one | No one | Admin | No one | Admin |
| `roles` | Viewer | Viewer | Admin | Admin | Admin |
| `run_records` | No one | Viewer | Editor | No one | Editor |
| `rust_checks` | No one | No one | Editor | No one | No one |
| `schedulers` | Viewer | No one | Editor | No one | No one |
| `search_results` | No one | No one | Viewer | No one | No one |
| `secrets` | Viewer | No one | Editor * | No one | Editor * |
| `seeds` | No one | No one | Owner | No one | No one |
| `sessions` | No one | No one | Anyone | Viewer | No one |
| `spark_applications` | Viewer | No one | No one | No one | No one |
| `spark_environments` | No one | Viewer | No one | No one | No one |
| `spark_executors` | Viewer | No one | No one | No one | No one |
| `spark_jobs` | Viewer | Viewer | No one | No one | No one |
| `spark_sqls` | Viewer | Viewer | No one | No one | No one |
| `spark_stage_attempt_task_summaries` | No one | Viewer | No one | No one | No one |
| `spark_stage_attempt_tasks` | Viewer | No one | No one | No one | No one |
| `spark_stage_attempts` | Viewer | Viewer | No one | No one | No one |
| `spark_stages` | Viewer | Viewer | No one | No one | No one |
| `spark_threads` | Viewer | No one | No one | No one | No one |
| `statuses` | Anyone | No one | No one | No one | No one |
| `stream_dead_letters` | No one | Editor | Editor | No one | No one |
| `stream_runs` | No one | Editor | Editor | Editor | Editor |
| `stream_sample_runs` | No one | No one | Editor | No one | No one |
| `stream_samples` | Editor | Editor | Editor | Editor | Editor |
| `syncs` | Viewer | Viewer | Editor | Editor | No one |
| `tags` | Viewer | No one | Editor | No one | No one |
| `user_roles` | No one | No one | Admin | No one | Admin |
| `users` | Admin | Admin | Owner | Admin | Owner |
| `variables` | Viewer | No one | Editor * | Editor * | Editor * |
| `version_control_branches` | Editor | Editor | Editor | Editor | Editor |
| `version_control_files` | Editor | Editor | Editor | Editor | Editor |
| `version_control_projects` | Editor | Editor | Editor | Editor | Editor |
| `version_control_remotes` | Editor | Editor | Editor | Editor | Editor |
| `widgets` | Viewer | No one | Editor * | Editor * | Editor * |
| `workspaces` | Viewer | Viewer | Owner | Owner | Owner |
