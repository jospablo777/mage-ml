UPDATE {{ env_var('MAGE_TEST_SCHEMA') }}.churn_scores AS target
SET score = incoming.score, model_version = incoming.model_version, scored_at = NOW()
FROM {{ df_1 }} AS incoming
WHERE target.driver_id = incoming.driver_id AND target.score IS NULL
