{% extends "transformers/transformer_actions/action.jinja" %}
{% block imports %}
{{ super() -}}
{% endblock %}
{% block action %}
    """
    Execute Transformer Action: ActionType.STANDARDIZE

    Docs: https://github.com/jospablo777/mage-ml/blob/master/docs-md/guides/blocks/transformer-blocks.md#standardize-data
    """
    action = build_transformer_action(
        df,
        action_type=ActionType.STANDARDIZE,
        arguments=[],  # Specify columns to normalize
        axis=Axis.COLUMN, # Specify normalization strategy
    )
{% endblock %}
