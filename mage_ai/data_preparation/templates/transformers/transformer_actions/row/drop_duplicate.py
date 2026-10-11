{% extends "transformers/transformer_actions/action.jinja" %}
{% block action %}
    """
    Execute Transformer Action: ActionType.DROP_DUPLICATE

    Docs: https://github.com/jospablo777/mage-ml/blob/master/docs-md/guides/blocks/transformer-blocks.md#drop-duplicates
    """
    action = build_transformer_action(
        df,
        action_type=ActionType.DROP_DUPLICATE,
        arguments=df.columns,  # Specify column names to use when comparing duplicates
        axis=Axis.ROW,
        options={'keep': 'first'},  # Specify whether to keep 'first' or 'last' duplicate
    )
{% endblock %}
