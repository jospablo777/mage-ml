{% extends "transformers/transformer_actions/action.jinja" %}
{% block action %}
    """
    Execute Transformer Action: ActionType.REMOVE

    Docs: https://github.com/jospablo777/mage-ml/blob/master/docs-md/guides/blocks/transformer-blocks.md#remove-columns
    """
    action = build_transformer_action(
        df,
        action_type=ActionType.REMOVE,
        arguments=[],  # Specify columns to remove
        axis=Axis.COLUMN,
    )
{% endblock %}
