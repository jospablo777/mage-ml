{% extends "transformers/transformer_actions/action.jinja" %}
{% block action %}
    """
    Execute Transformer Action: ActionType.SELECT

    Docs: https://github.com/jospablo777/mage-ml/blob/master/docs-md/guides/blocks/transformer-blocks.md#select-columns
    """
    action = build_transformer_action(
        df,
        action_type=ActionType.SELECT,
        arguments=df.columns,  # Specify columns to select
        axis=Axis.COLUMN,
    )
{% endblock %}
