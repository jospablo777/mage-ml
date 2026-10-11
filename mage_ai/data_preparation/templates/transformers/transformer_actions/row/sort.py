{% extends "transformers/transformer_actions/action.jinja" %}
{% block action %}
    """
    Execute Transformer Action: ActionType.SORT

    Docs: https://github.com/jospablo777/mage-ml/blob/master/docs-md/guides/blocks/transformer-blocks.md#sort
    """
    action = build_transformer_action(
        df,
        action_type=ActionType.SORT,
        arguments=[],  # Specify columns to sort rows by
        axis=Axis.ROW,
        options={'ascending': True},
    )
{% endblock %}
