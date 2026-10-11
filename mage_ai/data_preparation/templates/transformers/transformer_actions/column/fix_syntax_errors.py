{% extends "transformers/transformer_actions/action.jinja" %}
{% block action %}
    """
    Execute Transformer Action: ActionType.FIX_SYNTAX_ERRORS

    This marks any improperly formatted values in each column specified
    as invalid.

    Docs: https://github.com/jospablo777/mage-ml/blob/master/docs-md/guides/blocks/transformer-blocks.md#fix-syntax-errors
    """
    action = build_transformer_action(
        df,
        action_type=ActionType.FIX_SYNTAX_ERRORS,
        arguments=df.columns,  # Specify columns to fix syntax errors for.
        axis=Axis.COLUMN,
    )
{% endblock %}
