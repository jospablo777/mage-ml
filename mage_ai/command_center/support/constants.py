from mage_ai.command_center.constants import ItemType, ObjectType

ITEMS = [
    dict(
        item_type=ItemType.SUPPORT,
        object_type=ObjectType.DOCUMENT,
        title='Read developer documentation',
        description='Technical documentation for Mage.',
        display_settings_by_attribute=dict(
            icon=dict(
                color_uuid='background.success',
            ),
        ),
        actions=[
            dict(
                page=dict(
                    path='https://github.com/jospablo777/mage-ml/blob/master/docs-md/README.md',
                    external=True,
                    open_new_window=True,
                ),
                uuid='docs',
            ),
        ],
    ),
]
