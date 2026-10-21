"""The Rust block templates as the editor's menus make them, for the database tests."""


def template(path: str) -> str:
    """A template's code, such as template('data_loaders/rust/mysql.rs')."""
    from mage_ai.data_preparation.templates.template import template_env

    return template_env.get_template(path).render(code='')
