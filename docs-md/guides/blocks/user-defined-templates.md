# User-defined templates

*Create and use custom block templates and pipeline templates in MageML.*

## Overview

User-defined templates are reusable code patterns for blocks and pipelines. They are stored in the project's `custom_templates` folder, so they are versioned with the project.

## Block Templates

### Creating block templates

Block templates are reusable code patterns that can be customized with variables. They help standardize common patterns across your organization.

**Steps to create a block template:**

1. **Navigate to Templates** - Click "Templates" in the left navigation menu and open the **Blocks** tab. You can also go directly to `/templates`.
2. **Create new template** - Click "New block template" button
3. **Define template properties**:
   - **Template UUID** - Enter a unique identifier (e.g., `sql_data_loader`)
   - **Block type** - Select the type (Data loader, Transformer, Data exporter, etc.)
   - **Language** - Choose the programming language (Python, SQL, R, Rust, YAML, etc.)
4. **Edit template content** - Write the template code
5. **Save template** - Click "Create new template". For an existing template, the button is "Save template".

> [!NOTE]
> The template UUID also sets where the template is stored in the project. MageML cleans it the same way as a block name: it is lowercased, characters other than letters, numbers and underscores become underscores, and a UUID that starts with a number gets a `letter_` prefix.

### Template File Structure

When you create a block template, MageML creates the following file structure:

```
custom_templates/
└── blocks/
    └── {template_uuid}/
        ├── metadata.yaml    # Template metadata and configuration
        └── {template_uuid}.py  # Template code file
```

**Template files:**
- **`metadata.yaml`** - Contains template metadata, block type, language, and other configuration
- **`{template_uuid}.py`** - The actual template code file that users will customize

The code file extension follows the selected language. For example, Python templates use `.py`, SQL templates use `.sql`, R templates use `.r`, Rust templates use `.rs`, and YAML templates use `.yaml`.

**Example structure for `test_template`:**
```
custom_templates/
└── blocks/
    └── test_template/
        ├── metadata.yaml
        └── test_template.py
```

### Template library

The Templates page (`/templates`) lists all block and pipeline templates in the project:

- **Blocks** and **Pipelines** tabs separate block templates from pipeline templates.
- Block templates can be filtered by block type.
- Pipeline templates can be filtered by pipeline type (standard, data integration, streaming).
- Click a template to view and edit it.

**Accessing templates:**
1. **From left navigation** - Click "Templates" to open the template library.
2. **From the pipeline editor** - Choose "Custom template" from a block type's menu when adding a block.
3. **From pipeline creation** - Select "From a template" when creating a new pipeline.

### Using block templates

**Steps to use a block template:**

1. **Open pipeline editor** - Navigate to the pipeline where you want to add the block
2. **Add new block** - Open the menu of the block type you want to add, for example `Data loader`
3. **Browse templates** - Select "Custom template". The template browser opens filtered to that block type.
4. **Choose a template** - Click a template to add a block with the template's code to the pipeline
5. **Customize as needed** - Modify the generated block code for your specific use case

When you add a callback or conditional block from an existing block's add-on panel,
MageML opens the same template browser scoped to the matching block type.

The `dbt` menu in the pipeline editor offers three dbt starters:

- **Single model or snapshot (from file)** opens the file browser so you can choose an existing dbt file.
- **All models (w/ optional exclusion)** creates a dbt block configured to run all models, with optional selection or exclusion rules.
- **Generic dbt command** creates an editable dbt command block.

## Pipeline Templates

### Creating pipeline templates

Pipeline templates define complete pipeline structures that can be customized for different use cases.

**Steps to create a pipeline template:**

1. **Open the pipelines page** - Go to `/pipelines`.
2. **Create new template from an existing pipeline** - Right-click the pipeline you want to reuse and select **Create template**. This opens `/templates?object_type=pipelines&new=1&pipeline_uuid={pipeline_uuid}`.
3. **Configure template settings** - Set the template UUID, name, description, and other properties.
4. **Save template** - The pipeline structure will be saved as a reusable template.

**Example pipeline template structure:**

```
custom_templates/pipelines/
└── example_pipeline_template/
    ├── metadata.yaml
    └── triggers.yaml
```

### Using pipeline templates

**Steps to use a pipeline template:**

1. **Navigate to the pipelines page** in your project
2. **Click "New"** button to open the pipeline creation menu
3. **Select "From a template"** from the dropdown options
4. **Find a template** - Filter by pipeline type if needed
5. **Choose your template** - Clicking a template creates a new pipeline from it
6. **Customize the pipeline** - Modify blocks, connections, and settings as needed

> [!NOTE]
> **Note**: Pipeline templates are created from existing pipelines using the right-click context menu. This allows you to reuse successful pipeline patterns across your projects.

## Best Practices

### Template design

- **Keep templates focused** - Each template should serve a specific purpose
- **Use clear variable names** - Make variable purposes obvious
- **Include comprehensive documentation** - Explain usage and customization options
- **Test with different configurations** - Ensure templates work with various inputs
- **Follow coding standards** - Maintain consistent style and practices

### Template maintenance

- **Regular updates** - Keep templates current with the project's code and dependencies
- **User feedback** - Collect and incorporate user suggestions
- **Performance optimization** - Monitor and improve template performance
- **Security review** - Ensure templates follow security best practices
