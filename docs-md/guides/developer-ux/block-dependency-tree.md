# Block dependency tree

*Edit block dependencies in the pipeline dependency tree.*

## Drag-and-drop blocks to update dependencies

<p align="center">
</p>

<img src="https://mage-ai.github.io/assets/dev-ux/dependency-tree/drag-block.gif" alt="Drag-and-drop blocks." />

Click a block in the tree, hold down the mouse, and drag it to another block and release.

This will set the dragged block as a downstream block from the one that you dropped it on.

### Drag an entire group of blocks

<p align="center">
</p>

<img src="https://mage-ai.github.io/assets/dev-ux/dependency-tree/drag-group.gif" alt="Drag an entire group of blocks." />

Blocks in the tree are visually grouped if every block in the group has the same set of
upstream blocks.

When a group is visually formed, you can drag-and-drop the entire group onto another block
and the entire group will be set as the downstream for the block you dropped it on.

---

## Right click menu

<p align="center">
</p>

<img src="https://mage-ai.github.io/assets/dev-ux/dependency-tree/right-click.png" alt="Right click menu." />

Right click on any block to open a menu with these commands:

1. Run block
1. Rename block
1. Show all dependencies
    ![Show all dependencies](https://mage-ai.github.io/assets/dev-ux/dependency-tree/show-all-dependencies.png)
1. Add upstream block
1. Add downstream block
1. Remove upstream dependencies
1. Remove downstream dependencies
1. Add/Edit interactions
1. Delete block
1. Delete block (ignore dependencies)
1. View file versions

---

## Edit the connection between blocks

<p align="center">
</p>

<img src="https://mage-ai.github.io/assets/dev-ux/dependency-tree/edit-connection.png" alt="Edit the connection between blocks." />

Click on the line between any block and a menu will appear with these options:

1. Add new block between
1. Remove connection

---

## Drag lines to connect blocks

<p align="center">
</p>

<img src="https://mage-ai.github.io/assets/dev-ux/dependency-tree/drag-orbs.gif" alt="Drag lines to connect blocks." />

Hover your mouse over a block to reveal 2 orbs (aka circles, dots, etc.).
Click and hold down on the top orb and drag it to another block then release.
This will set the 1st block as the downstream dependency of the 2nd block.

If you click on the bottom orb, then the 1st block will be set as the upstream dependency of the 2nd block.

---
