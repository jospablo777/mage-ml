# Git integration for code version control

*MageML's Git integration lets you push changes to a remote repository, for example to collaborate on a project with your teammates.*

"Git integration" in MageML is a way to collaborate with teammates using Git. It provides a way to push changes to a branch in an external repository. The [Version control](version-control.md) app covers the same actions and more.

First, configure Git: see this [guide](../git/configure.md). Once your git settings are configured, you can perform various Git actions in MageML. To start, click the button at the top right with the name of the current branch.

<img src="https://mage-ai.github.io/assets/version-control/git-integration/branch-button.png" />

Once the modal is open, you can start performing actions. Currently, the Git integration supports the following actions:

<details><summary>Clone repository</summary>

Clone repository will allow you to clone the specified git repository to your local machine.
_This operation will overwrite all local changes you have in the local git directory._

</details>

<details><summary>Create new branch</summary>

Create new branch will create a new branch in your local git directory. Specify the name of the branch and click `Create`. You will
automatically be switched to the new branch.

<img src="https://mage-ai.github.io/assets/version-control/git-integration/create-new-branch.png" width="100%" />

</details>

<details><summary>Switch branch</summary>

To switch your local branch, you can click the dropdown menu with the current active branch and select the branch you would like to switch to.

<img src="https://mage-ai.github.io/assets/version-control/git-integration/switch-branch.png" width="100%" />

</details>

<details><summary>Commit & push</summary>

**1. Select files**

First, select the files you would like to commit. You can select all files by clicking the `Include all changes` button.

<img src="https://mage-ai.github.io/assets/version-control/git-integration/select-files.png" width="100%" />

**2. Commit your changes**

Then, enter a descriptive commit message and click `Commit`. This will commit your changes to your local git directory.

<img src="https://mage-ai.github.io/assets/version-control/git-integration/commit-changes.png" width="100%" />

**3. Push to remote**

Once your changes are committed, you can push your commit to the remote repository by clicking `Push`.

<img src="https://mage-ai.github.io/assets/version-control/git-integration/push-to-remote.png" width="100%" />

</details>

<details><summary>Pull</summary>

Pull your changes from the remote branch to your local branch. This operation is equivalent to `git pull mage-repo <current-branch>`.

</details>

<details><summary>Hard reset</summary>

Hard reset your local branch with the remote branch. This operation is equivalent to `git reset --hard mage-repo/<current-branch>`.

_This operation will overwrite your local changes._

</details>

For any other operations you may need, you can perform them through the terminal. You can open the terminal by clicking the terminal toggle in the top right of the modal.
