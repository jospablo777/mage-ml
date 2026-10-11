# Configure Git

*This page will walk you through the process of authenticating with SSH or HTTPS and configuring Git for use with MageML Git sync.*

If you're wondering which Git resource to use in MageML, see [this](../../development/git/setting-up-git.md) page for a high-level overview of Git functionality. The [Version control](../data-sync/version-control.md) app is configured differently: it uses an OAuth app that you register with your Git provider.

If you've decided on a method that uses HTTPS or SSH authentication (Git Sync or Git Integration), this page will walk you through configuring one of those methods.

<div align="center">
<img src="https://user-images.githubusercontent.com/78053898/198753409-c7c41a8d-c137-4273-bb3b-76ecc81f8ee2.png" />
</div>

## Configure Git settings

> [!NOTE]
> When you use the Git integration, MageML sets your local repo as a safe directory (i.e. `git config --global --add safe.directory <local repo path>`)

To use the MageML Git integration, you will need to first configure Git settings. The following instructions are tailored to Github, but the Git integration can work with other Git providers as well, such as GitLab, BitBucket, or Azure DevOps:

<details><summary>SSH</summary>

> [!NOTE]
> **SSH keys with passphrase are not supported**
>
> The Git integration currently does not support repositories that require an SSH key with a passphrase.
>
> You can, however, use the MageML terminal and the git CLI to perform your git actions.
> You'll still need to generate your SSH keys as normal. You can start a ssh-agent in the terminal so that
> you don't have to enter your SSH key passphrase every time you perform an action.
>
> `eval $(ssh-agent -s) && ssh-add ~/.ssh/id_rsa`

**1. Check .gitignore settings**

Ensure that `.preferences.yaml` is added to your repo's `.gitignore` file so that your Git settings aren't pushed to Github.

**2. Navigate to Git settings**

Click _Settings_ in the left navigation, then select _Git settings_ on the left side of the settings page.

**3. Select SSH as the authentication type.**

Change the authentication type to SSH. The interface will change for you to input a remote repo url.

<img src="https://mage-ai.github.io/assets/git-settings.png" />

**4. Set up your SSH key**

If you haven't already, configure your SSH key. This will allow your instance to access GitHub. Our [instructions](#generate-ssh-token) can help if you're unfamiliar with the process.

**5. Add Git details**

- `Remote repo url`: Your Github repository's remote url. Use the SSH link instead of HTTPS link.
- `Local directory path`: The directory in your machine that will be synced with the Github repository. If this field is omitted, it will default to the path determined by Python's `os.getcwd()`. You can check what your current working directory is in a Scratchpad block.

**6. [Optional] Enable One-way Git Sync**

Open **Settings** > **Git settings** and select the **One-way Git Sync** tab. Set the
**Branch name** to sync from, and choose whether to **Sync on server start up** and
**Sync before each trigger run**. A sync replaces local changes with the branch's code; see
[One-way code sync using Git](../data-sync/git-sync.md).

**7. Fill out the remaining fields.**

Filling out these fields will allow MageML to recreate your git and SSH settings if they are deleted (i.e. if your container restarts):

> [!NOTE]
> If you are using a AWS CodeCommit repository, you will need to set the username to the SSH Key ID of the SSH public key added to your AWS IAM user.

    - `Username`: Your git username. If you push to your remote repo, this username will show up next to your commits.
    - `Email`: Your git email. Used for the same purpose as your username.
    - `SSH public key in base64`, `SSH private key in base64`: Enter your SSH public and private key encoded in base64. You can follow the instructions in the field description if you want MageML to save your SSH keys.

**8. Save your Git settings**

Click "Save repository settings" to complete the setup.

</details>

<details><summary>HTTPS/Token</summary>

**1. Check .gitignore settings**

Ensure that `.preferences.yaml` is added to your repo's `.gitignore` file so that your Git settings aren't pushed to Github.

**2. Navigate to Git settings**

Click _Settings_ in the left navigation, then select _Git settings_ on the left side of the settings page.

**3. Create a Git access token and save it for later**

- [GitHub](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/creating-a-personal-access-token)
- [GitLab](https://docs.gitlab.com/ee/user/profile/personal_access_tokens.html#create-a-personal-access-token)

**4. Add Git details**

- `Remote repo url`: Your Github repository's remote url. Use the HTTPS link.
- `Local directory path`: The directory in your machine that will be synced with the Github repository. If this field is omitted, it will default to the path determined by Python's `os.getcwd()`. You can check what your current working directory is in a Scratchpad block.

**5. [Optional] Enable One-way Git Sync**

Open **Settings** > **Git settings** and select the **One-way Git Sync** tab. Set the
**Branch name** to sync from, and choose whether to **Sync on server start up** and
**Sync before each trigger run**. A sync replaces local changes with the branch's code; see
[One-way code sync using Git](../data-sync/git-sync.md).

**6. Fill out the remaining fields.**

- `Username`: Your git username. If you push to your remote repo, this username will show up next to your commits.
- `Email`: Your git email. Used for the same purpose as your username.
- `Access token`: Enter the Git access token that you created in the step above.

**7. Save your Git settings**

Click "Save repository settings" to complete the setup.

</details>

### Generate SSH Token

If you're using a method that requires an SSH token in MageML, this section will walk you through generating one. If you're using a method that requires an HTTPS token, you can skip this section.

<details><summary>SSH</summary>

**1. Check for existing SSH key**

Follow GitHub's guide [here](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/checking-for-existing-ssh-keys) to check for an existing SSH key.

**2. Generate a new SSH key**

If no key exists, generate a new key using this [guide](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/generating-a-new-ssh-key-and-adding-it-to-the-ssh-agent).

You may skip this step if you already have a key.

**3. Add SSH key to your Git provider**

This step allows your Git provider to recognize your SSH key and communicate.

- [GitHub Docs](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/adding-a-new-ssh-key-to-your-github-account)
- [GitLab Docs](https://docs.gitlab.com/ee/user/ssh.html#add-an-ssh-key-to-your-gitlab-account)

**4. Test your SSH connection**

- [GitHub Docs](https://docs.github.com/en/authentication/connecting-to-github-with-ssh/testing-your-ssh-connection)
- [GitLab Docs](https://docs.gitlab.com/ee/user/ssh.html#verify-that-you-can-connect)

**5. Add SSH to known hosts**

The first time you connect using your Git provider, you'll get a message about verifying authenticity and adding the provider to your known hosts.
For example:

```txt
The authenticity of host 'gitlab.example.com (35.231.145.151)' can't be established.
ECDSA key fingerprint is SHA256:HbW3g8zUjNSksFbqTiUWPWg2Bq1x8xdGUrliXFzSnUw.
Are you sure you want to continue connecting (yes/no)? yes
Warning: Permanently added 'gitlab.example.com' (ECDSA) to the list of known hosts.
```

This is normal and expected. Type `yes` and press enter to continue.

</details>
