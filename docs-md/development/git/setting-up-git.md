# Get started with Git

*The ways to use Git in MageML for version control, collaboration, and deployment.*

<div align="center">
<img src="https://mage-ai.github.io/assets/getting-started/adventure-time.gif" width="75%" />
</div>

## Choose a method

There are three ways to use Git in MageML and _four_ ways to version control your files. The methods for using Git are mutually exclusive.

MageML keeps versions of your files without any setup: you can restore a previous version of a file even if you haven't configured a version control system. See the [file browser docs](../../guides/developer-ux/file-browser.md).

## Methods of using Git in MageML

| Recommended | Use                                                 | Recommendation          | Ease of use | Functionality          |
| ----------- | --------------------------------------------------- | ----------------------- | ----------- | ---------------------- |
| ✅          | Develop/deploy MageML using GitHub | [Git application](../../guides/data-sync/version-control.md)     | Moderate    | Full w/ token, SSH key or your own OAuth app |
| ✅          | Deploy MageML using Git with any provider   | [Integration w/ Git Sync](../../guides/data-sync/git-sync.md) | Advanced    | Full w/ SSH/HTTPS Auth |
| ⚠️          | Develop MageML using Git with any provider  | [Integration _w/o_ Git Sync](../../guides/data-sync/git-integration.md)  | Advanced    | Full w/ SSH/HTTPS Auth |

Git Sync is particularly useful in production instances, where other Git methods can be used more meaningfully in development instances. Since Git sync is a one-way sync, it's not very useful for collaboration, since changes get overridden. However, this makes it a good fit for a production MageML instance, where verified changes should only be accepted from development environments.

A common pattern is to use development instances to edit and push (Git integration), then have production instances use Git Sync or CI/CD.
