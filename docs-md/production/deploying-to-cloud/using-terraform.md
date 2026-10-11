# Terraform

![](https://user-images.githubusercontent.com/78053898/198754720-d6b1e5bd-caa8-431e-a4cb-e26743c0ef36.png)

Deploy MageML to your cloud provider using Terraform.

Cloud providers supported by the Terraform scripts:

- AWS
- Azure
- GCP
- DigitalOcean

---

## Pre-install

Clone upstream Mage’s
[Terraform template repository](https://github.com/mage-ai/mage-ai-terraform-templates):

```bash
git clone https://github.com/mage-ai/mage-ai-terraform-templates.git
```

> [!NOTE]
> These templates deploy the upstream `mageai/mageai` image by default. MageML has no published
> image. Build one from the MageML repository's Dockerfile (`docker build -t mageml .`), push it
> to your cloud's container registry, and set the template's Docker image variable to it. Each
> cloud page below names the variable.

---

## Install Terraform CLI

```bash
brew tap hashicorp/tap && brew install hashicorp/tap/terraform
```

```bash
docker pull hashicorp/terraform:latest
```

### Windows

Follow [Microsoft’s instructions](https://learn.microsoft.com/en-us/azure/developer/terraform/get-started-windows-bash?tabs=bash#4-install-terraform-for-windows) to install Terraform on Windows.

### Errors

If you’re installing Terraform using Homebrew on an Apple M1 laptop, you may get this error:

```
Error: Incompatible provider version

Provider registry.terraform.io/hashicorp/template v2.2.0 does not have a package available for your current platform, darwin_arm64.

Provider releases are separate from Terraform CLI releases, so not all providers are available for all platforms. 
Other versions of this provider may have different platforms supported.
```

Follow this [guide](https://medium.com/@sssanjaya/terraform-error-incompatible-provider-version-m1-or-darwin-arm64-fae0159f913f) to fix the issue.

## Cloud specific Terraform instructions

Read the following documentation for the cloud provider of your choice:

- [Amazon Web Services (AWS)](aws/setup.md)
- [Microsoft Azure](azure/setup.md)
- [Google Cloud Platform (GCP)](gcp/setup.md)
- [DigitalOcean](digitalocean/setup.md)

---
