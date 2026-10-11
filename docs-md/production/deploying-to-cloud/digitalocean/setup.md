# Deploy to DigitalOcean with Terraform

## Pre-requisites

### Setup

[Download the Terraform scripts maintained by upstream Mage](../using-terraform.md).
The DigitalOcean template provisions a Droplet; you start MageML on it with Docker in
[step 5](#5-run-app-in-the-provisioned-droplet).

## Terraform plan

You can run the following command to see all the resources that will be created
by Terraform:

```bash
cd digitalocean
terraform plan
```

---

## 1. Configure access token

Generate DigitalOcean access token by following the guide: https://developer.hashicorp.com/terraform/tutorials/applications/digitalocean-provider#generate-digitalocean-access-token

Set the token in `DIGITALOCEAN_ACCESS_TOKEN` environment variable

---

## 2. Create ssh key

Create ssh key with command: `ssh-keygen`. In the file
[./digitalocean/variables.tf](https://github.com/mage-ai/mage-ai-terraform-templates/blob/master/digitalocean/variables.tf), update the value of `ssh_pub_key` to your path of public key.

---

## 3. Customize Terraform settings

**Application name (optional)**

In the file
[./digitalocean/variables.tf](https://github.com/mage-ai/mage-ai-terraform-templates/blob/master/digitalocean/variables.tf),
you can change the default application name that will appear in DigitalOcean:

```
variable "app_name" {
  type        = string
  description = "Application Name"
  default     = "mage-data-prep"
}
```

**Region (optional)**

In the file
[./digitalocean/variables.tf](https://github.com/mage-ai/mage-ai-terraform-templates/blob/master/digitalocean/variables.tf),
you can change the region:

```
variable "region" {
  type        = string
  description = "Region of the datacenters"
  default     = "sfo3"
}
```

**More**

Other variables defined in
[./digitalocean/variables.tf](https://github.com/mage-ai/mage-ai-terraform-templates/blob/master/digitalocean/variables.tf)
can also be customized to your needs.

---

## 4. Provision resources

**Using CLI**

1. Change directory into scripts folder:
```bash
   cd digitalocean
   ```
2. Initialize Terraform:
```bash
   terraform init
   ```
3. Deploy:
```bash
   terraform apply
   ```

Once it’s finished deploying, you can get the IP from the output.

## 5. Run app in the provisioned Droplet

SSH into the instance: `ssh -i [path_to_private_key] root@[ip]`

MageML has no published image (`mageai/mageai` is upstream Mage). Build the MageML image
on the Droplet, or pull one you pushed to your own registry:
```bash
git clone https://github.com/jospablo777/mage-ml.git
docker build -t mageml mage-ml
```

Run command to start the MageML app:
```bash
docker run -d -p 6789:6789 -v $(pwd):/home/src mageml /app/run_app.sh mage start default_repo
```

<br />

To update MageML, pull the latest code (`git -C mage-ml pull`), rebuild the image with the
same `docker build` command, then restart the MageML app with the same `docker run` command
above.

## 6. Open browser with MageML

Once you’ve launched the Droplet and started MageML, you can access MageML from your web browser by entering the following:

`http://[ip]:[port]`

Change the IP to match the IP of your Droplet and the port that you’re using for MageML.

---

## Misc

### Terminate all resources

If you want to delete all resources created from deploying, run the following
command:

```bash
terraform destroy
```

A sample output could look like this:

```
Destroy complete! Resources: 10 destroyed.
```
