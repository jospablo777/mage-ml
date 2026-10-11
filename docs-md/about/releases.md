# Releases

*MageML release notes and the upstream Mage changelog.*

**[Update MageML](../development/updating-mage.md)**

Install the newest version of MageML

**[MageML release notes](https://github.com/jospablo777/mage-ml/blob/master/docs/development/release-notes.md)**

Changes in each MageML release

---

## Upstream Mage releases

MageML is based on Mage 0.9.79. The entries below are upstream Mage releases up to the fork,
kept for history. All upstream releases are on
[Mage's GitHub releases page](https://github.com/mage-ai/mage-ai/releases).

<br />

### Kaos (0.9.74)

<a href="https://github.com/mage-ai/mage-ai/releases/tag/0.9.74">
  Read full release notes
</a>

### ☁️ Google cloud storage source

You can now configure your data integration source blocks to Google Cloud Storage using this snippet. It specifies the destination bucket, sets the file type to Parquet, and provides the path to your service account credentials needed for authentication.

<div align="center">
<img src="https://mage-ai.github.io/assets/pipelines/integration-pipelines/gcs_integration_test.png" width="600" />
</div>

by @TalaatHasanin in https://github.com/mage-ai/mage-ai/pull/5334

### 🌬️ 🍽️ Airtable Integration

Connect your data integration source blocks to Airtable with this script. It establishes a connection to your Airtable base, designates the specific table for integration, and securely integrates your Airtable API key for access.

<div align="center">
<img src="https://mage-ai.github.io/assets/pipelines/integration-pipelines/airtable_integration_test.png" width="600" />
</div>

by @TalaatHasanin in https://github.com/mage-ai/mage-ai/pull/5404

### ✂️ Add trim reformat action to transformer block
A new "trim reformat" action has been added to the Python transformer block, allowing for the removal of leading and trailing whitespace from specified text columns. This enhancement ensures cleaner and more consistent data formatting by automatically stripping unnecessary spaces around text entries.

by @cristopheridlc in https://github.com/mage-ai/mage-ai/pull/5321

### 🐻‍❄️ Enable polars dataframe in GCS data exporter

This update enhances the export method of the GCS IO module, expanding its functionality to support exporting Polars DataFrames in addition to the previously supported Pandas DataFrames. This new capability allows users to work with both data formats, offering greater flexibility in managing and exporting data from their workflows.

<div align="center">
<img src="https://media0.giphy.com/media/v1.Y2lkPTc5MGI3NjExbDM2amtld2toajMxMmR2cmt2eHc4MmdlZ2V6Y3IxcXdncmI1ZGN5YSZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/aK4wh0UE3oddS/giphy.webp?raw=true" width="50%" />
</div>

by @TalaatHasanin in https://github.com/mage-ai/mage-ai/pull/5348

### Add support to scheduler name on k8s executor

This update adds the ability to customize Kubernetes scheduler options when using the Kubernetes Executor. Users can now directly configure scheduler settings, providing increased flexibility and control over how pods are scheduled within Kubernetes environments.

by @messerzen in https://github.com/mage-ai/mage-ai/pull/5412

### Deadpool & Wolverine (0.9.73)

<a href="https://github.com/mage-ai/mage-ai/releases/tag/0.9.73">
  Read full release notes
</a>

### 🧠 Memory management upgrade

This feature enhances system performance and stability by optimizing allocation, deallocation, and overall utilization of RAM resources, resulting in improved efficiency and reduced memory-related errors.

Check out the [doc](https://www.notion.so/mageai/Memory-management-upgrades-e6c5e6e360ce410094091e00c46e3bb6?pvs=4) for details.

<br/>

### 🔄 Dynamic blocks 2.0

Introducing an enhanced Dynamic blocks feature for creating dynamic content blocks that can adapt based on user input or data changes, offering improved flexibility and interactivity for users. This update aims to streamline workflows and enhance the overall user experience by making content more responsive and customizable.

Learn more in this [doc](https://www.notion.so/mageai/Dynamic-Blocks-2-0-d2fc11a4f48148d68b817e9bfb38a732).

### 🔐 Azure DB connection via Key Vault

Securely retrieve a database connection URL stored in Azure Key Vault using environment variables for authentication.

```
AZURE_KEY_VAULT_URL
AZURE_CLIENT_ID
AZURE_CLIENT_SECRET
AZURE_TENANT_ID
```

by [@wangxiaoyou1993](https://github.com/wangxiaoyou1993) in [#5302](https://github.com/mage-ai/mage-ai/pull/5302)

### 👁️ Workspace monitoring

Add an "Overview" page and "Pipeline runs" page to the Workspace Management UI. This provides some overall monitoring for all of the pipeline runs in the different workspaces without having to individually open up each workspace instance.

<div align="center">
<img src="https://github.com/user-attachments/assets/0b8a1d83-3292-4e10-928b-5d625f817d40" width="600" />
</div>

<br/>

by @johnson-mage in https://github.com/mage-ai/mage-ai/pull/5311

### House Targaryen (0.9.72)

<a href="https://github.com/mage-ai/mage-ai/releases/tag/0.9.72">
  Read full release notes
</a>

[House Targaryen Release](https://www.youtube.com/embed/abiOckshzu8)

### X-Men (0.9.71)

<a href="https://github.com/mage-ai/mage-ai/releases/tag/0.9.71">
  Read full release notes
</a>

[X-Men Release](https://www.youtube.com/embed/m8iw0e8346k)

### Fallout (0.9.70)

<a href="https://github.com/mage-ai/mage-ai/releases/tag/0.9.70">
  Read full release notes
</a>

[Fallout Release](https://www.youtube.com/embed/rAp7psCTxC8)

### Invincible (0.9.68)

<a href="https://github.com/mage-ai/mage-ai/releases/tag/0.9.68">
  Read full release notes
</a>

[Invincible Release](https://www.youtube.com/embed/ck1UGjUeGGA)

- [0.9.66 | Shogun Release](https://github.com/mage-ai/mage-ai/releases/tag/0.9.66)
- [0.9.65 | Demon Slayer Release](https://github.com/mage-ai/mage-ai/releases/tag/0.9.65)
- [0.9.64 | Maestro Release](https://github.com/mage-ai/mage-ai/releases/tag/0.9.64)
- [0.9.63 | Halo Release](https://github.com/mage-ai/mage-ai/releases/tag/0.9.63)
- [0.9.62 | The Beekeeper Release](https://github.com/mage-ai/mage-ai/releases/tag/0.9.62)
- [0.9.60 | Yusuke Urameshi Release](https://github.com/mage-ai/mage-ai/releases/tag/0.9.60)
- [0.9.59 | Year of the Dragon Release](https://github.com/mage-ai/mage-ai/releases/tag/0.9.59)
- [0.9.50 | Wonka Release](https://github.com/mage-ai/mage-ai/releases/tag/0.9.50)
- [0.9.48 | The Boy and the Heron Release](https://github.com/mage-ai/mage-ai/releases/tag/0.9.48)
- [0.9.46 | Wish Release](https://github.com/mage-ai/mage-ai/releases/tag/0.9.46)
- [0.9.45 | Yuji Itadori Release](https://github.com/mage-ai/mage-ai/releases/tag/0.9.45)
- [0.9.43 | Attack on Titan Release](https://github.com/mage-ai/mage-ai/releases/tag/0.9.43)
- [0.9.41 | Halloween Release](https://github.com/mage-ai/mage-ai/releases/tag/0.9.41)
- [0.9.38 | Goosebumps Release](https://github.com/mage-ai/mage-ai/releases/tag/0.9.38)
- [0.9.35 | Loki Release](https://github.com/mage-ai/mage-ai/releases/tag/0.9.35)
- [0.9.34 | C-3PO Release](https://github.com/mage-ai/mage-ai/releases/tag/0.9.34)
- [0.9.30 | Cowboy Bebop Release](https://github.com/mage-ai/mage-ai/releases/tag/0.9.30)
- [0.9.28 | The Creator Release](https://github.com/mage-ai/mage-ai/releases/tag/0.9.28)
- [0.9.26 | Expend4bles Release](https://github.com/mage-ai/mage-ai/releases/tag/0.9.26)
- [0.9.23 | One Piece Release](https://github.com/mage-ai/mage-ai/releases/tag/0.9.23)
- [0.9.21 | Ahsoka Release](https://github.com/mage-ai/mage-ai/releases/tag/0.9.21)
- [0.9.19 | The Equalizer Release](https://github.com/mage-ai/mage-ai/releases/tag/0.9.19)
- [0.9.16 | Gran Turismo Release](https://github.com/mage-ai/mage-ai/releases/tag/0.9.16)
- [0.9.14 | Blue Beetle Release](https://github.com/mage-ai/mage-ai/releases/tag/0.9.14)
- [0.9.11 | Mutant Mayhem Release](https://github.com/mage-ai/mage-ai/releases/tag/0.9.11)
- [0.9.10 | Haunted Mansion Release](https://github.com/mage-ai/mage-ai/releases/tag/0.9.10)
- [0.9.8 | Dead Reckoning Release](https://github.com/mage-ai/mage-ai/releases/tag/0.9.8)
- [0.9.4 | Solar Flare Release](https://github.com/mage-ai/mage-ai/releases/tag/0.9.4)
- [0.9.0 | The Dial of Destiny Release](https://github.com/mage-ai/mage-ai/releases/tag/0.9.0)
- [0.8.93 | Rise of the Beasts Release](https://github.com/mage-ai/mage-ai/releases/tag/0.8.93)
- [0.8.86 | Fast Release](https://github.com/mage-ai/mage-ai/releases/tag/0.8.86)
- [0.8.83 | Fury of the Gods Release](https://github.com/mage-ai/mage-ai/releases/tag/0.8.83)
- [0.8.78 | Rise of the Machines Release](https://github.com/mage-ai/mage-ai/releases/tag/0.8.78)
- [0.8.75 | Guardians Release](https://github.com/mage-ai/mage-ai/releases/tag/0.8.75)
- [0.8.69 | Hairy Otter Release](https://github.com/mage-ai/mage-ai/releases/tag/0.8.69)
- [0.8.58 | Once & Always Release](https://github.com/mage-ai/mage-ai/releases/tag/0.8.58)
- [0.8.52 | The Super Mario Bros. Release](https://github.com/mage-ai/mage-ai/releases/tag/0.8.52)
- [0.8.44 | Dungeons and Dragons Release](https://github.com/mage-ai/mage-ai/releases/tag/0.8.44)
- [0.8.37 | Foundation Release](https://github.com/mage-ai/mage-ai/releases/tag/0.8.37)
- [0.8.29 | Wick Release](https://github.com/mage-ai/mage-ai/releases/tag/0.8.29)
- [0.8.27 | Shazam Release](https://github.com/mage-ai/mage-ai/releases/tag/0.8.27)
- [0.8.24 | Merlin Release](https://github.com/mage-ai/mage-ai/releases/tag/0.8.24)
- [0.8.15 | Creed Release](https://github.com/mage-ai/mage-ai/releases/tag/0.8.15)
- [0.8.11 | The Mandalorian Release](https://github.com/mage-ai/mage-ai/releases/tag/0.8.11)
- [0.8.3 | Everything Everywhere All at Once Release](https://github.com/mage-ai/mage-ai/releases/tag/0.8.3)
- [0.7.98 | Quantumania Release](https://github.com/mage-ai/mage-ai/releases/tag/0.7.98)
- [0.7.90 | That '90s Show Release](https://github.com/mage-ai/mage-ai/releases/tag/0.7.90)
- [0.7.84 | Rabbit Release](https://github.com/mage-ai/mage-ai/releases/tag/0.7.84)
- [0.7.74 | Lunar Release](https://github.com/mage-ai/mage-ai/releases/tag/0.7.74)
