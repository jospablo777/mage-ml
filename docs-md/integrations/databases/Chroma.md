# Chroma

## Credentials

Open the file named `io_config.yaml` at the root of your MageML project and enter the Chroma fields:

```yaml
version: 0.1.1
default:
  CHROMA_COLLECTION: collection_name
  CHROMA_PATH: path of the Chroma persistent storage
```

## Dependencies

The Chroma client is not installed in the image built from the repository's Dockerfile.
Add it to the project's `requirements.txt` and install it (see
[Install dependencies](../../getting-started/install-dependencies.md)), or, in a source checkout,
add `--extra chroma` to the `uv sync` command you installed with.

```
chromadb>=0.4.17
```

## Using Python block

1. Create a new pipeline or open an existing pipeline.
2. Add a data loader or data exporter with Template. Under "Databases" category you can
 find the "Chroma" template.

- Chroma data loader arguments:
  - n_results: Number of results to match.
  - collection: collection to use. Otherwise the collection defined in io_config.yml will be used.
  - query_texts: the texts or documents used to query.
  - query_embeddings: the embeddings used to query.
  Either `query_texts` or `query_embeddings` should be available.

- Chroma data exporter arguments:
  - df: Dataframe contains the actual data.
  - collection: collection to use. Otherwise the collection defined in io_config.yml will be used.
  - document_column: specify the column in df dataframe that contains documents to be stored in Chroma.
  - id_column: specify the column in df dataframe that contains the id of the document
  - metadata_column: specify the column in df dataframe that contains the metadata of the document (metadata should be dictionary format)

3. Add your customized code into the loader, exporter or add extra transformer blocks.
4. Run the block.
