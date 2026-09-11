# Custom Spark Docker Image for MageAI
This optional image uses Spark 3.5.9, Spark NLP 6.4.1, Java 11, and Python 3.12. Its Python environment is locked in `runtimes/custom-spark/uv.lock`. pandas 2 and NumPy 1 remain isolated from the main Mage runtime.

## Docker Build

Run from the repository root.
```bash
  docker build -f integrations/custom_spark/Dockerfile -t <docker_name/docker_repo>:<tag> .
```

## Docker Run
```bash
  docker run --rm -it <docker_name/docker_repo>:<tag> bash
```

## Docker Push
```bash
  docker push <docker_name/docker_repo>:<tag> 
  ```

## Acknowledgements
Special thanks to [Malaysia.AI](https://github.com/malaysia-ai) for assisting in creating this dockerfile to serve a custom Spark cluster along with its dependencies. 

The image build and Spark NLP execution require separate validation before deployment. The existing `spark.yaml` is a Bitnami chart values example, not a Helm chart; use it with the corresponding chart and review its image and entrypoint settings.
