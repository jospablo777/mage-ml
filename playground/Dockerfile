# The Mage image of this repository (the seed service builds it) with the R packages of
# the playground's R blocks. Packages without a binary for the platform compile here:
# on arm64 that includes the Arrow C++ library, which takes a while. rv's cache lives in
# a BuildKit cache mount, so a rebuild after a change to Mage reuses the compiled
# packages instead of compiling them again.
FROM mage-base

COPY r/rproject.toml /opt/playground-r/
COPY r/rv.loc[k] /opt/playground-r/
RUN --mount=type=cache,target=/root/.cache,id=mage-playground-rv,sharing=locked \
    cd /opt/playground-r && rv sync

ENV MAGE_R_PROJECT_DIR=/opt/playground-r
