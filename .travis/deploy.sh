#!/bin/bash

set -e

tag=$1

cd "${TRAVIS_BUILD_DIR}"
export TRAVIS_BUILD_RELEASE_TAG="${tag}"
release-manager \
    --config "./.travis/release.yml" \
    --check-version \
    --make-version \
    --make-artifact \
    --upload-artifact

echo "DEPLOY: igluctl deployed..."
