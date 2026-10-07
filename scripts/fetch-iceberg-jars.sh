#!/usr/bin/env bash
# Download the pinned Spark source dependencies from a configurable Maven repository.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p artifacts/runtime/jars
proxy="${MAVEN_REPOSITORY_URL:-https://repo.maven.apache.org/maven2}"
for artifact in iceberg-spark-runtime-4.1_2.13 iceberg-aws-bundle; do
    target="artifacts/runtime/jars/${artifact}-1.11.0.jar"
    if [[ ! -f "$target" ]]; then
        curl --fail --location --output "${target}.tmp" \
            "${proxy}/org/apache/iceberg/${artifact}/1.11.0/${artifact}-1.11.0.jar"
        mv "${target}.tmp" "$target"
    fi
done
shasum -a 256 -c scripts/iceberg-jars.sha256
