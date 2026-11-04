#!/bin/sh
# One-shot setup of the single-node dev Garage: layout, buckets and the dev
# access key. Runs in the garage container's network namespace with its
# metadata volume, so the `garage` CLI reaches the node like `docker exec`
# would. Safe to re-run.
set -eu

node_id=$(garage node id -q | cut -d@ -f1)

# Assign a role to the node once; a fresh node has layout version 0.
if garage layout show | grep -qi "no nodes currently have a role"; then
    garage layout assign -z dc1 -c 1G "$node_id"
    garage layout apply --version 1
else
    echo "layout already applied"
fi

if ! garage key info "$S3_ACCESS_KEY" >/dev/null 2>&1; then
    garage key import --yes -n dev "$S3_ACCESS_KEY" "$S3_SECRET_KEY"
fi

for bucket in "$ARTIFACT_BUCKET" "$LOG_BUCKET"; do
    garage bucket info "$bucket" >/dev/null 2>&1 || garage bucket create "$bucket"
    garage bucket allow --read --write --owner "$bucket" --key "$S3_ACCESS_KEY"
done

echo "garage ready: buckets $ARTIFACT_BUCKET, $LOG_BUCKET; key $S3_ACCESS_KEY"
