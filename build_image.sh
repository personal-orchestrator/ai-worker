#!/bin/bash
set -e

echo "Building local/ai-worker:latest with podman..."
podman build -t local/ai-worker:latest .

echo "Loading the image into the k3s containerd registry..."
podman save local/ai-worker:latest | sudo k3s ctr images import -
echo "Done!"
