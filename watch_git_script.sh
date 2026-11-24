#!/bin/bash
# Redeploy whenever origin has commits this checkout doesn't.
cd "$(dirname "$0")" || exit 1
while true; do
    if git fetch --quiet origin && [ "$(git rev-parse HEAD)" != "$(git rev-parse '@{u}')" ]; then
        ./start_service.sh
    fi
    sleep 60
done
