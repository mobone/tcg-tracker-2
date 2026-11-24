while true; do
    if ! git diff --quiet HEAD; then
        ./start_service.sh
    fi
    sleep 5
done
