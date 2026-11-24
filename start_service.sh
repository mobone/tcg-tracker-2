#!/bin/bash
# Redeploy: stop the app, back up the databases, pull, restore the databases, rebuild.
# Run from the repo directory (the watcher cds there first).
[ -f docker-compose.yml ] || { echo "start_service.sh: run this from the tcg-tracker-2 directory" >&2; exit 1; }

docker compose down

# The databases are untracked runtime data. Back them up before touching git so a pull can never
# lose them (the commit that untracked them deletes them from the working tree on pull).
backup_dir="$HOME/db-backup/$(date +%Y%m%d-%H%M%S)"
mkdir -p "$backup_dir/data"
cp -p data/mtg_tracker.db* "$backup_dir/data/" 2>/dev/null
cp -p tcg_collection.db "$backup_dir/" 2>/dev/null

# While the databases are still tracked (before the untracking commit is pulled), discard their
# local changes so the pull can apply. The live copies are restored from the backup below.
for f in data/mtg_tracker.db tcg_collection.db; do
    if git ls-files --error-unmatch "$f" >/dev/null 2>&1; then
        git checkout -- "$f"
    fi
done

git pull --ff-only || echo "git pull failed; restoring databases and restarting the current version"

cp -p "$backup_dir"/data/mtg_tracker.db* data/ 2>/dev/null
cp -p "$backup_dir"/tcg_collection.db . 2>/dev/null

# Keep only the 3 most recent backups.
ls -1dt "$HOME"/db-backup/*/ 2>/dev/null | tail -n +4 | xargs -r rm -rf

docker compose up -d --build
