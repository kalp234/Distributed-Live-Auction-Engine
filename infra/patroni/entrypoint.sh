#!/bin/bash
# Prepare volumes and render patroni.yml for this node, then hand over to Patroni.
set -euo pipefail

mkdir -p /data /wal_archive /backups
chown -R postgres:postgres /data /wal_archive /backups
chmod 700 /data

# Shard-specific settings substituted into the template.
: "${SYNC_COMMIT:=on}"
sed -e "s|__SYNC_COMMIT__|${SYNC_COMMIT}|g" \
    /etc/patroni/patroni.tmpl.yml > /etc/patroni/patroni.yml
chown postgres:postgres /etc/patroni/patroni.yml

exec gosu postgres patroni /etc/patroni/patroni.yml
