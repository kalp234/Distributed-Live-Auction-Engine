#!/bin/bash
# Runs ONCE on the node that bootstraps the cluster (Patroni post_init).
# $1 = superuser connection URL. Creates the app role, the shard database and its schema.
set -euo pipefail
psql "$1" -v ON_ERROR_STOP=1 <<SQL
CREATE ROLE ${APP_DB_USER} LOGIN PASSWORD '${APP_DB_PASSWORD}';
CREATE DATABASE ${SHARD_DB} OWNER ${APP_DB_USER};
\connect ${SHARD_DB}
SET ROLE ${APP_DB_USER};
\i /sql/${SHARD_DB}.sql
SQL
echo "post_init: created ${SHARD_DB} schema"
