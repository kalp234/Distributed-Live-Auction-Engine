# PostgreSQL 16 + Patroni (HA manager). Used for both shards.
FROM postgres:16-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-venv curl ca-certificates \
 && rm -rf /var/lib/apt/lists/* \
 && python3 -m venv /opt/patroni \
 && /opt/patroni/bin/pip install --no-cache-dir "patroni[etcd3]==4.0.4" "psycopg2-binary==2.9.10"

# [T3] Patroni clamps the leader-lease ttl to >= 20 s (a production-safety default).
# For a single-host demo we allow ttl=10 so a hard-killed primary is replaced in < 15 s.
# Trade-off: a shorter lease makes spurious failovers more likely on a slow/loaded host.
RUN f=/opt/patroni/lib/python3.11/site-packages/patroni/config.py \
 && grep -q "__get_and_maybe_adjust_int_value(config, 'ttl', 20)" $f \
 && sed -i "s/__get_and_maybe_adjust_int_value(config, 'ttl', 20)/__get_and_maybe_adjust_int_value(config, 'ttl', 8)/" $f

ENV PATH=/opt/patroni/bin:$PATH
COPY patroni.yml /etc/patroni/patroni.tmpl.yml
COPY entrypoint.sh post_init.sh archive_wal.sh /usr/local/bin/
RUN chmod +x /usr/local/bin/entrypoint.sh /usr/local/bin/post_init.sh /usr/local/bin/archive_wal.sh

EXPOSE 5432 8008
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
