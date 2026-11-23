# Demo / chaos / test runner. Talks to the stack over the compose network and uses the
# mounted Docker socket to kill/start containers for fault-injection demos.
FROM docker:28-cli AS dockercli
FROM python:3.12-slim
COPY --from=dockercli /usr/local/bin/docker /usr/local/bin/docker
COPY --from=dockercli /usr/local/libexec/docker/cli-plugins/docker-compose /usr/local/libexec/docker/cli-plugins/docker-compose
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/work:/work/app
WORKDIR /work
