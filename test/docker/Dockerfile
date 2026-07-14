FROM codercom/code-server:4.127.0

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends tmux \
    && rm -rf /var/lib/apt/lists/*
USER coder

WORKDIR /home/coder/project
