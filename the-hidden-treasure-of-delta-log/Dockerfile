FROM jupyter/base-notebook:latest

USER root

# Install jq
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        jq && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Python packages
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

USER $NB_UID