FROM jupyter/base-notebook:latest

USER root

# Install tree
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        tree && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Python packages
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt

USER $NB_UID