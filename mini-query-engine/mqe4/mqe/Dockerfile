FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim

ENV UV_LINK_MODE=copy \
    UV_NO_CACHE=true \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/mqe

WORKDIR /mqe

COPY pyproject.toml uv.lock ./

RUN uv sync

COPY ./core ./core

CMD ["bash"]