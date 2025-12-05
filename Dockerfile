FROM python:3.13-slim-trixie

ENV UV_COMPILE_BYTECODE=1
ENV UV_NO_CACHE=1

COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

WORKDIR /app

COPY . /app

RUN uv sync --locked --no-editable --no-dev

ENTRYPOINT [ "uv", "run", "schedule_scaling/main.py" ]