FROM ghcr.io/astral-sh/uv:0.9.18 AS uv
FROM python:3.13-slim-bookworm AS base
COPY --from=uv /uv /uvx /bin/
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    PATH="/service/.venv/bin:$PATH"
WORKDIR /service
COPY . .

FROM base AS development
RUN uv sync --group dev
CMD ["python", "-m", "pytest", "tests/unit"]

FROM base AS production
RUN uv sync --no-dev && \
    groupadd --gid 10001 service && \
    useradd --uid 10001 --gid 10001 --no-create-home service
USER 10001:10001
CMD ["python", "-m", "scripts.run_api"]
