FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim
WORKDIR /app

COPY uv.lock pyproject.toml ./
RUN uv sync --frozen --no-dev

COPY . .

ENV PATH="/app/.venv/bin:$PATH"

CMD ["python", "-m", "app.main"]
