FROM python:3.11-slim AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

ENV \
  PYTHONUNBUFFERED=1 \
  PYTHONDONTWRITEBYTECODE=1 \
  #
  PIP_NO_CACHE_DIR=off \
  PIP_DISABLE_PIP_VERSION_CHECK=on \
  #
  POETRY_VERSION="1.8.2" \
  POETRY_HOME="/src/package/" \
  POETRY_NO_INTERACTION=1 \
  POETRY_VIRTUALENVS_IN_PROJECT=true \
  #
  POETRY_VENV="/src/package/.venv"

ENV PATH="$POETRY_HOME/bin:$POETRY_VENV/bin:$PATH"

WORKDIR /src

RUN python -m pip install poetry

COPY python-package/pyproject.toml python-package/poetry.lock /src/package/

RUN cd /src/package/ && poetry install --no-root --without=dev --all-extras

FROM builder AS final

COPY python-package /src/package/

COPY --from=builder $POETRY_HOME $POETRY_HOME

COPY --from=builder $POETRY_VENV $POETRY_VENV

RUN cd /src/package/ && poetry install --with=docs

WORKDIR /src/docs

CMD mkdocs serve --dev-addr=0.0.0.0:8000
