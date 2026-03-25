FROM python:3.13-slim

WORKDIR /app

# Install uv
RUN pip install --no-cache-dir uv

# Copy tkati-node package and install it
COPY packages/tkati-node /build/tkati-node
RUN uv pip install --system --no-cache /build/tkati-node

# Copy pipeline code
COPY examples/clicks/pipelines /app/pipelines

ENV PYTHONPATH=/app

ENTRYPOINT ["tkati-node"]
