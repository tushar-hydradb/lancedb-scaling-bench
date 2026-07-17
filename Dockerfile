# Bench runner image. Pins LanceDB to the version MOVEIT runs in prod so the
# measured ceiling reflects production, not whatever is newest on PyPI.
FROM python:3.13-slim

# Pins: lancedb==0.33.0 + pyarrow==24.0.0 mirror MOVEIT prod
# (docs/sync-pipeline/research/lancedb-json-storage.md). boto3 is for the S3
# footprint probe (object bytes + fragment count), psutil for RSS/CPU.
RUN pip install --no-cache-dir \
      lancedb==0.33.0 \
      pylance==0.33.0 \
      pyarrow==24.0.0 \
      boto3==1.35.99 \
      psutil==6.1.1 \
      numpy==2.2.1 \
      matplotlib==3.10.0

WORKDIR /bench
COPY bench/ /bench/

# Keep the container alive; run_all.sh execs individual scripts into it.
CMD ["sleep", "infinity"]
