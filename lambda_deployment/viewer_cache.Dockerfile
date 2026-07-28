# Container-image Lambda for the daily viewer-cache precompute job. Built
# from the repo root as context (see .github/workflows/deploy-viewer-cache-lambda.yml):
#   docker build -f lambda_deployment/viewer_cache.Dockerfile -t <image> .
#
# Needs a full pandas/numpy/scipy/pygam stack, which is well past the zip-deploy
# 250MB limit that lambda_deployment/requirements.txt (the other, zip-based
# kaya-data-updater Lambda) has to stay under — hence a separate image and a
# separate, leaner requirements file (viewer_cache_requirements.txt) scoped to
# exactly what this job needs.
FROM public.ecr.aws/lambda/python:3.11

COPY lambda_deployment/viewer_cache_requirements.txt ${LAMBDA_TASK_ROOT}/requirements.txt
# --only-binary forces wheel-only installs for the compiled/C-extension
# packages: this base image has no C compiler, so if pip ever falls back to
# building one of these from source (e.g. because it picked an unpinned
# version with no prebuilt wheel for this platform), it fails opaquely deep
# inside a meson/setup.py subprocess instead of with a clear "no matching
# distribution" error. Forcing wheel-only surfaces that immediately instead.
RUN pip install --no-cache-dir \
    --only-binary=numpy,scipy,pandas,pyarrow \
    -r ${LAMBDA_TASK_ROOT}/requirements.txt --target ${LAMBDA_TASK_ROOT}

COPY src/kaya ${LAMBDA_TASK_ROOT}/kaya

CMD ["kaya.build_viewer_cache_lambda.lambda_handler"]
