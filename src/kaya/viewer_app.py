import os
from pathlib import Path
from typing import Any, Dict, List, Optional

import uvicorn
from fastapi import APIRouter, FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from kaya.viewer_payloads import VIEWER_ARTIFACTS_DIR, ViewerPayloadBuilder

STATIC_DIR = Path(__file__).with_name('viewer_static')

# kaya's assigned loopback port in the workspace registry; the systemd unit
# binds the same one.
VIEWER_PORT = int(os.getenv('KAYA_VIEWER_PORT', '8010'))

# Set KAYA_VIEWER_ENV=production (e.g. via the systemd unit's EnvironmentFile)
# to disable dev-only behavior: the reloader in main(), the wildcard/no-cache
# defaults below, and the no-cache-everything middleware.
IS_PRODUCTION = os.getenv('KAYA_VIEWER_ENV', 'development') == 'production'

# Live since 2026-07-28 at kaya.peterwilliams.dev, iframe-embedded into
# peterwilliams.dev/kaya. The viewer's own requests are same-origin (asset paths
# are root-relative and the iframe is its own browsing context), so CORS governs
# only third-party callers — which is why production defaults to the two known
# origins instead of the dev wildcard. Override with a comma-separated
# KAYA_VIEWER_ALLOWED_ORIGINS if another origin ever needs API access.
PRODUCTION_ORIGINS = ['https://kaya.peterwilliams.dev', 'https://peterwilliams.dev']
_allowed_origins_env = os.getenv('KAYA_VIEWER_ALLOWED_ORIGINS', '')
ALLOWED_ORIGINS = [origin.strip() for origin in _allowed_origins_env.split(',') if origin.strip()]
if not ALLOWED_ORIGINS and IS_PRODUCTION:
    ALLOWED_ORIGINS = list(PRODUCTION_ORIGINS)

app = FastAPI(title='Kaya Data Analyzer')
app.add_middleware(
    CORSMiddleware,
    # allow_origins=['*'] with allow_credentials=True is a combination
    # browsers reject outright, so credentials only turn on once real
    # origins are configured.
    allow_origins=ALLOWED_ORIGINS or ['*'],
    allow_credentials=bool(ALLOWED_ORIGINS),
    allow_methods=['*'],
    allow_headers=['*'],
)


@app.middleware('http')
async def disable_caching(request: Request, call_next):
    # This is a local-only dev-loop hack: stale browser caches of static
    # JSON/JS have repeatedly hidden real fixes, so every response is
    # explicitly non-cacheable rather than relying on reloads. Skipped in
    # production so the ~80KB JS bundle and multi-MB JSON payloads can
    # actually be cached.
    response = await call_next(request)
    if not IS_PRODUCTION:
        response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate'
        response.headers['Pragma'] = 'no-cache'
    return response

app.mount('/static', StaticFiles(directory=STATIC_DIR), name='static')
if VIEWER_ARTIFACTS_DIR.exists():
    app.mount('/viewer-data', StaticFiles(directory=VIEWER_ARTIFACTS_DIR), name='viewer-data')

payload_builder = ViewerPayloadBuilder()

# Development-only API. These endpoints query the SQLite mirror per request, and
# /charts/body-metrics fits GAMs (generalized additive models) with a 15-point
# cross-validated gridsearch while the request is open. That is exactly what the
# viewer is not allowed to do in production: routes read precomputed payloads,
# they never fit a model or query a database on demand.
#
# The deployed page never calls these — index.html sets the viewer's data mode to
# 'static', so the frontend reads /viewer-data/*.json instead (see
# fetchViewerData in app.js). They exist for the local dev loop, where hitting
# live SQLite beats rebuilding artifacts after every data pull, so the router is
# registered only outside production rather than deleted.
dev_api = APIRouter(prefix='/api', tags=['development-only'])


@app.get('/')
def serve_index() -> FileResponse:
    return FileResponse(STATIC_DIR / 'index.html')


@dev_api.get('/summary')
def get_summary(
    gym_id: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> Dict[str, Any]:
    return payload_builder.build_summary(
        gym_id=gym_id,
        start_date=start_date,
        end_date=end_date,
    )


@dev_api.get('/gyms')
def get_gyms() -> List[Dict[str, Any]]:
    return payload_builder.build_gyms()


@dev_api.get('/charts/time-series')
def get_time_series(
    gym_id: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    freq: str = Query(default='D', pattern='^(D|W|M)$'),
) -> List[Dict[str, Any]]:
    return payload_builder.build_time_series(
        gym_id=gym_id,
        start_date=start_date,
        end_date=end_date,
        freq=freq,
    )


@dev_api.get('/charts/grade-distribution')
def get_grade_distribution(
    gym_id: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    discipline: str = Query(default='bouldering', pattern='^(bouldering|routes)$'),
) -> List[Dict[str, Any]]:
    return payload_builder.build_grade_distribution(
        discipline=discipline,
        gym_id=gym_id,
        start_date=start_date,
        end_date=end_date,
    )


@dev_api.get('/charts/top-gyms')
def get_top_gyms(limit: Optional[int] = None) -> List[Dict[str, Any]]:
    return payload_builder.build_top_gyms(limit=limit)


@dev_api.get('/charts/body-metrics')
def get_body_metrics(
    discipline: str = Query(default='bouldering', pattern='^(bouldering|routes)$'),
    active_only: bool = True,
) -> Dict[str, Any]:
    return payload_builder.build_body_metrics(discipline=discipline, active_only=active_only)


@dev_api.get('/charts/user-segmentation')
def get_user_segmentation() -> Dict[str, Any]:
    return payload_builder.build_user_segmentation()


@dev_api.get('/charts/gym-comparison-base')
def get_gym_comparison_base() -> Dict[str, Any]:
    return payload_builder.build_gym_comparison_base()


@dev_api.get('/state-preview')
def get_state_preview(limit: int = 20) -> List[Dict[str, Any]]:
    return payload_builder.build_state_preview(limit=limit)


if not IS_PRODUCTION:
    app.include_router(dev_api)


def main() -> None:
    # Production is meant to be launched via the systemd unit invoking
    # `uvicorn kaya.viewer_app:app` directly (see KAYA_VIEWER_DESIGN_HANDOFF.md),
    # which bypasses this function entirely. reload is still gated here too,
    # so main() itself is never accidentally hot-reloading in production.
    #
    # 8010 is kaya's assigned port in the workspace registry. It used to be
    # 8000, which is the main site's gunicorn port — running both locally
    # meant one silently failed to bind.
    uvicorn.run('kaya.viewer_app:app', host='127.0.0.1', port=VIEWER_PORT, reload=not IS_PRODUCTION)


if __name__ == '__main__':
    main()
