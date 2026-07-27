from pathlib import Path
from typing import Any, Dict, List, Optional

import uvicorn
from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from kaya.viewer_payloads import VIEWER_ARTIFACTS_DIR, ViewerPayloadBuilder


STATIC_DIR = Path(__file__).with_name('viewer_static')

app = FastAPI(title='Kaya Local Viewer')
app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['*'],
)


@app.middleware('http')
async def disable_caching(request: Request, call_next):
    # This is a local-only dev tool that's actively iterated on; stale browser
    # caches of static JSON/JS have repeatedly hidden real fixes, so make
    # every response explicitly non-cacheable rather than rely on reloads.
    response = await call_next(request)
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    return response

app.mount('/static', StaticFiles(directory=STATIC_DIR), name='static')
if VIEWER_ARTIFACTS_DIR.exists():
    app.mount('/viewer-data', StaticFiles(directory=VIEWER_ARTIFACTS_DIR), name='viewer-data')

payload_builder = ViewerPayloadBuilder()


@app.get('/')
def serve_index() -> FileResponse:
    return FileResponse(STATIC_DIR / 'index.html')


@app.get('/api/summary')
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


@app.get('/api/gyms')
def get_gyms() -> List[Dict[str, Any]]:
    return payload_builder.build_gyms()


@app.get('/api/charts/time-series')
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


@app.get('/api/charts/grade-distribution')
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


@app.get('/api/charts/top-gyms')
def get_top_gyms(limit: Optional[int] = None) -> List[Dict[str, Any]]:
    return payload_builder.build_top_gyms(limit=limit)


@app.get('/api/charts/body-metrics')
def get_body_metrics(
    discipline: str = Query(default='bouldering', pattern='^(bouldering|routes)$'),
    active_only: bool = True,
) -> Dict[str, Any]:
    return payload_builder.build_body_metrics(discipline=discipline, active_only=active_only)


@app.get('/api/charts/user-segmentation')
def get_user_segmentation() -> Dict[str, Any]:
    return payload_builder.build_user_segmentation()


@app.get('/api/charts/gym-comparison-base')
def get_gym_comparison_base() -> Dict[str, Any]:
    return payload_builder.build_gym_comparison_base()


@app.get('/api/state-preview')
def get_state_preview(limit: int = 20) -> List[Dict[str, Any]]:
    return payload_builder.build_state_preview(limit=limit)


def main() -> None:
    uvicorn.run('kaya.viewer_app:app', host='127.0.0.1', port=8000, reload=True)


if __name__ == '__main__':
    main()