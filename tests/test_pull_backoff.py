"""The retry loop's patience, which is what turned one bad gym into eleven.

Kaya's API surfaces rate limiting as a generic `INTERNAL_SERVER_ERROR`, not a
429 with a `Retry-After` header. The old loop read that as "transient blip",
retried three times back-to-back, and gave up in about a second -- so a quota
that needed minutes to refill instead got hammered, and gyms that pulled fine
the next morning were recorded as failures.

What has to be true, and is tested here:

  * the waits GROW, and the total is on the timescale a quota refills on
    (minutes) rather than the one a network blip clears on (seconds),
  * the loop actually sleeps for them rather than merely computing them, and
  * the error a caller finally sees says how long was spent and names rate
    limiting, because "max retries exceeded" sent the last investigation
    looking at the gym's data instead of at our request volume.

No network: `get_data_for_gym` and `time.sleep` are both stubbed.
"""
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))

dp = pytest.importorskip('kaya.data_puller')


def test_each_wait_is_longer_than_the_last():
    waits = [dp._backoff_seconds(i) for i in range(1, len(dp._BACKOFF_SCHEDULE) + 1)]
    assert waits == sorted(waits) and len(set(waits)) == len(waits)


def test_the_schedule_holds_at_its_last_value_rather_than_running_off_the_end():
    """Attempts beyond the schedule must not IndexError, and must not reset."""
    last = dp._BACKOFF_SCHEDULE[-1]
    assert dp._backoff_seconds(len(dp._BACKOFF_SCHEDULE) + 5) == last


def test_a_full_run_of_retries_waits_minutes_not_seconds():
    """The whole point of the change: 3 fast retries became ~22 min of patience.

    If this drops back under a couple of minutes, the loop has gone back to
    giving up before a rate limit could plausibly have cleared.
    """
    total = sum(dp._backoff_seconds(i) for i in range(1, 6))
    assert total > 20 * 60


def _drive_a_failing_pull(monkeypatch, max_offset_retries=6,
                          max_consecutive_skips=0):
    """Run the loop against an API that always 500s; return the sleeps it took.

    `max_consecutive_skips=0` so these tests still measure ONE offset's retry
    schedule. With skipping enabled the loop steps past an exhausted page and
    starts the schedule again on the next one, which is the right behaviour and
    the wrong thing to measure here.
    """
    slept = []
    monkeypatch.setattr(dp.time, 'sleep', slept.append)
    monkeypatch.setattr(dp, '_resolve_storage_backend', lambda b, use_aws=False: 'db')

    def always_500(gym_id, offset=0):
        raise RuntimeError(
            '{"errors":[{"message":"INTERNAL_SERVER_ERROR"}]}')

    monkeypatch.setattr(dp, 'get_data_for_gym', always_500)
    with pytest.raises(RuntimeError) as exc:
        dp.update_gym_data('904', mode='full',
                           max_offset_retries=max_offset_retries,
                           max_consecutive_skips=max_consecutive_skips)
    return slept, str(exc.value)


def test_the_loop_sleeps_the_schedule_it_computes(monkeypatch):
    """Guards against the version that logs a backoff and then doesn't take it."""
    slept, _ = _drive_a_failing_pull(monkeypatch)
    assert slept == [5.0, 30.0, 120.0, 300.0, 900.0]


def test_it_gives_up_after_the_configured_number_of_attempts(monkeypatch):
    """Five sleeps, six attempts -- it does not sleep after the last failure."""
    slept, _ = _drive_a_failing_pull(monkeypatch, max_offset_retries=6)
    assert len(slept) == 5


def test_the_final_error_points_at_throttling_not_at_the_gym(monkeypatch):
    """The message is the only thing the next investigation will read."""
    _, msg = _drive_a_failing_pull(monkeypatch)
    assert 'rate limiting' in msg
    assert '1355s' in msg, 'the message should say how long was actually spent'
    assert '904' in msg


# --- the hang, which is worse than a failure ---------------------------------


def test_every_kaya_request_carries_a_timeout(monkeypatch):
    """A `requests` call with no timeout waits forever.

    On 2026-08-06 the laptop slept mid-pull, the peer dropped the connection,
    and the read blocked on a dead socket for six hours. Nothing raised, so the
    retry loop never retried and the backpressure break never broke -- every
    recovery path in this module is watching for an *error*, and a hang is not
    one. This asserts the timeout is passed, which is what converts the hang
    back into an error the rest of the machinery already handles.
    """
    seen = {}

    class _Resp:
        status_code = 200

        def raise_for_status(self):
            return None

        def json(self):
            return {'data': {'webAscentsForGym': []}}

    def fake_post(url, **kw):
        seen.update(kw)
        return _Resp()

    monkeypatch.setattr(dp.requests, 'post', fake_post)
    dp.kaya_api_post('https://example.invalid/graphql', {})
    assert 'timeout' in seen, 'a request with no timeout can hang forever'
    connect, read = seen['timeout']
    assert 0 < connect <= 30, 'connect timeout should fail fast'
    assert 0 < read <= 300, 'read timeout must be shorter than a human notices'


def test_an_explicit_timeout_is_not_overridden(monkeypatch):
    """The default must not clobber a caller that knows better."""
    seen = {}

    class _Resp:
        status_code = 200

        def raise_for_status(self):
            return None

        def json(self):
            return {}

    monkeypatch.setattr(dp.requests, 'post',
                        lambda url, **kw: (seen.update(kw), _Resp())[1])
    dp.kaya_api_post('https://example.invalid/graphql', {},
                               timeout=(1.0, 2.0))
    assert seen['timeout'] == (1.0, 2.0)


# ---------------------------------------------------------------------------
# The retry budget has to survive the trip through SQS.

def test_the_fanout_stamps_the_shared_retry_budget_into_each_job():
    """The queued job must carry the same budget the library defaults to.

    This is the failure this test exists for, found in production on
    2026-08-07: `update_gym_data`'s default was raised from 3 to 6, and it
    changed nothing. The fanout writes an explicit `max_offset_retries` into
    every SQS message and *its* default was still 3, so the deployed path never
    read the library default. The DLQ went on filling at ~8 gyms/day with
    "Exceeded max retries at offset N", and every message body said
    `"max_offset_retries": 3`.

    Three places have to agree, so none of them may hold a literal.
    """
    import inspect

    from kaya.data_puller import DEFAULT_OFFSET_RETRIES, update_gym_data
    from kaya.update_data_script import dispatch_gym_updates

    puller = inspect.signature(update_gym_data)
    fanout = inspect.signature(dispatch_gym_updates)

    assert puller.parameters['max_offset_retries'].default == DEFAULT_OFFSET_RETRIES
    assert fanout.parameters['max_offset_retries'].default == DEFAULT_OFFSET_RETRIES, (
        'the SQS fanout defaults to a different retry budget than the puller. '
        'It stamps its own value into every job message, so the puller default '
        'is dead code in production and raising it fixes nothing.'
    )


def test_the_consumer_falls_back_to_the_shared_budget_not_a_literal():
    """A job message with no explicit budget must still get the full one."""
    from pathlib import Path

    from kaya.data_puller import DEFAULT_OFFSET_RETRIES

    src = Path(__file__).resolve().parents[1] / 'src' / 'kaya'
    body = (src / 'update_data_script.py').read_text()
    assert "payload.get('max_offset_retries', 3)" not in body.replace('\n', ' '), (
        'the SQS consumer falls back to a hardcoded 3 rather than '
        'DEFAULT_OFFSET_RETRIES; an older queued message would silently run '
        'with the pre-fix budget.'
    )
    assert DEFAULT_OFFSET_RETRIES > 3, (
        'the whole point of the change was more patience than the 3 attempts '
        'that were losing ~8 gyms a day to the DLQ.'
    )


# --- skipping a poisoned page ------------------------------------------------
#
# Gym 944 went 37 days without a successful update because offset 90 returned
# INTERNAL_SERVER_ERROR deterministically. Raising abandoned the whole gym --
# including every page after the bad one -- and no retry budget can fix a
# deterministic error. These pin the behaviour that recovers it.


def _pull_with_one_bad_page(monkeypatch, bad_offset=90, n_pages=8,
                            **kwargs):
    """An API where exactly one page always fails and the rest are fine."""
    import pandas as pd

    monkeypatch.setattr(dp.time, 'sleep', lambda _s: None)
    monkeypatch.setattr(dp, '_resolve_storage_backend', lambda b, use_aws=False: 'db')
    written = []
    monkeypatch.setattr(dp, '_write_batch',
                        lambda df, **kw: written.append(df))

    seen = []

    def api(gym_id, offset=0):
        seen.append(offset)
        if offset == bad_offset:
            raise RuntimeError(
                '{"errors":[{"message":"INTERNAL_SERVER_ERROR"}]}')
        if offset >= n_pages * 15:
            return pd.DataFrame()
        return pd.DataFrame({'send_id': [f'{offset}-{i}' for i in range(15)]})

    monkeypatch.setattr(dp, 'get_data_for_gym', api)
    dp.update_gym_data('944', mode='full', max_offset_retries=2, **kwargs)
    return seen, written


def test_a_deterministically_failing_page_is_stepped_over(monkeypatch):
    """The whole point: one bad page must not cost the whole gym."""
    seen, written = _pull_with_one_bad_page(monkeypatch)
    assert 105 in seen, 'the puller stopped at the bad page instead of stepping past it'
    assert max(seen) >= 105, 'nothing after the bad page was fetched'
    rows = sum(len(d) for d in written)
    assert rows > 0, 'no data was written at all'


def test_the_pages_after_the_bad_one_are_all_pulled(monkeypatch):
    """A gap of one page, not a gap of everything from there on."""
    seen, _ = _pull_with_one_bad_page(monkeypatch, bad_offset=30, n_pages=6)
    assert [o for o in seen if o > 30] == sorted({45, 60, 75, 90})[:4]


def test_the_skipped_offset_reaches_the_state_file(monkeypatch):
    """A skip is a success with a KNOWN hole. Unrecorded, it is just data loss."""
    import pandas as pd

    monkeypatch.setattr(dp.time, 'sleep', lambda _s: None)
    monkeypatch.setattr(dp, '_resolve_storage_backend', lambda b, use_aws=False: 's3')
    monkeypatch.setattr(dp, '_write_batch', lambda df, **kw: None)
    monkeypatch.setattr(dp, 'get_existing_send_ids', lambda *a, **k: [])

    class FakeWriter:
        run_id = 'r1'
        run_started_at = None
        bucket = 'b'

        def __init__(self, gym_id):
            pass

    monkeypatch.setattr(dp, 'S3SendRunWriter', FakeWriter)
    captured = {}
    monkeypatch.setattr(dp, 'write_recent_send_state',
                        lambda **kw: captured.update(kw))

    def api(gym_id, offset=0):
        if offset == 15:
            raise RuntimeError('{"errors":[{"message":"INTERNAL_SERVER_ERROR"}]}')
        if offset >= 45:
            return pd.DataFrame()
        return pd.DataFrame({'send_id': [f'{offset}-{i}' for i in range(15)]})

    monkeypatch.setattr(dp, 'get_data_for_gym', api)
    dp.update_gym_data('944', mode='full', max_offset_retries=2)
    assert captured.get('skipped_offsets') == [15]


def test_a_run_of_failures_still_fails_the_gym(monkeypatch):
    """Skipping must not turn an outage into a silent no-op.

    Consecutive failures are the API being down, not a poisoned page, and that
    belongs in the DLQ where something can notice it.
    """
    import pandas as pd

    monkeypatch.setattr(dp.time, 'sleep', lambda _s: None)
    monkeypatch.setattr(dp, '_resolve_storage_backend', lambda b, use_aws=False: 'db')
    monkeypatch.setattr(dp, '_write_batch', lambda df, **kw: None)

    def api(gym_id, offset=0):
        if offset == 0:
            return pd.DataFrame({'send_id': [f'a{i}' for i in range(15)]})
        raise RuntimeError('{"errors":[{"message":"INTERNAL_SERVER_ERROR"}]}')

    monkeypatch.setattr(dp, 'get_data_for_gym', api)
    with pytest.raises(RuntimeError) as exc:
        dp.update_gym_data('944', mode='full', max_offset_retries=2,
                           max_consecutive_skips=2)
    assert 'in a row' in str(exc.value)


def test_the_total_skip_budget_stops_a_grind(monkeypatch):
    """Alternating good/bad pages never trips the consecutive rule, so the
    total budget is what stops the loop walking a broken gym forever."""
    import pandas as pd

    monkeypatch.setattr(dp.time, 'sleep', lambda _s: None)
    monkeypatch.setattr(dp, '_resolve_storage_backend', lambda b, use_aws=False: 'db')
    monkeypatch.setattr(dp, '_write_batch', lambda df, **kw: None)

    def api(gym_id, offset=0):
        if (offset // 15) % 2 == 1:
            raise RuntimeError('{"errors":[{"message":"INTERNAL_SERVER_ERROR"}]}')
        return pd.DataFrame({'send_id': [f'{offset}-{i}' for i in range(15)]})

    monkeypatch.setattr(dp, 'get_data_for_gym', api)
    with pytest.raises(RuntimeError) as exc:
        dp.update_gym_data('944', mode='full', max_offset_retries=1,
                           max_consecutive_skips=2, max_skipped_pages=3)
    assert 'page(s) this run' in str(exc.value)


def test_the_lambda_handler_does_not_hold_its_own_retry_literal():
    """The FOURTH place this number lived, and the one production actually ran.

    EventBridge invokes `lambda_handler` directly, so its default is the value
    every scheduled run used. It said 3 while the library said 6, which voided
    the retry-budget fix for the entire deployed path -- the same failure as
    the fanout's literal, one layer further out.
    """
    import inspect

    from kaya import update_data_script as uds
    from kaya.data_puller import DEFAULT_OFFSET_RETRIES

    src = inspect.getsource(uds.lambda_handler)
    assert "'max_offset_retries', 3" not in src, (
        'the handler is back to hardcoding a retry budget'
    )
    assert 'DEFAULT_OFFSET_RETRIES' in src, (
        'the handler must fall back to the shared budget'
    )
    assert DEFAULT_OFFSET_RETRIES > 3
