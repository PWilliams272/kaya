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


def _drive_a_failing_pull(monkeypatch, max_offset_retries=6):
    """Run the loop against an API that always 500s; return the sleeps it took."""
    slept = []
    monkeypatch.setattr(dp.time, 'sleep', slept.append)
    monkeypatch.setattr(dp, '_resolve_storage_backend', lambda b, use_aws=False: 'db')

    def always_500(gym_id, offset=0):
        raise RuntimeError(
            '{"errors":[{"message":"INTERNAL_SERVER_ERROR"}]}')

    monkeypatch.setattr(dp, 'get_data_for_gym', always_500)
    with pytest.raises(RuntimeError) as exc:
        dp.update_gym_data('904', mode='full',
                           max_offset_retries=max_offset_retries)
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
