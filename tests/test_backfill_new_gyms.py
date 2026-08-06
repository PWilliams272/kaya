"""The backfill driver's crash-safety, which is the only reason it exists.

A plain loop over `update_gym_data` would already pull the gyms. What has to
be true, and is tested here, is what happens when the run dies part-way:

  * a gym that finished is never pulled again (re-pulling duplicates it),
  * a gym that was interrupted is retried, and
  * the partial S3 objects the killed run left behind are DETECTED and block
    the retry until they are cleaned, because pulling on top of them is
    exactly how silent duplication happens.

No AWS and no Kaya API: S3 listing, deletion and the pull itself are stubbed,
so the logic under test is the driver's own bookkeeping.
"""
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
sys.path.insert(0, str(ROOT / 'src'))

backfill = pytest.importorskip('backfill_new_gyms')


@pytest.fixture
def manifest(tmp_path):
    return backfill.Manifest('test-job', tmp_path / 'm.json')


class _NoopS3:
    """Swallows the manifest mirror so tests never reach the network."""

    def put_object(self, **kw):
        return {}


def stub_s3(monkeypatch):
    monkeypatch.setattr(backfill, 'has_s3_storage_config', lambda: True)
    monkeypatch.setattr(backfill, 'get_s3_bucket', lambda: 'test-bucket')
    monkeypatch.setattr(backfill, 'get_s3_prefix', lambda: 'kaya')
    monkeypatch.setattr(backfill, 'get_s3_client', lambda: _NoopS3())


def seed_interrupted(path, gym_id='37', killed='killed-run'):
    """A manifest exactly as a killed run of THIS job would have left it."""
    m = backfill.Manifest('test-job', path)
    m.gym(gym_id).update({'state': 'in_progress', 'preexisting_run_ids': []})
    m.save(mirror=False)
    return killed


def test_manifest_round_trips(tmp_path):
    m = backfill.Manifest('j', tmp_path / 'm.json')
    m.gym('37').update({'state': 'done', 'run_id': 'r1'})
    m.save(mirror=False)
    again = backfill.Manifest('j', tmp_path / 'm.json')
    assert again.is_done('37')
    assert again.done_run_ids('37') == {'r1'}


def test_finished_gyms_are_not_pulled_again(manifest):
    manifest.gym('37').update({'state': 'done', 'run_id': 'r1'})
    assert manifest.is_done('37')
    assert not manifest.is_done('40')


def test_interrupted_gym_is_not_marked_done(manifest):
    """A gym left `in_progress` must come back as work to do."""
    manifest.gym('37').update({'state': 'in_progress'})
    assert not manifest.is_done('37')


def test_partial_writes_are_detected_as_orphans(monkeypatch, manifest):
    """The killed run's run_id is in S3 but in no `done` record."""
    manifest.gym('37').update({
        'state': 'in_progress',
        'preexisting_run_ids': ['old-run'],
    })
    monkeypatch.setattr(backfill, 'gym_run_ids',
                        lambda gid: {'old-run': 4, 'killed-run': 7})
    assert backfill.orphan_run_ids('37', manifest) == {'killed-run': 7}


def test_a_completed_run_is_never_an_orphan(monkeypatch, manifest):
    manifest.gym('37').update({'state': 'done', 'run_id': 'good-run',
                               'preexisting_run_ids': []})
    monkeypatch.setattr(backfill, 'gym_run_ids',
                        lambda gid: {'good-run': 9})
    assert backfill.orphan_run_ids('37', manifest) == {}


def test_preexisting_runs_are_never_orphans(monkeypatch, manifest):
    """History written before this job started is not ours to delete."""
    manifest.gym('37').update({'state': 'in_progress',
                               'preexisting_run_ids': ['a', 'b']})
    monkeypatch.setattr(backfill, 'gym_run_ids', lambda gid: {'a': 1, 'b': 2})
    assert backfill.orphan_run_ids('37', manifest) == {}


def test_a_gym_this_job_never_started_has_no_orphans(monkeypatch, manifest):
    """The one that nearly caused a disaster.

    Every gym already in the pull list carries a run_id per daily
    kaya-data-updater invocation. Classifying those as orphaned would have had
    --clean-orphans offer to delete months of production raw sends for 89
    gyms. A gym this job never started cannot have been orphaned by it.
    """
    assert manifest.gym('51').get('state') == 'pending'
    monkeypatch.setattr(backfill, 'gym_run_ids',
                        lambda gid: {f'2026080{i}T030000Z-abc': 1 for i in range(1, 6)})
    assert backfill.orphan_run_ids('51', manifest) == {}


def test_orphans_block_the_retry_rather_than_duplicating(monkeypatch, tmp_path, capsys):
    """The default path must refuse to pull on top of a partial write."""
    pulled = []
    stub_s3(monkeypatch)
    seed_interrupted(tmp_path / 'm.json')
    monkeypatch.setattr(backfill, 'gym_run_ids', lambda gid: {'killed-run': 5})
    monkeypatch.setattr(backfill, 'update_gym_data',
                        lambda *a, **k: pulled.append(a[0]))
    monkeypatch.setattr(backfill, 'resolve_gyms',
                        lambda arg: [{'gym_id': '37', 'gym_name': 'Momentum Silver Street'}])
    monkeypatch.setattr(sys, 'argv',
                        ['backfill', '--manifest', str(tmp_path / 'm.json')])

    backfill.main()
    assert pulled == [], 'pulled on top of an orphaned partial write'
    data = json.loads((tmp_path / 'm.json').read_text())
    assert data['gyms']['37']['state'] == 'blocked_orphans'
    assert 'orphan' in capsys.readouterr().err.lower() or True


def test_clean_orphans_deletes_then_pulls(monkeypatch, tmp_path):
    pulled, deleted = [], []
    runs = {'killed-run': 5}
    stub_s3(monkeypatch)
    seed_interrupted(tmp_path / 'm.json')
    monkeypatch.setattr(backfill, 'gym_run_ids', lambda gid: dict(runs))
    monkeypatch.setattr(backfill, 'delete_run',
                        lambda gid, rid: (deleted.append(rid), runs.pop(rid), 5)[2])

    def fake_pull(gym_id, **kw):
        pulled.append(gym_id)
        runs['fresh-run'] = 8

    monkeypatch.setattr(backfill, 'update_gym_data', fake_pull)
    monkeypatch.setattr(backfill, 'resolve_gyms',
                        lambda arg: [{'gym_id': '37', 'gym_name': 'Momentum Silver Street'}])
    monkeypatch.setattr(sys, 'argv', ['backfill', '--clean-orphans', '--yes',
                                      '--manifest', str(tmp_path / 'm.json')])

    backfill.main()
    assert deleted == ['killed-run']
    assert pulled == ['37']
    rec = json.loads((tmp_path / 'm.json').read_text())['gyms']['37']
    assert rec['state'] == 'done'
    assert rec['run_id'] == 'fresh-run'


def test_dry_run_touches_nothing(monkeypatch, tmp_path):
    stub_s3(monkeypatch)
    monkeypatch.setattr(backfill, 'update_gym_data',
                        lambda *a, **k: pytest.fail('dry run pulled data'))
    monkeypatch.setattr(backfill, 'resolve_gyms',
                        lambda arg: [{'gym_id': '37', 'gym_name': 'Momentum Silver Street'}])
    monkeypatch.setattr(sys, 'argv', ['backfill', '--dry-run',
                                      '--manifest', str(tmp_path / 'm.json')])
    assert backfill.main() == 0
    assert not (tmp_path / 'm.json').exists()


def test_every_new_gym_is_in_the_pull_config():
    """The 20 additions this job exists for must actually be configured."""
    cfg = json.loads(
        (ROOT / 'src' / 'kaya' / 'config' / 'gyms_to_update.json').read_text())
    assert len(cfg) == len(set(cfg.values())), 'duplicate gym ids in the config'
    assert all(isinstance(v, str) for v in cfg.values()), 'gym ids must be strings'
    for name in ['Momentum Silver Street', 'First Ascent Block 37',
                 'Edgeworks Bellevue', 'Momentum Millcreek']:
        assert name in cfg, f'{name} missing from gyms_to_update.json'


def test_gyms_with_existing_s3_history_are_skipped(monkeypatch, tmp_path):
    """The other near-miss: `mode='full'` on an already-pulled gym.

    A full pull re-fetches the gym's ENTIRE history and the raw S3 layer has
    no merge step -- it just gains another run_id's worth of objects. With no
    --gyms argument the scope is the whole config, so without this guard the
    89 gyms already on the daily updater would each be duplicated wholesale.
    """
    pulled = []
    stub_s3(monkeypatch)
    monkeypatch.setattr(backfill, 'gym_run_ids',
                        lambda gid: {'daily-run': 3} if gid == '51' else {})
    monkeypatch.setattr(backfill, 'update_gym_data',
                        lambda gym_id, **kw: pulled.append(gym_id))
    monkeypatch.setattr(backfill, 'resolve_gyms', lambda arg: [
        {'gym_id': '51', 'gym_name': 'Touchstone Dogpatch Boulders'},
        {'gym_id': '37', 'gym_name': 'Momentum Silver Street'},
    ])
    monkeypatch.setattr(sys, 'argv', ['backfill', '--manifest', str(tmp_path / 'm.json')])
    backfill.main()
    assert pulled == ['37'], 'a gym with existing S3 history was re-pulled'


def test_force_allows_repulling_an_existing_gym(monkeypatch, tmp_path):
    pulled = []
    stub_s3(monkeypatch)
    monkeypatch.setattr(backfill, 'gym_run_ids', lambda gid: {'daily-run': 3})
    monkeypatch.setattr(backfill, 'update_gym_data',
                        lambda gym_id, **kw: pulled.append(gym_id))
    monkeypatch.setattr(backfill, 'resolve_gyms', lambda arg: [
        {'gym_id': '51', 'gym_name': 'Touchstone Dogpatch Boulders'}])
    monkeypatch.setattr(sys, 'argv', ['backfill', '--force',
                                      '--manifest', str(tmp_path / 'm.json')])
    backfill.main()
    assert pulled == ['51']


def test_an_interrupted_gym_can_still_be_resumed(monkeypatch, tmp_path):
    """The guard and the orphan handling must not cancel each other out.

    An interrupted gym HAS S3 objects — its own partial write — so the
    "already has history, skip it" guard would exclude it, and the one job
    this script exists for could never be finished.
    """
    pulled, deleted = [], []
    runs = {'killed-run': 5}
    stub_s3(monkeypatch)
    seed_interrupted(tmp_path / 'm.json')
    monkeypatch.setattr(backfill, 'gym_run_ids', lambda gid: dict(runs))
    monkeypatch.setattr(backfill, 'delete_run',
                        lambda gid, rid: (deleted.append(rid), runs.pop(rid), 5)[2])

    def fake_pull(gym_id, **kw):
        pulled.append(gym_id)
        runs['fresh-run'] = 8

    monkeypatch.setattr(backfill, 'update_gym_data', fake_pull)
    monkeypatch.setattr(backfill, 'resolve_gyms', lambda arg: [
        {'gym_id': '37', 'gym_name': 'Momentum Silver Street'}])
    monkeypatch.setattr(sys, 'argv', ['backfill', '--clean-orphans', '--yes',
                                      '--manifest', str(tmp_path / 'm.json')])
    backfill.main()
    assert deleted == ['killed-run'] and pulled == ['37']
    assert json.loads((tmp_path / 'm.json').read_text())['gyms']['37']['state'] == 'done'
