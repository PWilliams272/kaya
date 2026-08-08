"""Pick which gyms to add to the pull roster, and say why for each one.

`discover_gyms.py` finds every gym Kaya's search will return (1,002 of them).
This decides which are worth pulling, against three goals, in this order:

  1. OVERLAP -- climbers who log at two gyms. This is not a nice-to-have, it is
     the whole identifiability story. A gym's correction is only ever estimated
     RELATIVE to other gyms, through climbers who visited both; a gym nobody
     shares with the rest of the network is an island whose correction is not
     identifiable at all. This is the same trap that produced ~19,000 standard
     errors in the gym-drift solve when the contrast graph split into
     components (docs/two-stage-and-grade-compression.md 6.5). Adding an island
     does not just fail to help -- it adds parameters the data cannot constrain.

  2. CHAIN COVERAGE -- a chain we already pull, in a new place. Chains are the
     long edges of the graph: climbers move between branches, so a new branch
     ties a new city back to the network even before local overlap builds up.
     Chains also carry a shared setting culture, which is exactly the thing a
     per-gym correction is trying to measure against.

  3. CITY COVERAGE -- a metro we do not have. Valuable, but only worth taking
     as a CLUSTER: two or more gyms in the same new city can at least be tied
     to each other. A lone gym in a lone city is the island case above.

Size gates come last and are blunt: a gym with no boulders contributes nothing
to a bouldering model however well connected it is.

    python scripts/select_new_gyms.py                    # the ranked proposal
    python scripts/select_new_gyms.py --top 40 --write   # write the manifest

The manifest it writes is what `backfill_new_gyms.py --manifest` consumes. The
order is deliberate: full history is pulled BEFORE a gym joins the roster, so
the nightly incremental pull never meets a gym with no history behind it.
"""
from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
AVAILABLE = ROOT / 'src' / 'kaya' / 'config' / 'gyms_available.json'
ROSTER = ROOT / 'src' / 'kaya' / 'config' / 'gyms_to_update.json'
MANIFEST = ROOT / 'runs' / 'new_gyms_manifest.json'

MIN_BOULDERS = 20
MIN_FOLLOWERS = 60

# Words that carry no chain identity, so "The Circuit" and "Circuit Bouldering"
# collapse to the same chain and "Rock Gym" never becomes one.
STOPWORDS = {'the', 'gym', 'gyms', 'rock', 'climbing', 'bouldering', 'boulders',
             'center', 'centre', 'co', 'company', 'club', 'wall', 'walls',
             'fitness', 'and', 'of', 'at'}


def chain_key(name: str) -> str:
    """A chain's identity: its first meaningful word.

    ONE word, not two. Two was tried and matched nothing, because the second
    word is the branch: "Bouldering Project Poplar" and "Bouldering Project -
    Upper Walls" became 'project poplar' and 'project upper', which are
    different strings for the same chain. Once the generic words are dropped
    the chain lives entirely in the first one.
    """
    words = [w for w in re.split(r'[^a-z0-9]+', str(name).lower()) if w]
    kept = [w for w in words if w not in STOPWORDS]
    return kept[0] if kept else ''


def load():
    av = pd.DataFrame(json.loads(AVAILABLE.read_text()))
    roster = json.loads(ROSTER.read_text())
    have_ids = {str(v) for v in roster.values()}
    av['id'] = av['id'].astype(str)
    av['have'] = av['id'].isin(have_ids)
    av['chain'] = av['name'].map(chain_key)
    av['place'] = av['city'].fillna('') + ', ' + av['region'].fillna('')
    return av, roster


def score(av: pd.DataFrame) -> pd.DataFrame:
    have = av[av.have]
    our_places = set(have['place'])
    # A chain counts as ours once we pull two of its branches; one branch is
    # usually just a gym whose name happens to start with a common word.
    # A chain is ours once we pull two of its branches IN A COUNTRY. Both
    # halves matter. One branch is usually a gym whose name happens to start
    # with a common word; and matching on the word alone across countries
    # paired California's "Hangar 18" with Britain's "The Climbing Hangar" --
    # unrelated companies sharing one syllable, which would have arrived as an
    # island triple wearing a chain's credentials.
    chain_counts = Counter(
        zip(have.loc[have.chain != '', 'chain'],
            have.loc[have.chain != '', 'country']))
    our_chains = {ck for ck, n in chain_counts.items() if n >= 2}
    our_countries = set(have['country'].dropna())

    cand = av[~av.have].copy()
    cand = cand[(cand.boulder_count >= MIN_BOULDERS)
                & (cand.follower_count >= MIN_FOLLOWERS)]

    # Cluster size among CANDIDATES, for the new-city case: two gyms in one new
    # city can at least be compared with each other.
    cluster = cand.groupby('place')['id'].transform('size')

    cand['same_city'] = cand['place'].isin(our_places)
    cand['same_chain'] = [
        (c, k) in our_chains for c, k in zip(cand['chain'], cand['country'])]
    # A new metro only counts in a country we already pull. Climbers travel
    # domestically far more than internationally, so a big gym in Singapore or
    # London is the island case this file exists to avoid -- it would arrive as
    # its own disconnected component, carrying parameters nothing can
    # constrain, however many followers it has.
    cand['new_city_cluster'] = ((~cand['same_city']) & (cluster >= 2)
                                & cand['country'].isin(our_countries))

    # Weights encode the ordering in the docstring, not a calibration. Overlap
    # dominates because an island gym is worse than no gym: an island scores below every connected gym no matter its size, so the
    # follower bonus can never float one to the top: that is what put two
    # Singapore gyms above Movement Fishtown on the first pass.
    cand['connected'] = (cand['same_city'] | cand['same_chain']
                         | cand['new_city_cluster'])
    cand['score'] = (
        100 * cand['same_city'].astype(int)
        + 60 * cand['same_chain'].astype(int)
        + 25 * cand['new_city_cluster'].astype(int)
        + 200 * cand['connected'].astype(int)
        + cand['follower_count'].clip(upper=1200) / 40
        + cand['boulder_count'].clip(upper=400) / 40
    )

    def why(r):
        bits = []
        if r.same_city:
            bits.append(f'overlap: we already pull {r.place}')
        if r.same_chain:
            bits.append(f'chain: {r.chain}')
        if r.new_city_cluster:
            bits.append(f'new metro cluster: {r.place}')
        if not bits:
            bits.append('standalone')
        return '; '.join(bits)

    cand['why'] = cand.apply(why, axis=1)
    return cand.sort_values('score', ascending=False)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split('\n')[0])
    ap.add_argument('--top', type=int, default=40)
    ap.add_argument('--write', action='store_true',
                    help=f'write {MANIFEST.name} for backfill_new_gyms.py')
    args = ap.parse_args()

    av, roster = load()
    cand = score(av)
    pick = cand.head(args.top)

    print(f'{len(av)} gyms discovered, {int(av.have.sum())} already pulled')
    print(f'{len(cand)} candidates clear the size gates '
          f'(>= {MIN_BOULDERS} boulders, >= {MIN_FOLLOWERS} followers)\n')
    print(f'Proposing {len(pick)}:\n')
    for _, r in pick.iterrows():
        print(f'  {r.id:>5}  {r["name"][:34]:34}  {r.place[:26]:26} '
              f'{int(r.boulder_count):>4}b {int(r.follower_count):>5}f   {r.why}')

    print('\nWhat this buys, by goal')
    print(f'  overlap (city we already pull) : {int(pick.same_city.sum())}')
    print(f'  chain coverage                 : {int(pick.same_chain.sum())}')
    print(f'  new metro clusters             : {int(pick.new_city_cluster.sum())}')
    print(f'  islands (no link to the network): {int((~pick.connected).sum())}'
          '   <- should be 0')
    new_places = sorted(set(pick.loc[~pick.same_city, 'place']))
    print(f'  new metros ({len(new_places)}): {", ".join(new_places[:8])}'
          f'{" ..." if len(new_places) > 8 else ""}')
    print(f'  roster would go {len(roster)} -> {len(roster) + len(pick)}')

    if args.write:
        MANIFEST.parent.mkdir(parents=True, exist_ok=True)
        MANIFEST.write_text(json.dumps({
            'created_for': 'expand the pull roster: overlap, chains, cities',
            'min_boulders': MIN_BOULDERS, 'min_followers': MIN_FOLLOWERS,
            'gyms': [
                {'gym_id': r.id, 'name': r['name'], 'place': r.place,
                 'why': r.why, 'boulder_count': int(r.boulder_count),
                 'follower_count': int(r.follower_count)}
                for _, r in pick.iterrows()
            ],
        }, indent=2))
        print(f'\nwrote {MANIFEST.relative_to(ROOT)} ({len(pick)} gyms)')
        print('next: python scripts/backfill_new_gyms.py --manifest '
              f'{MANIFEST.relative_to(ROOT)} --dry-run')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
