"""Store-mirrored checkpointing so a Spot eviction resumes instead of recomputing.

Generic block-checkpoint helper plus an xgboost-aware ``fit`` (imported lazily) that
trains in ``every``-round blocks via ``xgb_model=`` continuation, saving the
cumulative booster to ``ckpt/<key>/round-<k>.json`` after each block. On (re)start
it restores the latest checkpoint and continues — a worker that dies mid-fit resumes
where it left off when the task is redelivered.
"""

from __future__ import annotations

import os
import re

from .store import Store


class Checkpoint:
    def __init__(self, store: Store, key: str, work_dir: str = "/tmp/gleanflow/ckpt"):
        self.store = store
        self.key = key
        self.dir = os.path.join(work_dir, key)
        os.makedirs(self.dir, exist_ok=True)

    def _ckpt_key(self, rounds: int) -> str:
        return f"ckpt/{self.key}/round-{rounds}.json"

    def restore(self) -> tuple[str | None, int]:
        """Return ``(local_booster_path, rounds_done)`` of the latest checkpoint."""
        best, best_r = None, 0
        for k in self.store.list(f"ckpt/{self.key}/"):
            m = re.search(r"round-(\d+)\.json$", k)
            if m and int(m.group(1)) > best_r:
                best, best_r = k, int(m.group(1))
        if best is None:
            return None, 0
        local = os.path.join(self.dir, os.path.basename(best))
        return self.store.get_file(best, local), best_r

    def save(self, local_path: str, rounds: int) -> None:
        self.store.put_file(self._ckpt_key(rounds), local_path)

    def fit(self, clf, X, y, rounds: int, every: int = 50, **fit_kw):
        """xgboost block-continuation fit with checkpointing. Returns the fitted clf."""
        booster, done = self.restore()
        if done:
            print(f"[ckpt] {self.key}: resume from round {done}/{rounds}", flush=True)
        if done >= rounds and booster:
            clf.load_model(booster)
            return clf
        while done < rounds:
            block = min(every, rounds - done)
            clf.set_params(n_estimators=block)
            clf.fit(X, y, xgb_model=booster, **fit_kw)
            done += block
            local = os.path.join(self.dir, f"round-{done}.json")
            clf.save_model(local)
            self.save(local, done)
            booster = local
        return clf
