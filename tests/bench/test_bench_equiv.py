from argparse import Namespace

from engine.bench import equiv


def test_missing_lane_fails_equivalence(tmp_path, monkeypatch):
    args = Namespace(step="S2K", root=tmp_path, out=tmp_path, lanes=("etl", "elt"))

    def make_spec(args, lane, dataset, name):
        return Namespace(name=name, export_root=None)

    monkeypatch.setattr(equiv, "run_lane_bench", lambda spec: {"status": "aborted"})
    assert equiv.lanes_stage(args, make_spec) == 1
    assert (tmp_path / "equivalence" / "lanes-S2K.json").exists()
