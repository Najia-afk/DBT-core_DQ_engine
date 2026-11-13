import pytest

from engine.lanes.elt.__main__ import _parser as elt_parser
from engine.lanes.etl.cli import build_parser as etl_parser
from engine.lanes.graph.cli import _parser as graph_parser
from engine.runtime.executor import build_argv


@pytest.mark.parametrize("parser", [etl_parser, elt_parser, graph_parser])
@pytest.mark.parametrize("command", ["init", "ingest", "reconcile", "export", "reset"])
def test_actual_lane_parser_accepts_runtime_command(parser, command):
    argv = build_argv(
        ["{python}", "-m", "engine.lanes.{lane}"],
        python="python",
        lane="test",
        command=command,
        config="referential.yml",
        run_id="run-1",
        source="crm" if command == "ingest" else None,
        out="export-dir" if command == "export" else None,
    )
    parsed = parser().parse_args(argv[3:])
    assert parsed.command == command
    assert parsed.run_id == "run-1"
