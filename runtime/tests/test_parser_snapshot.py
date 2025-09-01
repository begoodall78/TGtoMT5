import json, pathlib
# from app.parser_core import parse  # TODO: wire in Phase 1

def parse(text: str):
    """TEMP stub to keep test file importable until Phase 1 wiring."""
    return {"_stub": True, "text": text[:40]}

SNAP_DIR = pathlib.Path("runtime/snapshots")
CORPUS = pathlib.Path("runtime/test_corpus/messages.jsonl")

def test_corpus_against_snapshots():
    assert CORPUS.exists(), "Missing test corpus file"
    with CORPUS.open("r", encoding="utf-8") as f:
        for line in f:
            msg = json.loads(line)
            out = parse(msg["text"])  # Phase 1: swap to real parser
            snap_file = SNAP_DIR / f"{msg['id']}.json"
            assert snap_file.exists(), f"Missing snapshot for {msg['id']}"
            expected = json.loads(snap_file.read_text(encoding="utf-8"))
            assert out == expected
