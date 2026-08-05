"""The append-only journal shared by the audit trail and the fill journal.

The watermark counts LINES, not records, and every test here is ultimately
about that: a caller that drops an unreadable line must still be able to move
past it, or it re-reads the same bad byte forever.
"""

from nakagai_edge.edge.journal import Journal


def test_pending_returns_what_has_not_shipped_and_stops_there(tmp_path):
    j = Journal(tmp_path / "j.jsonl")
    for i in range(3):
        j.append({"n": i})
    assert [r["n"] for r in j.pending()] == [0, 1, 2]
    j.mark_shipped(2)
    assert [r["n"] for r in j.pending()] == [2]
    j.mark_shipped(1)
    assert j.pending() == []


def test_an_unreadable_line_is_none_rather_than_an_empty_record(tmp_path):
    # None, not {}: the caller has to decide what a lost line means, and an
    # empty record would let it skip that decision without noticing. The audit
    # trail ships a `corrupt` marker; the fill journal drops it.
    path = tmp_path / "j.jsonl"
    j = Journal(path)
    j.append({"n": 0})
    with path.open("a") as f:
        f.write("{not json\n")
    j.append({"n": 2})
    assert [r if r is None else r["n"] for r in j.pending()] == [0, None, 2]


def test_a_json_scalar_counts_as_unreadable(tmp_path):
    # `123` and `"x"` parse cleanly and are not records. Letting them through
    # hands the caller something it will subscript.
    path = tmp_path / "j.jsonl"
    j = Journal(path)
    with path.open("a") as f:
        f.write("123\n")
    assert j.pending() == [None]


def test_marking_shipped_covers_the_bad_line_too(tmp_path):
    # The regression this file exists for: mark by lines consumed, and the bad
    # line is behind the watermark. Mark by records kept, and it never is.
    path = tmp_path / "j.jsonl"
    j = Journal(path)
    with path.open("a") as f:
        f.write("{not json\n")
    j.append({"n": 1})
    batch = j.pending()
    j.mark_shipped(len(batch))
    assert j.pending() == []


def test_a_missing_watermark_reships_rather_than_skips(tmp_path):
    # Telling the platform something twice is recoverable. Never telling it is
    # not, so an unreadable watermark reads as zero.
    path = tmp_path / "j.jsonl"
    j = Journal(path)
    j.append({"n": 0})
    j.mark_shipped(1)
    path.with_suffix(".shipped").write_text("not a number")
    assert [r["n"] for r in j.pending()] == [0]


def test_records_ignores_the_watermark(tmp_path):
    # The fill journal rebuilds its seen-set from this at startup. An order
    # already shipped must not be journaled again just because the platform
    # confirmed it.
    j = Journal(tmp_path / "j.jsonl")
    j.append({"n": 0})
    j.append({"n": 1})
    j.mark_shipped(2)
    assert [r["n"] for r in j.records()] == [0, 1]


def test_pending_on_a_journal_that_was_never_written(tmp_path):
    assert Journal(tmp_path / "absent.jsonl").pending() == []
    assert list(Journal(tmp_path / "absent.jsonl").records()) == []
