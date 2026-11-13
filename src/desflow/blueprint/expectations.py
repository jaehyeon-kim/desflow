"""Writes the measured statistics into the `expect` values of a file's `tests`.

`desflow test --fix` uses it. It edits the text of a value on its own line, so the
comments, the order of keys and the layout of the rest of the file stay as written.
"""

import math
import re
from typing import Dict, List, Mapping, Union

from desflow.blueprint.loader import BlueprintError, SourceMap
from desflow.blueprint.models import BlueprintTest, ExpectedWithin

Number = Union[int, float]


def number_text(value: Number) -> str:
    """A number as YAML writes it: an integer as it is, a float as its shortest text
    that reads back as the same float.

    Args:
        value: The number.

    Returns:
        str: The text.
    """
    if isinstance(value, int):
        return str(value)
    if math.isinf(value):
        return ".inf" if value > 0 else "-.inf"
    text = repr(value)
    # The loader reads 1e-05 and 1e+16 as text; YAML needs a dot in the mantissa
    mantissa, marker, exponent = text.partition("e")
    if marker and "." not in mantissa:
        text = f"{mantissa}.0e{exponent}"
    return text


def holds(expected: Union[Number, ExpectedWithin], measured: Number) -> bool:
    """Whether a measured statistic meets what `expect` states: equal for a number,
    within the tolerance for `{value, within}`.

    Args:
        expected: The number, or the `{value, within}` mapping, `expect` holds.
        measured: The statistic the run published.

    Returns:
        bool: True when the statistic meets the expectation.
    """
    if isinstance(expected, ExpectedWithin):
        return abs(measured - expected.value) <= expected.within
    return measured == expected


def fix_expectations(
    text: str,
    source: SourceMap,
    tests: List[BlueprintTest],
    measured: List[Mapping[str, Number]],
) -> str:
    """Returns `text` with each test's `expect` values replaced by the measured ones.

    A value that already holds is left as written. A test with no `expect` gets one
    key for each measured statistic that is not NaN, in name order, and is left as
    written when the run published none. A statistic that a test expects and the run
    did not publish keeps its text.

    Args:
        text: The file's text.
        source: The lines of the file's keys, from reading the same text.
        tests: The file's tests, in order.
        measured: For each test, the statistics its run published, by name.

    Returns:
        str: The new text.

    Raises:
        BlueprintError: If a test is written as a flow mapping on one line, or a line
            is not where the source map puts it, so the edit cannot be made safely.
    """
    lines = text.splitlines(keepends=True)
    inserts: Dict[int, List[str]] = {}
    owners = _tests_by_expect_line(source, tests)
    for index, test in enumerate(tests):
        found = measured[index]
        if test.expect:
            for name, expected in test.expect.items():
                if (
                    name in found
                    and not math.isnan(found[name])
                    and not holds(expected, found[name])
                ):
                    at = _where(source, "tests", index, "expect", name)
                    _refuse_a_shared_expect(source, tests, owners[at], at)
                    _replace(lines, source, index, name, expected, found[name])
        else:
            _add_expect(lines, source, index, found, inserts)
    # Inserted last and from the bottom, so the line numbers above stay right
    for after in sorted(inserts, reverse=True):
        lines[after + 1 : after + 1] = inserts[after]
    return "".join(lines)


def _tests_by_expect_line(
    source: SourceMap, tests: List[BlueprintTest]
) -> Dict[int, List[int]]:
    """For each line that holds a key of an `expect`, the indexes of the tests that
    read it. A line is read by two tests when they share an `expect` through an
    anchor and an alias."""
    owners: Dict[int, List[int]] = {}
    for index, test in enumerate(tests):
        for name in test.expect or {}:
            readers = owners.setdefault(
                _where(source, "tests", index, "expect", name), []
            )
            if index not in readers:
                readers.append(index)
    return owners


def _refuse_a_shared_expect(
    source: SourceMap, tests: List[BlueprintTest], owners: List[int], at: int
) -> None:
    """Raises when more than one test reads the line `at`, as writing the value of one
    test there would change what the others expect."""
    if len(owners) > 1:
        names = ", ".join(f"'{tests[index].name}'" for index in owners)
        raise BlueprintError(
            f"{source.path}:{at + 1}: --fix cannot change this expect, which tests "
            f"{names} share through an anchor; write a separate expect for each test "
            f"and run again"
        )


def _where(source: SourceMap, *location: Union[str, int]) -> int:
    """The index in the list of lines of the line at `location`."""
    return source.line(location) - 1


def _replace(
    lines: List[str],
    source: SourceMap,
    index: int,
    name: str,
    expected: Union[Number, ExpectedWithin],
    value: Number,
) -> None:
    """Replaces the number written for `name` in test `index` with `value`."""
    new = number_text(value)
    at = _where(source, "tests", index, "expect", name)
    if isinstance(expected, ExpectedWithin) and "{" not in _after_key(lines[at], name):
        # {value: ..., within: ...} written as a block: the value has its own line
        at = _where(source, "tests", index, "expect", name, "value")
        pattern = r"^(\s*value\s*:\s*)([^\s#,}]+)"
    elif isinstance(expected, ExpectedWithin):
        # {value: ..., within: ...} on the line of its key, which may share the line
        # with the `{value, within}` of other keys: the value of this key is the first
        # `value:` after the key, inside its braces
        pattern = (
            rf"((?<![\w.])[\"']?{re.escape(name)}[\"']?\s*:\s*\{{[^}}]*?"
            r"\bvalue\s*:\s*)([^\s#,}]+)"
        )
    else:
        pattern = rf"^(\s*(?:-\s+)?[\"']?{re.escape(name)}[\"']?\s*:\s*)([^\s#,}}]+)"

    def swap(found: "re.Match[str]") -> str:
        return found.group(1) + new

    changed, count = re.subn(pattern, swap, lines[at], count=1)
    if not count and not isinstance(expected, ExpectedWithin):
        # `expect: {name: number}` written as a flow mapping on one line
        flow = rf"([{{,]\s*[\"']?{re.escape(name)}[\"']?\s*:\s*)([^\s#,}}]+)"
        changed, count = re.subn(flow, swap, lines[at], count=1)
    if not count:
        raise BlueprintError(
            f"{source.path}:{at + 1}: --fix cannot find the value of {name} on this "
            f"line; write it as `{name}: <number>` and run again"
        )
    lines[at] = changed


def _after_key(line: str, name: str) -> str:
    """What follows the key `name` on its line."""
    return line.partition(":")[2] if name in line else ""


def _add_expect(
    lines: List[str],
    source: SourceMap,
    index: int,
    found: Mapping[str, Number],
    inserts: Dict[int, List[str]],
) -> None:
    """Writes an `expect` block for a test that has none, or an empty one."""
    start = _where(source, "tests", index)
    dash = re.match(r"^(\s*)-\s+", lines[start])
    if dash is None:
        raise BlueprintError(
            f"{source.path}:{start + 1}: --fix writes expect into a test written as a "
            f"block mapping that starts with `- `; write this test that way"
        )
    indent = " " * len(dash.group(0))
    # A statistic that is not a number can never equal an expected value
    block = [f"{indent}expect:\n"] + [
        f"{indent}  {name}: {number_text(found[name])}\n"
        for name in sorted(found)
        if not math.isnan(found[name])
    ]
    if len(block) == 1:
        # A bare `expect:` reads as null, which the next load refuses
        return
    own = _where(source, "tests", index, "expect")
    if re.match(r"^\s*(?:-\s+)?expect\s*:", lines[own]):
        # An expect with no keys: replaced by the block, after the dash when `expect`
        # is the first key of the test
        empty = re.match(
            r"^(\s*(?:-\s+)?)expect\s*:\s*(\{\s*\})?\s*(#.*)?$", lines[own].rstrip("\n")
        )
        if not empty:
            raise BlueprintError(
                f"{source.path}:{own + 1}: --fix writes the keys of an empty expect "
                f"only when it is written `expect: {{}}`, and this one is not; write "
                f"it that way and run again"
            )
        lines[own] = empty.group(1) + block[0].lstrip()
        inserts.setdefault(own, []).extend(block[1:])
        return
    end = start
    for number in range(start + 1, len(lines)):
        line = lines[number]
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if len(line) - len(line.lstrip()) < len(indent):
            break
        end = number
    inserts.setdefault(end, []).extend(block)
    if not lines[end].endswith("\n"):
        lines[end] += "\n"
