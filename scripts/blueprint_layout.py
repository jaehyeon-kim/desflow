"""Write desflow YAML blueprints in one layout, or check that they are in it.

Usage:
    python scripts/blueprint_layout.py [--check] FILE...

The layout, which every collection follows except the document's root mapping (always
a block):

1. A mapping whose values are all scalars, aliases, sequences of scalars or sequences
   of sequences of scalars is written inline, `{a: 1, b: x}`. A sequence of scalars
   is written inline, `[1, 2]`, and so is a sequence of sequences of scalars and a
   sequence that mixes scalars with collections, such as a `[priority, item]` pair.
2. A mapping that holds a mapping, or a sequence that holds one, opens into one key
   per line. A sequence whose items are all collections, at least one of them a
   mapping or holding one, opens into one item per line. Every collection inside an
   inline sequence stays inline, whatever it holds, because a block cannot be
   written inside an inline sequence.
3. An inline collection whose line would pass 88 characters opens, outermost first,
   except a sequence of scalars, which is wrapped after a comma with its further
   lines starting under its first item.
4. A block collection stays a block when a comment is on its own lines, from its
   first line to the line of its last scalar, or when a scalar without quotes inside
   it holds `,`, `[`, `]`, `{` or `}`, such as `${SEED:-42}`, which an inline
   collection would have to quote. The blank lines and comments after a collection
   are outside it: they move to after the collection when it becomes inline.
5. Each top-level key after the first has one blank line before it, or before the
   comments at column 0 above it.
6. A full-line comment over 88 characters is wrapped at the same indentation, and
   its last words join the next line of the same paragraph. A comment whose text
   starts after two or more spaces, such as a command, is kept as written. An
   end-of-line comment on a line over 88 characters moves onto its own lines above
   that line, and one on a sequence item that opens moves above the item.
7. A scalar value still over 88 characters is folded at single spaces onto lines
   indented 2 more than its key or `-`; YAML reads each line break as one space. A
   value with no space to fold at moves onto the next line when it fits there.

Everything else is kept as ruamel.yaml's round-trip mode reads it: comments, key
order, anchors and aliases, tags, quotes, and the text of every scalar. Scalars are
read with YAML 1.2 rules. Under YAML 1.1 rules ruamel.yaml reads `on`, `y` and `n`
as booleans and writes them back as `true` and `false`, and adds a `%YAML 1.1`
line; under 1.2 they are strings, written back as they were.

The result is composed with PyYAML, as desflow's loader reads it, and compared with
the original: tags, scalar values and quoting, shared nodes and anchor names. A file
whose result differs is reported and left as it is.

`--check` writes nothing, prints each file that would change, and exits 1 if any
would.
"""

import argparse
import io
import re
import sys
import textwrap
from pathlib import Path
from typing import Any, NamedTuple, TypeAlias

import yaml as pyyaml
from ruamel.yaml import YAML
from ruamel.yaml.comments import CommentedMap, CommentedSeq, TaggedScalar
from ruamel.yaml.error import CommentMark
from ruamel.yaml.scalarstring import ScalarString
from ruamel.yaml.tokens import CommentToken

MAX_WIDTH = 88
# ruamel.yaml never breaks a line at this width; the rules below do
UNLIMITED_WIDTH = 1_000_000
# Each pass opens the collections of the lines that are still too long, so the passes
# end long before this many
MAX_OPEN_PASSES = 100
FLOW_INDICATORS = frozenset(",[]{}")

Collection: TypeAlias = CommentedMap | CommentedSeq


def _represent_null(representer: Any, data: None) -> Any:
    """Write None as `null`; ruamel.yaml writes it as an empty value in a mapping."""
    return representer.represent_scalar("tag:yaml.org,2002:null", "null")


def _make_yaml() -> YAML:
    """Build the ruamel.yaml object that reads and writes the blueprints.

    Returns:
        A round-trip `YAML` with YAML 1.2 scalars, quotes kept, desflow's
        indentation, the braces of `[{fire: x}]` kept and `null` written in full.
    """
    yaml = YAML()
    yaml.preserve_quotes = True
    yaml.width = UNLIMITED_WIDTH
    yaml.indent(mapping=2, sequence=4, offset=2)
    yaml.brace_single_entry_mapping_in_flow_sequence = True
    yaml.representer.add_representer(type(None), _represent_null)
    return yaml


def _entries(node: Any) -> list[tuple[Any, Any]]:
    """The `(key, value)` pairs of a mapping, without the keys a merge key brings in,
    or the `(index, item)` pairs of a sequence."""
    if isinstance(node, CommentedMap):
        return list(node.non_merged_items())
    if isinstance(node, CommentedSeq):
        return list(enumerate(node))
    return []


def _children(node: Any) -> list[Any]:
    """The values of a mapping, or the items of a sequence."""
    return [value for _, value in _entries(node)]


def _is_collection(node: Any, aliases: set[int]) -> bool:
    """True for a mapping or a sequence written in place. An alias is written as
    `*name`, so it counts as a scalar."""
    return isinstance(node, (CommentedMap, CommentedSeq)) and id(node) not in aliases


def _holds_mapping(node: Any, aliases: set[int]) -> bool:
    """True for a mapping, or a sequence with a mapping somewhere inside it."""
    if not _is_collection(node, aliases):
        return False
    if isinstance(node, CommentedMap):
        return True
    return any(_holds_mapping(item, aliases) for item in node)


def _comment_tokens(parts: Any) -> list[CommentToken]:
    """The comment tokens in a slot of `ca.items`, which holds a token, a list of
    tokens, or None."""
    found = []
    for part in parts or []:
        if isinstance(part, list):
            found.extend(_comment_tokens(part))
        elif part is not None:
            found.append(part)
    return found


def _holds_comment(node: Collection, aliases: set[int], ends: bool = True) -> bool:
    """Rule 4: whether a `#` comment is on the collection's own lines, from its first
    line to the line of its last scalar, or on those of a collection inside it.

    The blank lines and comment lines that follow the collection are stored with its
    last key or item, or in the end of a sequence. They are outside the collection, so
    they do not count.

    Args:
        node: The collection.
        aliases: The ids of the collections written as aliases.
        ends: Whether the collection ends the collection that holds it, so that the
            lines after its last entry are not inside the outer one.
    """
    comments = node.ca
    kept = _comment_tokens(comments.comment)
    if not ends:
        kept += _comment_tokens(comments.end)
    if any("#" in token.value for token in kept):
        return True
    entries = _entries(node)
    trailing_slot = 2 if isinstance(node, CommentedMap) else 0
    for position, (key, child) in enumerate(entries):
        is_last = ends and position == len(entries) - 1
        for slot, part in enumerate(comments.items.get(key) or []):
            for token in _comment_tokens([part]):
                text = token.value
                if is_last and slot == trailing_slot:
                    text = text.partition("\n")[0]
                if "#" in text:
                    return True
        if _is_collection(child, aliases) and _holds_comment(child, aliases, is_last):
            return True
    return False


def _plain_text(value: Any) -> str | None:
    """The text of a scalar written without quotes, or None."""
    if isinstance(value, TaggedScalar):
        return str(value.value) if value.style is None else None
    if isinstance(value, str) and not isinstance(value, ScalarString):
        return value
    return None


def _plain_scalars_fit_flow(node: Collection, aliases: set[int]) -> bool:
    """False when a key or value without quotes holds `,`, `[`, `]`, `{` or `}`, such
    as `${SEED:-42}`. Inside an inline collection ruamel.yaml would have to quote
    it, and desflow keeps a quoted value a string after `${VAR}` substitution."""
    keys = list(node.keys()) if isinstance(node, CommentedMap) else []
    for value in keys + _children(node):
        if _is_collection(value, aliases):
            if not _plain_scalars_fit_flow(value, aliases):
                return False
            continue
        text = _plain_text(value)
        if text is not None and FLOW_INDICATORS & set(text):
            return False
    return True


def _find_aliases(node: Any, seen: set[int], aliases: set[int]) -> None:
    """Fill `aliases` with the ids of collections met a second time, which are written
    as `*name` where they appear again."""
    if not isinstance(node, (CommentedMap, CommentedSeq)):
        return
    if id(node) in seen:
        aliases.add(id(node))
        return
    seen.add(id(node))
    for child in _children(node):
        _find_aliases(child, seen, aliases)


def _must_open(node: Collection, aliases: set[int]) -> bool:
    """Rule 2: whether a collection with children opens into one entry per line."""
    children = _children(node)
    if isinstance(node, CommentedMap):
        return any(_holds_mapping(child, aliases) for child in children)
    return all(_is_collection(child, aliases) for child in children) and any(
        _holds_mapping(child, aliases) for child in children
    )


def _choose_styles(
    node: Any,
    aliases: set[int],
    done: set[int],
    moves: list[tuple[tuple[Any, ...], list[str]]],
    path: tuple[Any, ...] | None = None,
) -> None:
    """Set the flow or block style of each collection by rules 1, 2 and 4, innermost
    first.

    Args:
        node: The collection.
        aliases: The ids of the collections written as aliases.
        done: The ids of the collections already handled.
        moves: Filled with the blank lines and comment lines that a collection leaves
            behind when it becomes inline, with the keys and indexes that lead to it.
        path: The keys and indexes that lead to `node`, or None for the root.
    """
    if not isinstance(node, (CommentedMap, CommentedSeq)) or id(node) in done:
        return
    done.add(id(node))
    here = () if path is None else path
    for key, child in _entries(node):
        _choose_styles(child, aliases, done, moves, here + (key,))
    if path is None:
        node.fa.set_block_style()  # the document's root mapping
        return
    if not _children(node):
        return  # `{}` and `[]` stay as written
    if _must_open(node, aliases):
        node.fa.set_block_style()
        return
    written_inline = node.fa.flow_style() is True
    stays_block = _holds_comment(node, aliases) or not _plain_scalars_fit_flow(
        node, aliases
    )
    if stays_block and not written_inline:
        return
    if not written_inline:
        lines = _take_comment_lines(node, aliases)
        if lines:
            moves.append((path, lines))
    node.fa.set_flow_style()


def _is_block(node: Any) -> bool:
    """True for a non-empty collection written as a block."""
    return (
        isinstance(node, (CommentedMap, CommentedSeq))
        and len(node) > 0
        and node.fa.flow_style() is not True
    )


class _CommentSlot(NamedTuple):
    """A place in `ca.items` that holds the comment lines after a collection."""

    owner: Collection
    key: Any
    slot: int


def _slot_after(parent: Collection, key: Any) -> _CommentSlot:
    """The slot that holds the lines after the entry `key` of `parent`."""
    return _CommentSlot(parent, key, 2 if isinstance(parent, CommentedMap) else 0)


def _last_chain(node: Collection, seen: set[int]) -> list[Collection]:
    """The block collection `node` and the block collections that end it, each the
    last entry of the one before."""
    chain = [node]
    while True:
        seen = seen | {id(chain[-1])}
        child = _entries(chain[-1])[-1][1]
        if not _is_block(child) or id(child) in seen:
            return chain
        chain.append(child)


def _last_comment_slot(node: Collection, seen: set[int]) -> _CommentSlot:
    """Find where ruamel.yaml keeps the comment lines that follow a block collection:
    the slot of its last key or item, in the deepest block collection that ends it."""
    deepest = _last_chain(node, seen)[-1]
    return _slot_after(deepest, _entries(deepest)[-1][0])


def _put_comment(place: _CommentSlot, text: str) -> None:
    """Add the lines `text`, which come after the first line of a comment slot, to a
    slot."""
    owner, key, slot = place
    empty_entry = (
        [None, None, None, None] if isinstance(owner, CommentedMap) else [None, None]
    )
    entry = owner.ca.items.setdefault(key, empty_entry)
    if entry[slot] is None:
        entry[slot] = CommentToken("\n" + text, CommentMark(0))
    else:
        entry[slot].value = entry[slot].value + text


def _add_comment_after(node: Collection, text: str, seen: set[int]) -> None:
    """Put comment lines after the last key or item of a block collection."""
    _put_comment(_last_comment_slot(node, seen), text)


def _line_of(token: CommentToken) -> str:
    """The line that a token of a per-line list holds, with its indentation."""
    text = token.value.rstrip("\n")
    return " " * token.start_mark.column + text if text else ""


def _take_comment_lines(node: Collection, aliases: set[int]) -> list[str]:
    """Remove the blank lines and comment lines stored in a block collection, and in
    the collections inside it, before it becomes inline: an inline collection cannot
    hold them.

    The lines after the collection come last, as written. They are the only ones that
    can hold a comment, because `_holds_comment` is false for the collection. They are
    in the slot after its last entry, and in the end of a sequence that ends it.

    Returns:
        The lines, without line breaks.
    """
    chain = _last_chain(node, set())
    deepest = chain[-1]
    key = _entries(deepest)[-1][0]
    entry = deepest.ca.items.get(key)
    trailing: list[str] = []
    slot = _slot_after(deepest, key).slot
    if entry and entry[slot] is not None:
        _, _, rest = entry[slot].value.partition("\n")
        trailing = rest.removesuffix("\n").split("\n") if rest else []
        entry[slot] = None
    for collection in reversed(chain):
        trailing += [_line_of(token) for token in _comment_tokens(collection.ca.end)]
        collection.ca.end = []
    return [""] * _drop_blank_lines(node, aliases) + trailing


def _blank_lines_in(part: Any) -> int:
    """How many blank lines a comment slot holds. A list has one token for each line.
    A token starts with the line break that ends its own line, and the lines after
    that one follow."""
    if isinstance(part, list):
        return len(part)
    if part is None:
        return 0
    return max(part.value.count("\n") - 1, 0)


def _drop_blank_lines(node: Collection, aliases: set[int]) -> int:
    """Remove the comment slots of a collection, and of those inside it, which hold
    blank lines only.

    Returns:
        How many blank lines they held.
    """
    comments = node.ca
    blanks = sum(_blank_lines_in(part) for part in comments.comment or [])
    blanks += _blank_lines_in(getattr(comments, "end", None))
    comments.comment = None
    comments.end = []
    for key, child in _entries(node):
        blanks += sum(_blank_lines_in(part) for part in comments.items.pop(key, []))
        if _is_collection(child, aliases):
            blanks += _drop_blank_lines(child, aliases)
    return blanks


def _lift_item_comments(node: Any, column: int, seen: set[int]) -> None:
    """Move the end-of-line comment of a sequence item that is written as a block onto
    its own line above the item. ruamel.yaml would write it after the `-`, with the
    item's content on the lines below. Comment lines that followed the comment stay
    after the item."""
    if not _is_block(node) or id(node) in seen:
        return
    seen = seen | {id(node)}
    if isinstance(node, CommentedMap):
        # A nested mapping's keys, and a nested sequence's `-`, are 2 columns in
        for _, value in node.non_merged_items():
            _lift_item_comments(value, column + 2, seen)
        return
    for item in node:
        _lift_item_comments(item, column + 2, seen)
    for index, item in enumerate(node):
        entry = node.ca.items.get(index)
        token = entry[0] if entry else None
        if not (_is_block(item) and token is not None and token.value.startswith("#")):
            continue
        first, _, rest = token.value.partition("\n")
        entry[0] = None
        if not any(entry):
            del node.ca.items[index]
        above = node.ca.items.setdefault(index, [None, None])
        if len(above) < 2:
            above.extend([None] * (2 - len(above)))
        if above[1] is None:
            above[1] = []
        above[1].append(CommentToken(first + "\n", CommentMark(column)))
        if rest:
            _add_comment_after(item, rest, seen)


def _dump(yaml: YAML, data: Any) -> str:
    """Write the document as text, with the comments of opened items lifted."""
    _lift_item_comments(data, 0, set())
    out = io.StringIO()
    yaml.dump(data, out)
    return out.getvalue()


def _node_at(root: Any, path: tuple[Any, ...]) -> Any:
    """The PyYAML node that the keys and indexes of `path` lead to.

    Raises:
        ValueError: If a key is not there.
    """
    node = root
    for key in path:
        if isinstance(node, pyyaml.MappingNode):
            matches = []
            for key_node, value_node in node.value:
                if getattr(key_node, "value", None) == str(key):
                    matches.append(value_node)
            if len(matches) != 1:
                raise ValueError(f"cannot find the key {key!r} again; not changed")
            node = matches[0]
        else:
            node = node.value[key]
    return node


def _restore_lines(text: str, moves: list[tuple[tuple[Any, ...], list[str]]]) -> str:
    """Put the blank lines and comment lines that collections left behind when they
    became inline on the lines after each collection."""
    if not moves:
        return text
    root = pyyaml.compose(text, Loader=pyyaml.SafeLoader)
    after: dict[int, list[str]] = {}
    for path, lines in moves:
        end_line = _node_at(root, path).end_mark.line
        after.setdefault(end_line, []).extend(lines)
    rows = text.split("\n")
    for line in sorted(after, reverse=True):
        rows[line + 1 : line + 1] = after[line]
    return "\n".join(rows)


def _open_long_flows(node: Any, long_lines: set[int], seen: set[int]) -> int:
    """Set block style on each outermost inline collection that starts on a long
    line, except a sequence of scalars, which rule 3 wraps instead.

    Returns:
        How many collections it opened.
    """
    if not isinstance(node, (CommentedMap, CommentedSeq)) or id(node) in seen:
        return 0
    seen.add(id(node))
    if node.fa.flow_style() is True and node.lc.line in long_lines and len(node):
        holds_collection = any(
            isinstance(item, (CommentedMap, CommentedSeq)) for item in node
        )
        if isinstance(node, CommentedSeq) and not holds_collection:
            return 0
        node.fa.set_block_style()
        return 1
    return sum(_open_long_flows(child, long_lines, seen) for child in _children(node))


def _scalar_spans(text: str) -> list[tuple[int, int]]:
    """The start and end index of every scalar in the text."""
    spans = []
    for token in pyyaml.scan(text, Loader=pyyaml.SafeLoader):
        if isinstance(token, pyyaml.ScalarToken):
            spans.append((token.start_mark.index, token.end_mark.index))
    return spans


def _comment_column(
    row: str, row_start: int, scalars: list[tuple[int, int]]
) -> int | None:
    """Find the comment on a line.

    A `#` starts a comment at the start of a line or after a space, outside any scalar.

    Args:
        row: The text of the line.
        row_start: The index in the whole text where the line starts.
        scalars: The `(start, end)` indexes of the scalars of the whole text.

    Returns:
        The column of the `#`, or None when the line has no comment.
    """
    for column, char in enumerate(row):
        if char == "#" and (column == 0 or row[column - 1] in " \t"):
            index = row_start + column
            if not any(start <= index < end for start, end in scalars):
                return column
    return None


def _long_content_lines(text: str) -> set[int]:
    """The numbers of the lines whose content, without an end-of-line comment, passes
    MAX_WIDTH."""
    scalars = _scalar_spans(text)
    found = set()
    row_start = 0
    for number, row in enumerate(text.split("\n")):
        column = _comment_column(row, row_start, scalars)
        row_start += len(row) + 1
        content = row if column is None else row[:column].rstrip()
        if len(content) > MAX_WIDTH:
            found.add(number)
    return found


def _wrap_comment(indent: str, comment: str) -> list[str]:
    """The comment, starting at its `#`, as lines of at most MAX_WIDTH characters
    where its words allow."""
    body = comment[1:]
    space = " " if body.startswith(" ") else ""
    parts = textwrap.wrap(
        body.strip(),
        width=max(MAX_WIDTH - len(indent) - 1 - len(space), 20),
        break_long_words=False,
        break_on_hyphens=False,
    ) or [""]
    return [f"{indent}#{space}{part}".rstrip() for part in parts]


_LIST_ITEM = re.compile(r"^(?:[-*] |\d+[.)] |--8<--)")


def _is_preformatted(body: str) -> bool:
    """True for a comment whose text starts after two or more spaces, such as a command
    shown as `#   desflow fit ...`. Its lines are kept as written."""
    return body.startswith("  ")


def _continues_paragraph(body: str) -> bool:
    """True when a comment line can take words carried from the line above: it is not
    empty, preformatted, a snippet marker or a list item."""
    return (
        bool(body.strip())
        and not _is_preformatted(body)
        and not _LIST_ITEM.match(body.strip())
    )


def _wrap_comments(text: str) -> str:
    """Apply rule 6. A full-line comment over MAX_WIDTH is wrapped. The words of its
    last wrapped line join the next comment line when that line continues the same
    paragraph at the same indentation, and that line is wrapped in turn, so a
    paragraph is not left with a line of one or two words."""
    scalars = _scalar_spans(text)
    out: list[str] = []
    carry: tuple[str, str] | None = None  # the indentation and the words carried
    row_start = 0
    for row in text.split("\n"):
        column = _comment_column(row, row_start, scalars)
        row_start += len(row) + 1
        is_full_line = column is not None and not row[:column].strip()
        if carry is not None:
            indent, words = carry
            carry = None
            body = row[column + 1 :] if column is not None and is_full_line else ""
            if is_full_line and row[:column] == indent and _continues_paragraph(body):
                row = f"{indent}# {words} {body.strip()}"
            else:
                out.append(f"{indent}# {words}")
        if column is None or len(row) <= MAX_WIDTH:
            out.append(row)
            continue
        if is_full_line and _is_preformatted(row[column + 1 :]):
            out.append(row)  # shortened by hand, if at all
            continue
        content = row[:column].rstrip()
        indent = row[: len(row) - len(row.lstrip())]
        parts = _wrap_comment(
            indent, row[len(indent) :] if is_full_line else row[column:]
        )
        if is_full_line:
            out.extend(parts[:-1])
            carry = (indent, parts[-1][len(indent) + 1 :].strip())
        else:
            out.extend(parts)  # an end-of-line comment moves above its line
            out.append(content)
    if carry is not None:
        out.append(f"{carry[0]}# {carry[1]}")
    return "\n".join(out)


def _sequence_items(tokens: list[Any], start: int, row: str) -> list[str] | None:
    """The written items of the flow sequence whose `[` is `tokens[start]`, or None
    when it holds anything but scalars, spans lines or has text after it."""
    items = []
    for inner in tokens[start + 1 :]:
        if isinstance(inner, pyyaml.ScalarToken):
            items.append(row[inner.start_mark.column : inner.end_mark.column])
        elif isinstance(inner, pyyaml.FlowEntryToken):
            continue
        elif isinstance(inner, pyyaml.FlowSequenceEndToken):
            first = tokens[start]
            if inner.start_mark.line != first.start_mark.line or not items:
                return None
            if row[inner.end_mark.column :].strip():
                return None
            return items
        else:
            return None
    return None


def _wrap_sequence(row: str, column: int, items: list[str]) -> list[str]:
    """The lines of a sequence of scalars whose `[` is at `column` of `row`: broken
    after a comma, each further line starting under the first item."""
    indent = " " * (column + 1)
    lines, current = [], row[: column + 1] + items[0]
    for item in items[1:]:
        if len(current) + 2 + len(item) + 1 > MAX_WIDTH:
            lines.append(current + ",")
            current = indent + item
        else:
            current += ", " + item
    lines.append(current + "]")
    return lines


def _wrap_long_sequences(text: str) -> str:
    """Apply rule 3, second part: an inline sequence of scalars at the end of a line
    over MAX_WIDTH is wrapped after a comma, and each further line starts at the
    column of its first item, as yamllint's indentation rule expects."""
    rows = text.split("\n")
    if not any(len(row) > MAX_WIDTH for row in rows):
        return text
    tokens = list(pyyaml.scan(text, Loader=pyyaml.SafeLoader))
    changes: dict[int, list[str]] = {}
    for index, token in enumerate(tokens):
        if not isinstance(token, pyyaml.FlowSequenceStartToken):
            continue
        line = token.start_mark.line
        if len(rows[line]) <= MAX_WIDTH or line in changes:
            continue
        items = _sequence_items(tokens, index, rows[line])
        if items is not None:
            changes[line] = _wrap_sequence(rows[line], token.start_mark.column, items)
    return _replace_lines(rows, changes)


def _replace_lines(rows: list[str], changes: dict[int, list[str]]) -> str:
    """The text of `rows` with each line number in `changes` replaced by its lines."""
    for line in sorted(changes, reverse=True):
        rows[line : line + 1] = changes[line]
    return "\n".join(rows)


def _fold_points(raw: str) -> list[int]:
    """The positions of single spaces where a scalar's text can be folded onto the next
    line. YAML reads such a line break, with the next line's indentation, as one
    space, in a quoted scalar and in a plain one."""
    found = []
    for i, char in enumerate(raw):
        if char != " " or i == 0 or i == len(raw) - 1:
            continue
        if raw[i - 1] in " \\" or raw[i + 1] == " ":
            continue
        found.append(i)
    return found


def _fold_text(head: str, raw: str, indent: str, points: list[int]) -> list[str]:
    """The lines of `head` followed by `raw`, folded at `points`, with each further
    line starting with `indent`."""
    lines, current, last = [], head, 0
    for cut in points + [len(raw)]:
        word = raw[last:cut]
        if len(current) + len(word) > MAX_WIDTH and current.strip() and current != head:
            lines.append(current)
            current = indent
            word = word.lstrip(" ")
        current += word
        last = cut
    lines.append(current)
    return [part.rstrip() for part in lines]


def _fold_indent(tokens: list[Any], index: int, before: Any) -> str | None:
    """The indentation of the further lines of the scalar `tokens[index]`: 2 more than
    its `-` or its key. None when the scalar is neither an item nor a value."""
    if isinstance(before, pyyaml.BlockEntryToken):
        return " " * (before.start_mark.column + 2)
    if isinstance(before, pyyaml.ValueToken):
        for token in reversed(tokens[:index]):
            if isinstance(token, pyyaml.KeyToken):
                return " " * (token.start_mark.column + 2)
    return None


def _fold_long_scalars(text: str) -> str:
    """Apply rule 7: a scalar value on a line over MAX_WIDTH is folded at its spaces
    onto more lines, indented 2 more than its key or its `-`. A value that has no
    space to fold at, such as `!python module.attribute`, moves onto the next line
    instead when it fits there."""
    rows = text.split("\n")
    if not any(len(row) > MAX_WIDTH for row in rows):
        return text
    tokens = list(pyyaml.scan(text, Loader=pyyaml.SafeLoader))
    changes: dict[int, list[str]] = {}
    for index, token in enumerate(tokens):
        if not isinstance(token, pyyaml.ScalarToken) or token.style in ("|", ">"):
            continue
        line = token.start_mark.line
        row = rows[line]
        if token.end_mark.line != line or len(row) <= MAX_WIDTH:
            continue
        if row[token.end_mark.column :].strip():
            continue  # something follows the scalar on its line
        start = token.start_mark.column
        before = tokens[index - 1]
        if isinstance(before, pyyaml.TagToken) and before.start_mark.line == line:
            start = before.start_mark.column
            before = tokens[index - 2]
        indent = _fold_indent(tokens, index, before)
        if indent is None:
            continue
        head, raw = row[:start], row[start:]
        is_tagged = start != token.start_mark.column
        points = [] if is_tagged else _fold_points(raw)
        if points:
            changes[line] = _fold_text(head, raw, indent, points)
        elif isinstance(before, pyyaml.ValueToken) and len(indent + raw) <= MAX_WIDTH:
            changes[line] = [head.rstrip(), indent + raw]
    return _replace_lines(rows, changes)


def _top_level_key_lines(text: str) -> list[int]:
    """The line number of each key of the document's root mapping."""
    root = pyyaml.compose(text, Loader=pyyaml.SafeLoader)
    if not isinstance(root, pyyaml.MappingNode):
        return []
    return [key.start_mark.line for key, _ in root.value]


def _separate_top_level_keys(text: str) -> str:
    """Apply rule 5: one blank line before each top-level key after the first, or
    before the comments at column 0 directly above it."""
    rows = text.split("\n")
    for line in reversed(_top_level_key_lines(text)[1:]):
        above = line
        while above > 0 and rows[above - 1].startswith("#"):
            above -= 1
        if above > 0 and rows[above - 1].strip():
            rows.insert(above, "")
    return "\n".join(rows)


def _layout(text: str) -> str:
    """Write a blueprint in the layout.

    Args:
        text: The YAML text of one blueprint file.

    Returns:
        The same document in the layout. A second call returns it unchanged.

    Raises:
        ValueError: If the result would mean something else than `text`, or if
            opening long collections does not finish.
    """
    yaml = _make_yaml()
    data = yaml.load(text)
    if not isinstance(data, (CommentedMap, CommentedSeq)):
        return text
    aliases: set[int] = set()
    _find_aliases(data, set(), aliases)
    moves: list[tuple[tuple[Any, ...], list[str]]] = []
    _choose_styles(data, aliases, set(), moves)
    out = _restore_lines(_dump(yaml, data), moves)
    # Rule 3: open the outermost inline collection on each long line, then read the
    # result again so line numbers are those of the new text, until none is left
    for _ in range(MAX_OPEN_PASSES):
        data = yaml.load(out)
        if not _open_long_flows(data, _long_content_lines(out), set()):
            break
        out = _dump(yaml, data)
    else:
        raise ValueError("the layout did not settle; not changed")
    out = _wrap_long_sequences(_wrap_comments(out))
    out = _separate_top_level_keys(_fold_long_scalars(out))
    if _node_tree(out) != _node_tree(text):
        raise ValueError("the layout would change what the file means; not changed")
    return out


def _reduce(node: Any, seen: dict[int, int]) -> Any:
    """Reduce a PyYAML node to nested tuples, a node met again becoming a reference to
    its first place."""
    if id(node) in seen:
        return ("same node as", seen[id(node)])
    seen[id(node)] = len(seen)
    if isinstance(node, pyyaml.ScalarNode):
        return ("scalar", node.tag, node.value, node.style)
    if isinstance(node, pyyaml.SequenceNode):
        return ("sequence", node.tag, tuple(_reduce(i, seen) for i in node.value))
    return (
        "mapping",
        node.tag,
        tuple((_reduce(k, seen), _reduce(v, seen)) for k, v in node.value),
    )


def _node_tree(text: str) -> Any:
    """Reduce a document to what desflow's loader reads from it.

    Args:
        text: The YAML text.

    Returns:
        The node tree as PyYAML composes it, with tags, scalar values and quoting,
        shared nodes and anchor names, and without flow or block style and positions.
    """
    root = pyyaml.compose(text, Loader=pyyaml.SafeLoader)
    anchors = []
    for event in pyyaml.parse(text, Loader=pyyaml.SafeLoader):
        if getattr(event, "anchor", None):
            anchors.append((type(event).__name__, event.anchor))
    return _reduce(root, {}), anchors


def main(argv: list[str]) -> int:
    """Write the files in the layout, or with `--check` list those that are not in it.

    Args:
        argv: The command-line arguments, without the program name.

    Returns:
        0, or 1 when a file was refused or, with `--check`, would change.
    """
    parser = argparse.ArgumentParser(description="Write blueprints in one layout.")
    parser.add_argument("--check", action="store_true", help="write nothing")
    parser.add_argument("files", nargs="*", type=Path)
    args = parser.parse_args(argv)
    changed = refused = 0
    for path in args.files:
        text = path.read_text(encoding="utf-8")
        try:
            new = _layout(text)
        except ValueError as exc:
            print(f"{path}: {exc}")
            refused += 1
            continue
        if new == text:
            continue
        changed += 1
        if args.check:
            print(f"{path}: not in the blueprint layout")
        else:
            path.write_text(new, encoding="utf-8")
            print(f"{path}: rewritten")
    return 1 if refused or (args.check and changed) else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
