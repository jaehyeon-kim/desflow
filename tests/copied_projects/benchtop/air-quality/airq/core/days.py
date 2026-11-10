"""Turns day phrases into dates in UTC.

The command-line tools, the Airflow DAG parameters and the assistant all accept day
phrases, so a date never has to be written out. Examples are "yesterday", "3 days ago",
"last monday", "this weekend", "20 September" and YYYY-MM-DD.

`resolve` reads the common phrases itself. Any other phrase goes to the dateparser
library, which reads many more, such as "three days ago".
"""

import argparse
import re
from datetime import UTC, date, datetime, timedelta

from dateutil.relativedelta import relativedelta, weekdays

WEEKDAYS = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]  # fmt: skip
FORMS = 'a phrase such as yesterday, "3 days ago", "a week ago", "last monday", "this weekend" or "20 September", or YYYY-MM-DD'  # fmt: skip


def today() -> date:
    """
    Returns today's date in UTC.

    Returns:
        date: Today's date in UTC.
    """
    return datetime.now(UTC).date()


def resolve(text: str, on: date | None = None, forward: bool = False) -> date:
    """
    Returns the date that a day phrase names.

    A bare weekday such as "saturday" can mean the next one or the last one. `forward`
    chooses: the next one for a forecast, the last one for a reading. `on` itself counts
    as that weekday in both cases.

    Args:
        text (str): The day phrase, such as "yesterday", "in 3 days" or YYYY-MM-DD.
        on (date, optional): The day the phrase is counted from. Defaults to today, UTC.
        forward (bool): Whether a bare weekday means the next one rather than the last
            one.

    Returns:
        date: The date the phrase names.

    Raises:
        ValueError: If neither this module nor dateparser can read the phrase.
    """
    on = on or today()
    phrase = " ".join(text.lower().split())
    # dateparser reads "today", "3 days ago", "in 2 days" and dates, but not the forms
    # below: "N days ahead", weekends, and weekdays counted from `on` itself.
    if match := re.fullmatch(r"(\d+) days? (?:from now|ahead)", phrase):
        return on + timedelta(days=int(match[1]))
    phrase = {"this weekend": "saturday", "next weekend": "next saturday", "last weekend": "last saturday"}.get(phrase, phrase)  # fmt: skip
    if (match := re.fullmatch(r"(?:(this|next|last) )?(\w+)", phrase)) and match[2] in WEEKDAYS:  # fmt: skip
        weekday = weekdays[WEEKDAYS.index(match[2])]
        if match[1] == "next":
            return on + relativedelta(days=1, weekday=weekday(+1))
        if match[1] == "last":
            return on + relativedelta(days=-1, weekday=weekday(-1))
        return on + relativedelta(weekday=weekday(+1 if forward else -1))
    return _parse(phrase, on, forward)


def _parse(phrase: str, on: date, forward: bool) -> date:
    """
    Returns the date that dateparser reads from a phrase.

    dateparser is imported inside the function, because it takes about a second to load.

    Args:
        phrase (str): The day phrase, in lower case with single spaces.
        on (date): The day the phrase is counted from.
        forward (bool): Whether to prefer a future date over a past one.

    Returns:
        date: The date the phrase names.

    Raises:
        ValueError: If dateparser cannot read the phrase.
    """
    import dateparser  # imported here: it takes about a second to load

    settings = {
        "RELATIVE_BASE": datetime.combine(on, datetime.min.time()),
        "PREFER_DATES_FROM": "future" if forward else "past",
    }
    parsed = dateparser.parse(phrase, languages=["en"], settings=settings)
    if parsed is None:
        raise ValueError(f"cannot read the day {phrase!r}")
    return parsed.date()


def argument(text: str) -> date:
    """
    Returns the date that a command-line day phrase names.

    It is the argparse `type` for day options, such as `--date` and `--as-of`.

    Args:
        text (str): The day phrase given on the command line.

    Returns:
        date: The date the phrase names, counted from today, UTC.

    Raises:
        argparse.ArgumentTypeError: If the phrase cannot be read. The message lists the
            accepted forms.
    """
    try:
        return resolve(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"cannot read {text!r}; use {FORMS}") from None
