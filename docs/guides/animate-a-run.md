# Animate a Run

Animate a run so each order is seen moving through the shop: arriving, waiting, being packed and leaving. It builds on [Analyse the Event Log](analyse-the-event-log.md), whose event log it animates, and it uses [vidigi](https://hsma-tools.github.io/vidigi/vidigi_docs/), a separate package that animates a discrete event simulation from its event log. Install it with `pip install "vidigi==2.0.0"` and the `parquet` extra of desflow.

---

## Build the Log

vidigi takes a pandas table with the columns `entity_id`, `event_type`, `event` and `time`, as the event log of Analyse the Event Log has them. It recognises the event types `arrival_departure`, `queue`, `resource_use` and `resource_use_end`. It draws a resource as numbered slots, so the rows of `resource_use` need a `resource_id` column. There is one packer, so the slot is 1:

```python title="docs/snippets/guides/animate_a_run.py:log"
def vidigi_log(out: Path) -> pd.DataFrame:
    """The run's event log as a pandas table, with the packer's slot on its use rows."""
    log = event_log(read_events(out)).to_pandas()
    # vidigi draws the packers in numbered slots; there is one packer, so slot 1
    log["resource_id"] = log["event_type"].eq("resource_use").map({True: 1})
    return log
```

vidigi reads one run at a time, and refuses a log that holds more than one.

---

## Place the Events

Each event is drawn at a position, with a label. The event that uses a resource names it, so vidigi draws the packers there:

```python title="docs/snippets/guides/animate_a_run.py:positions"
# Where each event is drawn: the arrival, the queue, the packer and the exit
POSITIONS = pd.DataFrame(
    [
        {"event": "arrival", "x": 250, "y": 220, "label": "Arrival"},
        {"event": "waiting_for_packer", "x": 250, "y": 160, "label": "Waiting"},
        {
            "event": "packing_start",
            "x": 250,
            "y": 100,
            "label": "Packing",
            "resource": "packers",
        },
        {"event": "depart", "x": 250, "y": 40, "label": "Shipped"},
    ]
)
```

---

## Animate

`animate_activity_log` returns a Plotly figure. `scenario` gives the number of packers to draw, the times are in seconds, and `every_x_time_units` makes one frame for every 2 seconds:

```python title="docs/snippets/guides/animate_a_run.py:animate"
def animation(log: pd.DataFrame) -> Figure:
    """The Plotly animation of the run, one frame for every 2 simulated seconds."""
    figure = animate_activity_log(
        log,
        POSITIONS,
        scenario={"packers": 1},  # the number of packers drawn at the packing event
        simulation_time_unit="seconds",
        every_x_time_units=FRAME_SECONDS,
        limit_duration=RUN_SECONDS,
        plotly_height=480,
        override_x_max=400,
        stage_label_offset=30,  # room for the order icon before each label
        override_y_max=260,
        text_size=14,
        entity_icon_size=16,
        gap_between_entities=32,  # clear of the icon size at a phone width
    )
    # Narrow side margins leave the plot wide enough for the labels on a phone
    figure.update_layout(margin={"l": 10, "r": 10})
    return figure
```

The 2-minute run makes **61** frames, named 0, 2, 4 and on to 120, and the figure draws all **64** orders.

---

## See It

`figure.write_html("animate_a_run.html", include_plotlyjs="cdn", config={"responsive": True})` saves the figure as a page that fits the width of its window. This is the animation of the script below. Press play, or drag the slider:

<iframe src="../../assets/charts/animate-a-run.html" title="The shop's orders arriving, waiting, being packed and leaving over two minutes" width="100%" height="500" loading="lazy"></iframe>

---

## Example

The whole file is below. It imports the functions of Analyse the Event Log, so keep the two files in one folder.

```python title="docs/snippets/guides/animate_a_run.py"
"""Animate a Run: the shop's event log, animated with vidigi.

The script runs the shop of Analyse the Event Log for two minutes, reads its Parquet
files back as an event log, adds the one column vidigi needs beyond it, and animates
the run with vidigi 2.0.0's animate_activity_log: one frame for every 2 simulated
seconds. python animate_a_run.py writes the animation to animate_a_run.html.
"""

import tempfile
from pathlib import Path

import pandas as pd
from analyse_the_event_log import RUN_SECONDS, event_log, read_events, run
from plotly.graph_objects import Figure
from vidigi.animation import animate_activity_log

FRAME_SECONDS = 2


def vidigi_log(out: Path) -> pd.DataFrame:
    """The run's event log as a pandas table, with the packer's slot on its use rows."""
    log = event_log(read_events(out)).to_pandas()
    # vidigi draws the packers in numbered slots; there is one packer, so slot 1
    log["resource_id"] = log["event_type"].eq("resource_use").map({True: 1})
    return log


# Where each event is drawn: the arrival, the queue, the packer and the exit
POSITIONS = pd.DataFrame(
    [
        {"event": "arrival", "x": 250, "y": 220, "label": "Arrival"},
        {"event": "waiting_for_packer", "x": 250, "y": 160, "label": "Waiting"},
        {
            "event": "packing_start",
            "x": 250,
            "y": 100,
            "label": "Packing",
            "resource": "packers",
        },
        {"event": "depart", "x": 250, "y": 40, "label": "Shipped"},
    ]
)


def animation(log: pd.DataFrame) -> Figure:
    """The Plotly animation of the run, one frame for every 2 simulated seconds."""
    figure = animate_activity_log(
        log,
        POSITIONS,
        scenario={"packers": 1},  # the number of packers drawn at the packing event
        simulation_time_unit="seconds",
        every_x_time_units=FRAME_SECONDS,
        limit_duration=RUN_SECONDS,
        plotly_height=480,
        override_x_max=400,
        stage_label_offset=30,  # room for the order icon before each label
        override_y_max=260,
        text_size=14,
        entity_icon_size=16,
        gap_between_entities=32,  # clear of the icon size at a phone width
    )
    # Narrow side margins leave the plot wide enough for the labels on a phone
    figure.update_layout(margin={"l": 10, "r": 10})
    return figure


def make(out: Path) -> Figure:
    """Runs the shop into out and animates it."""
    run(out)
    return animation(vidigi_log(out))


if __name__ == "__main__":
    with tempfile.TemporaryDirectory() as folder:
        figure = make(Path(folder))
    figure.write_html(
        "animate_a_run.html", include_plotlyjs="cdn", config={"responsive": True}
    )
    print(f"{len(figure.frames)} frames")
```
