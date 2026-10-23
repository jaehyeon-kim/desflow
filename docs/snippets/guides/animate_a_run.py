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


# --8<-- [start:log]
def vidigi_log(out: Path) -> pd.DataFrame:
    """The run's event log as a pandas table, with the packer's slot on its use rows."""
    log = event_log(read_events(out)).to_pandas()
    # vidigi draws the packers in numbered slots; there is one packer, so slot 1
    log["resource_id"] = log["event_type"].eq("resource_use").map({True: 1})
    return log


# --8<-- [end:log]


# --8<-- [start:positions]
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
# --8<-- [end:positions]


# --8<-- [start:animate]
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


# --8<-- [end:animate]


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
