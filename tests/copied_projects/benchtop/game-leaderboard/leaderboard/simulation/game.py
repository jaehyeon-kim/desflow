"""The teams playing now: players join, teams form and dissolve. Plain Python only."""

from dataclasses import dataclass, field

import numpy as np

TEAM_SIZE = 15  # a team this full takes no one else
NEW_TEAM_SHARE = 0.1  # how often an arriving player forms a team although one has room
COLORS = ["Amber", "Azure", "Coral", "Fuchsia", "Indigo", "Jade", "Magenta", "Olive", "Ruby", "Teal"]  # fmt: skip
ANIMALS = ["Bilby", "Dingo", "Emu", "Koala", "Kookaburra", "Numbat", "Platypus", "Quokka", "Wallaby", "Wombat"]  # fmt: skip


@dataclass
class Team:
    """
    A team and the players in it now.

    Attributes:
        team_id (str): A 10-digit id, unique across the run.
        name (str): A colour and an animal, unique among the teams playing.
        members (set[str]): The ids of the players in the team now.
    """

    team_id: str
    name: str
    members: set[str] = field(default_factory=set)


class Game:
    """
    The teams playing now.

    Attributes:
        rng (np.random.Generator): The simulation's random number generator.
        teams (dict[str, Team]): The teams playing, by id.
    """

    def __init__(self, rng: np.random.Generator):
        """
        Starts a game with no teams.

        Args:
            rng (np.random.Generator): The simulation's random number generator.
        """
        self.rng = rng
        self.teams: dict[str, Team] = {}
        self._formed = 0

    def join(self, player: str) -> Team:
        """
        Puts a player in a team with room, or in a new team.

        Args:
            player (str): The player's id.

        Returns:
            Team: The team the player joined.
        """
        open_teams = [t for t in self.teams.values() if len(t.members) < TEAM_SIZE]
        if open_teams and self.rng.random() >= NEW_TEAM_SHARE:
            team = open_teams[self.rng.integers(len(open_teams))]
        else:
            taken = {t.name for t in self.teams.values()}
            free = [
                f"{c}-{a}" for c in COLORS for a in ANIMALS if f"{c}-{a}" not in taken
            ]
            self._formed += 1
            team = Team(f"{self._formed:010}", free[self.rng.integers(len(free))])
            self.teams[team.team_id] = team
        team.members.add(player)
        return team

    def leave(self, team: Team, player: str) -> None:
        """
        Takes a player out of their team, and dissolves the team when it is empty.

        Args:
            team (Team): The player's team.
            player (str): The player's id.
        """
        team.members.discard(player)
        if not team.members:
            del self.teams[team.team_id]
