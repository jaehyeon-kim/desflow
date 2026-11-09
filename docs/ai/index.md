# Agent Skill

The desflow skill gives an AI coding assistant the rules of the library, so that it writes, checks, tests and runs desflow models instead of guessing them.

The skill is a folder, `skills/desflow/`, in the Agent Skills format that Claude Code and other assistants read. An assistant that supports the format loads the skill when a request mentions desflow, a blueprint, a test of a simulation model, or a SimPy model that streams records. The folder is generated from the code, so it lists what the release has.

---

## What the Skill Holds

`SKILL.md` gives the assistant the rules that assistants get wrong, and tells it how to:

- pick a way to write the model;
- write and run a blueprint;
- draft a model from a description of a process;
- check a model before a long run, and against the request;
- test a model;
- run a study with a seed, a [warm-up](../learn/concepts/output-analysis.md#one-run-starts-empty) and [replications](../learn/concepts/output-analysis.md#replications), compare designs and fit measured times.

It links to files for every key, every distribution, every connector, every command, every name the package exports, one shop model in all three APIs, the blueprint JSON Schema, a guide to testing a model and a guide to porting a SimPy program. It also holds `scripts/check_model.py`, which checks the files of a model and compares the records of its forms.

[Build a Model](build-a-model.md) follows the skill step by step. [Port from SimPy](from-simpy.md) covers the SimPy guide, and [Evaluation](evaluation.md) shows how much the skill changed the mistakes that assistants made.

---

## Install the Skill

1. Install desflow, so the assistant can run the `desflow` commands that [Command Line](../cli/index.md) shows, such as `desflow explain` and `desflow run`.
2. Copy `skills/desflow/` into the skills folder of the assistant. Take it from the repository at the tag of the release you installed: `v` followed by what `desflow --version` prints. For Claude Code, `~/.claude/skills/desflow/` is for every project and `.claude/skills/desflow/` in a project is for that project. With git, these lines copy it for every project:

    ```bash
    # The source of the installed release, which holds the skill
    git clone --depth 1 --branch "v$(desflow --version)" https://github.com/jaehyeon-kim/desflow /tmp/desflow-src
    mkdir -p ~/.claude/skills
    cp -R /tmp/desflow-src/skills/desflow ~/.claude/skills/desflow
    ```

    The block is for a first install. Before you run it again, delete `/tmp/desflow-src` and `~/.claude/skills/desflow`, because `git clone` stops at an existing folder and `cp -R` would put the new copy inside the old one. If `CLAUDE_CONFIG_DIR` is set, use that folder in place of `~/.claude`, here and in the check below.

3. Ask the assistant for a model, such as "write a desflow blueprint for a shop with two packers and a lunch rush, and run it". It reads the skill when the request mentions desflow or a blueprint.

With the GitHub CLI, one command does the copy. `gh skill install` is a preview command of the GitHub CLI, so it needs `gh` 2.90.0 or later:

```bash
gh skill install jaehyeon-kim/desflow desflow --agent claude-code --scope user
```

`--scope user` installs for every project, in your home folder. `--scope project`, the default, installs for the current project, inside its git repository, so run it from that repository. Another assistant takes its own `--agent` value, listed in `gh skill install --help`. Without `--agent`, `gh` asks which assistant to install for. When it cannot ask, it installs for GitHub Copilot. At project scope, Copilot uses `.agents/skills`, a folder several other assistants also read. `gh` installs the latest release. `desflow@v<version>` in place of `desflow` pins one, with `<version>` the release you installed.


---

## Check the Install

The folder must hold `SKILL.md`, and Claude Code lists the skills it has loaded with `/skills`, where `desflow` must appear:

```bash
ls ~/.claude/skills/desflow/SKILL.md
```

---

## Other Ways to Teach an Assistant

- **Schema.** Give the assistant `https://jaehyeon.me/desflow/latest/schema/blueprint.schema.json`, or the output of `desflow schema`, to validate what it writes.
- **Documentation as text.** `https://jaehyeon.me/desflow/latest/llms.txt` is an index of the pages, and `https://jaehyeon.me/desflow/latest/llms-full.txt` holds all of them in one file.

---

## Sources

The folder layout and the `SKILL.md` fields follow the [Agent Skills specification](https://agentskills.io/specification). The folders Claude Code reads are in its [skills documentation](https://code.claude.com/docs/en/skills). The `gh skill install` flags are those of `gh skill install --help` in GitHub CLI 2.95.0.
