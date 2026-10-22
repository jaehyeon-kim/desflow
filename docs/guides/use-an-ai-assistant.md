# Use an AI Assistant

Let an AI coding assistant write, check and run desflow blueprints with the rules of the library in front of it, instead of guessing them. It builds on [YAML Blueprints](../user-guide/blueprints.md) and on [Set Up Your Editor](set-up-your-editor.md), whose schema the assistant also uses.

The skill is a folder, `skills/desflow/`, in the Agent Skills format that Claude Code and other assistants read. Its `SKILL.md` tells the assistant how to pick a way to write the model, how to write and run a blueprint, the rules assistants get wrong, and how to run a study with a seed, a warm-up and replications. It links to files for every key, every distribution, every connector, the `local` example in all three APIs, and the blueprint JSON Schema. The folder is generated from the code, so it lists what the release has.

---

## Steps

1. Install desflow, so the assistant can run `desflow run`, `desflow examples` and `desflow schema`.
2. Copy `skills/desflow/` from the repository at the tag of the release you installed, such as `v0.24.0`, into the skills folder of the assistant. For Claude Code, `~/.claude/skills/desflow/` is for every project and `.claude/skills/desflow/` in a project is for that project.
3. Ask the assistant for a model, such as "write a desflow blueprint for a shop with two packers and a lunch rush, and run it". It reads the skill when the request mentions desflow or a blueprint.

With the GitHub CLI, one command does the copy. `gh skill install` is a preview command of the GitHub CLI, so it needs `gh` 2.90.0 or later:

```bash
gh skill install jaehyeon-kim/desflow desflow --agent claude-code --scope user
```

`--scope user` installs for every project and `--scope project`, the default, for the current one. Another assistant takes its own `--agent` value, listed in `gh skill install --help`. Without `--agent`, `gh` asks which assistant to install for, and when it cannot ask it installs for GitHub Copilot, which at project scope uses `.agents/skills`, a folder several other assistants also read. `gh` installs the latest release, and `desflow@v0.24.0` after the skill name pins one.

---

## Other Ways to Teach an Assistant

- **Schema.** Give the assistant `https://jaehyeon.me/desflow/latest/schema/blueprint.schema.json`, or the output of `desflow schema`, to validate what it writes.
- **Documentation as text.** `https://jaehyeon.me/desflow/latest/llms.txt` is an index of the pages, and `https://jaehyeon.me/desflow/latest/llms-full.txt` holds all of them in one file.

---

## What to Check

An assistant can still write a model that runs and answers the wrong question. Read the blueprint before you trust its records, and check `desflow run` ends without an error. The skill asks the assistant to run a study rather than one run, so ask for the confidence interval, and read [Trusting Results](../tutorials/trusting-results.md) for what a number from a simulation supports.

---

## Sources

The folder layout and the `SKILL.md` fields follow the [Agent Skills specification](https://agentskills.io/specification). The folders Claude Code reads are in its [skills documentation](https://code.claude.com/docs/en/skills). The `gh skill install` flags are those of `gh skill install --help` in GitHub CLI 2.95.0. The install was not run for this page.
