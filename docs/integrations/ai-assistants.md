# AI Assistants

The desflow plugin adds the [skill](../ai/index.md) and the [MCP server](mcp-server.md) to Claude Code, Codex or Antigravity with one install. The skill gives the assistant the rules of the library, and the server gives it the desflow commands as tools. The plugin starts the server as `desflow mcp`, without `--allow-changes`, so the assistant gets the tools that write nothing.

The repository holds the plugin in `plugin/`: a manifest for each tool, the MCP entry, a logo and a copy of `skills/desflow/`. `.claude-plugin/marketplace.json` and `.agents/plugins/marketplace.json` at the repository root point at that folder. Each tool copies the plugin folder, about 780 KB, into its own plugin folder.

---

## Before You Install

The plugin does not install desflow. The skill tells the assistant to run `desflow` commands, and the MCP server is the `desflow mcp` command, which needs the `mcp` extra. So install desflow with the extra first:

=== "uv"

    ```bash title="docs/snippets/integrations/commands.sh:install-uv"
    uv tool install "desflow[mcp]"
    ```

=== "pip"

    ```bash title="docs/snippets/integrations/commands.sh:install-pip"
    pip install "desflow[mcp]"
    ```

Without the extra, `desflow mcp` exits with an error that names the `pip install` command, and the assistant shows the server as failed.

---

## Install the Plugin

Run the lines of your tool. Each installs the plugin from the repository's `main` branch:

=== "Claude Code"

    ```bash title="docs/snippets/integrations/commands.sh:claude-code"
    claude plugin marketplace add jaehyeon-kim/desflow
    claude plugin install desflow@desflow
    ```

=== "Codex"

    ```bash title="docs/snippets/integrations/commands.sh:codex"
    codex plugin marketplace add jaehyeon-kim/desflow
    codex plugin add desflow@desflow
    ```

=== "Antigravity"

    ```bash title="docs/snippets/integrations/commands.sh:antigravity"
    agy plugin install https://github.com/jaehyeon-kim/desflow
    ```

For Claude Code and Codex, `desflow@desflow` is the plugin's name, an `@`, and the marketplace's name, which are both `desflow`. Start a new session after the install, so the tool loads the skill and the server.

---

## Check the Install

The skill is named `desflow:desflow` in Claude Code and Codex, which join the plugin's name and the skill's name with a colon. Antigravity uses the skill's own name, `desflow`.

=== "Claude Code"

    `claude plugin list` shows `desflow@desflow` as enabled, and `claude plugin details desflow` lists the skill and the MCP server.

=== "Codex"

    `codex plugin list` shows `desflow@desflow` as installed and enabled.

=== "Antigravity"

    `agy plugin list` shows `desflow` as enabled, with 1 skill and 1 MCP server.

Then ask the assistant for a model, such as "write a desflow blueprint for a shop with two packers and a lunch rush, and check it". It loads the skill when the request mentions desflow or a blueprint, and it can call the MCP tool `check`.

---

## Update the Plugin

A new desflow release changes the plugin's version. Upgrade desflow first, with `uv tool upgrade desflow` or `pip install --upgrade "desflow[mcp]"`, so the skill and the command are of one release. Then update the plugin with the same tool:

=== "Claude Code"

    `claude plugin update desflow@desflow`, then start a new session.

=== "Codex"

    `codex plugin marketplace upgrade desflow` refreshes Codex's copy of the repository.

=== "Antigravity"

    Run `agy plugin install https://github.com/jaehyeon-kim/desflow` again.

---

## Install Without the Plugin

Another assistant that reads the Agent Skills format takes the skill as a folder, `skills/desflow/`, which holds the same files as the plugin's copy. Take it from the repository at the tag of the release you installed: `v` followed by what `desflow --version` prints. For Claude Code, `~/.claude/skills/desflow/` is for every project and `.claude/skills/desflow/` in a project is for that project. These lines copy it for every project:

```bash title="docs/snippets/integrations/commands.sh:copy"
# The source of the installed release, which holds the skill
src=$(mktemp -d)
git clone --depth 1 --branch "v$(desflow --version)" https://github.com/jaehyeon-kim/desflow "$src" &&
  mkdir -p ~/.claude/skills &&
  rm -rf ~/.claude/skills/desflow &&
  cp -R "$src/skills/desflow" ~/.claude/skills/desflow
rm -rf "$src"
```

The lines clone into a new private folder, replace an earlier copy of the skill only when the clone succeeds, and remove the clone, so they also update the skill to a newer release. If `CLAUDE_CONFIG_DIR` is set, use that folder in place of `~/.claude`.

With the GitHub CLI 2.90.0 or later, one command does the copy. `gh skill install` is a preview command of the GitHub CLI. The path `skills/desflow` names the folder, because the repository holds a second copy in the plugin, and `--pin` takes the release you installed:

```bash title="docs/snippets/integrations/commands.sh:gh-skill"
gh skill install jaehyeon-kim/desflow skills/desflow --pin "v$(desflow --version)" \
  --agent claude-code --scope user
```

`--scope user` installs for every project, in your home folder. `--scope project`, the default, installs for the current project, inside its git repository. Another assistant takes its own `--agent` value, listed in `gh skill install --help`.

Check that the folder holds `SKILL.md`:

```bash title="docs/snippets/integrations/commands.sh:check-copy"
ls ~/.claude/skills/desflow/SKILL.md
```

In Claude Code, `/skills` lists the loaded skills, and `desflow` must be among them. After a project copy, check `.claude/skills/desflow/SKILL.md` in the project instead. [MCP Server](mcp-server.md#add-it-to-an-assistant) adds the server to an assistant without the plugin.

---

## Sources

The install, list and update commands are those of each tool's own help: `claude plugin --help` in Claude Code 2.1.326, `codex plugin --help` in codex-cli 0.166.0 and `agy plugin help` in Antigravity 1.5.12. The skill's name in each tool follows Claude Code's [plugins reference](https://code.claude.com/docs/en/plugins-reference) and Codex's source (`ext/skills/src/loader/namespace.rs`). The folder layout and the `SKILL.md` fields follow the [Agent Skills specification](https://agentskills.io/specification), and the `gh skill install` flags are those of `gh skill install --help` in GitHub CLI 2.95.0.
