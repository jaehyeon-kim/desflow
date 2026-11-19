# Commands from docs/integrations/mcp-server.md and ai-assistants.md.
# tests/docs/test_docs_mcp_server.py runs list-tools, list-tools-changes and
# run-example. The others install a tool globally, write to the user's home folder or
# need Claude Code, Codex, Antigravity or the GitHub CLI, which the tests do not have.
# tests/docs/test_plugin.py and the CI job plugin check the files the plugin installs.

# --8<-- [start:install-uv]
uv tool install "desflow[mcp]"
# --8<-- [end:install-uv]

# --8<-- [start:install-pip]
pip install "desflow[mcp]"
# --8<-- [end:install-pip]

# --8<-- [start:claude-code]
claude plugin marketplace add jaehyeon-kim/desflow
claude plugin install desflow@desflow
# --8<-- [end:claude-code]

# --8<-- [start:codex]
codex plugin marketplace add jaehyeon-kim/desflow
codex plugin add desflow@desflow
# --8<-- [end:codex]

# --8<-- [start:antigravity]
agy plugin install https://github.com/jaehyeon-kim/desflow
# --8<-- [end:antigravity]

# --8<-- [start:copy]
# The source of the installed release, which holds the skill
src=$(mktemp -d)
git clone --depth 1 --branch "v$(desflow --version)" https://github.com/jaehyeon-kim/desflow "$src" &&
  mkdir -p ~/.claude/skills &&
  rm -rf ~/.claude/skills/desflow &&
  cp -R "$src/skills/desflow" ~/.claude/skills/desflow
rm -rf "$src"
# --8<-- [end:copy]

# --8<-- [start:gh-skill]
gh skill install jaehyeon-kim/desflow skills/desflow --pin "v$(desflow --version)" \
  --agent claude-code --scope user
# --8<-- [end:gh-skill]

# --8<-- [start:check-copy]
ls ~/.claude/skills/desflow/SKILL.md
# --8<-- [end:check-copy]

# --8<-- [start:mcp-claude-code]
# Writes .mcp.json in this folder
claude mcp add --scope project desflow -- desflow mcp
# --8<-- [end:mcp-claude-code]

# --8<-- [start:mcp-codex]
codex mcp add desflow -- desflow mcp
# --8<-- [end:mcp-codex]

# --8<-- [start:list-tools]
python mcp_client.py desflow mcp
# --8<-- [end:list-tools]

# --8<-- [start:list-tools-changes]
python mcp_client.py desflow mcp --allow-changes
# --8<-- [end:list-tools-changes]

# --8<-- [start:run-example]
python mcp_example.py
# --8<-- [end:run-example]
