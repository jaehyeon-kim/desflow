# desflow explain on the busy hour of Queues and Capacity, run in docs/snippets/yaml
# with the command the page shows.
# Record it with: uv run python scripts/diagrams.py explain

Set Shell "bash"
Set FontSize 16
Set Width 1100
Set Height 720
Set Padding 20
Set Theme "Catppuccin Mocha"
Set TypingSpeed 45ms

Hide
Type "cd docs/snippets/yaml && export PS1='$ ' && clear"
Enter
Sleep 1s
Show

Type "desflow explain queues_and_capacity.yaml"
Sleep 400ms
Enter
Wait+Screen@60s /Hints for utilisation/
Sleep 4s
