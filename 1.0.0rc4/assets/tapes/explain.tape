# desflow explain on the experiment's shop, as Command Line runs it: in a folder that
# holds experiment/yaml/experiment_shop.yaml, as `desflow examples get experiment` makes.
# Record it with: uv run python scripts/diagrams.py explain

Set Shell "bash"
Set FontSize 16
Set Width 1200
Set Height 720
Set Padding 20
Set Theme "Catppuccin Mocha"
Set TypingSpeed 45ms

Hide
Type "d=$(mktemp -d) && mkdir -p $d/experiment/yaml && cp examples/yaml/experiment_shop.yaml $d/experiment/yaml/ && cd $d && export PS1='$ ' && clear"
Enter
Sleep 1s
Show

Type "desflow explain experiment/yaml/experiment_shop.yaml"
Sleep 400ms
Enter
Wait+Screen@60s /Hints for utilisation/
Sleep 4s
