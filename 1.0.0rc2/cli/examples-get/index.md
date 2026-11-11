# desflow examples get

The help of `desflow examples get`, generated from the command's code. [Command Line](https://jaehyeon.me/desflow/latest/cli/#get-an-example) uses this one on the shop, and its [What It Is](https://jaehyeon.me/desflow/latest/cli/#what-it-is) lists every command.

```text
Usage: desflow examples get [OPTIONS] {name}

  Download an example into <dest>/<name>/<api>/, at the installed release.

Arguments:
  name  The example, as `desflow examples` lists it.  [required]

Options:
  --api <str>           Only this version: yaml, builder or low-level.
  --dest <path>         The folder to download into.  [default: .]
  --ref <str>           A git tag or branch. Defaults to the installed release's
                        tag.
  --force / --no-force  Replace files that are already there.  [default: no-
                        force]
  --help                Show this message and exit.

  Examples:

    # Download the three versions of an example into ./local/

    $ desflow examples get local

    # Download only the YAML version into another folder

    $ desflow examples get local --api yaml --dest work

    # Download it from the main branch

    $ desflow examples get local --ref main
```
