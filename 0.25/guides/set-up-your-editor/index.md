# Set Up Your Editor

Get completion for the keys of a blueprint as you type, and a mark under a misspelt key or a value of the wrong kind. It builds on [YAML Blueprints](https://jaehyeon.me/desflow/latest/user-guide/blueprints/index.md), which describes what a blueprint file is, and on [YAML Keys](https://jaehyeon.me/desflow/latest/reference/yaml-keys/index.md), which lists every key the editor will offer.

The editor checks the shape of the file against the blueprint JSON Schema. It does not run the model, so `desflow run` is still what proves a blueprint works.

______________________________________________________________________

## Steps

1. Install Visual Studio Code and the Red Hat YAML extension (`redhat.vscode-yaml`) version 1.24.0 or later, which is the extension that reads a JSON Schema for a YAML file.

1. Put this line at the top of the blueprint, or use the setting in step 3 for every blueprint in a folder:

   ```yaml
   # yaml-language-server: $schema=https://jaehyeon.me/desflow/latest/schema/blueprint.schema.json
   ```

1. To cover a folder of blueprints without a line in each file, add the schema to the workspace settings (`.vscode/settings.json`) with a glob of the files it applies to. The same object holds the tags of step 4:

   ```json
   {
     "yaml.schemas": {
       "https://jaehyeon.me/desflow/latest/schema/blueprint.schema.json": "blueprints/*.yaml"
     },
     "yaml.customTags": [
       "!python scalar:string",
       "!include scalar:object",
       "!include mapping:object"
     ]
   }
   ```

1. The `yaml.customTags` entries tell the extension about the tags a blueprint uses. Without them, it marks `!python`, and an `!include` written as a mapping, as unknown tags, and it marks a file name given to `!include` as the wrong type. They go in the same `.vscode/settings.json`. Your user settings work too.

1. Open the file and write a key. The editor offers the keys of the section you are in, shows the section's description when you hover over it, and underlines a key it does not know or a value of the wrong kind.

To work offline, or to pin the schema to the version you have installed, write the copy that comes with the package to a file and name that file instead of the address:

```bash
desflow schema > blueprint.schema.json
```

Then use `# yaml-language-server: $schema=./blueprint.schema.json` as the line, or `"./blueprint.schema.json"` as the key in `yaml.schemas`. In the line the path is relative to the blueprint's folder. In `yaml.schemas` it is relative to the workspace folder, in a single-folder workspace.

The address above always serves the latest release, so it can offer keys that an older install does not have. To pin a release, use its versioned address, `https://jaehyeon.me/desflow/<version>/schema/blueprint.schema.json`, with `<version>` the release you installed, which `desflow --version` prints.

______________________________________________________________________

## What the Editor Cannot Check

The schema describes the shape of one file. `desflow run` checks the following instead, and reports each with the file and line:

- The content of a file named by `!include`.
- Whether the module and attribute of a `!python` reference exist.
- The text of an expression, such as a `when` condition or an amount.
- A name that refers to another section, such as a `spawn` that names a task that is not there, or a `resource` that is not declared.
- The rules between the parameters of one distribution, such as a `low` below a `high`.

A mapping `!include`, such as `!include {file: fitted/orders.yaml, spawn: pack_order}`, is checked as the entry it stands for, so the editor marks `file` as an unknown key. `desflow run` reads it correctly.

Where a value is a number, a whole number, `true` or `false`, or one of a fixed set of names, a `${VAR}` or `${VAR:-default}` reference is also accepted.

______________________________________________________________________

## Example

Below is the whole blueprint, with the line that names the schema highlighted. Run it with `desflow run editor_setup.yaml`.

```yaml
# A small blueprint with the line that tells the Red Hat YAML extension which schema to use.
# Run it with: desflow run editor_setup.yaml
# yaml-language-server: $schema=https://jaehyeon.me/desflow/latest/schema/blueprint.schema.json
simulation:
  sim_id: Shop
  factor: 0
  random_seed: 42

resources:
  packers: {current_cap: 1, max_cap: 1}

services:
  packing: {dist: constant, value: 1.5}

arrivals:
  orders: {dist: exponential, rate: 0.5, spawn: pack_order}

tasks:
  pack_order:
    service: packing
    resource: packers
    payload: {event_type: order_packed}

egress:
  - type: Console

run:
  until: 20
```

______________________________________________________________________

## Sources

The modeline and the `yaml.schemas` and `yaml.customTags` settings belong to the [yaml-language-server](https://github.com/redhat-developer/yaml-language-server/tree/2.0.0) that the extension runs, as its README at version 2.0.0 describes. That README does not say how a tag with both a scalar and a mapping form is written. The source at 2.0.0 shows it: `src/languageservice/parser/custom-tag-provider.ts` adds one tag for each entry of `yaml.customTags`, and its own `!include` is a scalar only. The YAML library the server uses (version 2.8.3, `src/compose/compose-collection.ts`) then chooses a tag by its name and its kind of node. Without the `!include mapping:object` entry, a mapping `!include` is reported as `Unresolved tag: !include`.
