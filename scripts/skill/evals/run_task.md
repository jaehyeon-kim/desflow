# Running the evaluation

This file holds the main session's steps, from the two suites to the result table. `claude plugin eval` makes the runs, each in an empty folder inside its OS sandbox; `grade.py` scores what a script can measure on each kept run; a blind Opus reviewer judges the rest against `review_rubric.md`; and `summarise.py` makes the table of skill-lessons.md, Evaluation, Result. None of these files is shipped: `scripts/gen_skill.py` copies only the files it lists, and `tests/docs/test_skill.py` checks the skill folder against it.

`<scratch>` is a folder outside every repository. Each use of `claude plugin eval` starts nested Claude runs and needs the owner's approval, which the owner gave for the 12e3 runs on 7 Nov 2026.

## Before any counted run

1. The self-test passes: `.venv/bin/python scripts/skill/evals/selftest/selftest.py --python-dir <scratch>/suite-b/py` prints `ok` for every case and exits 0. It uses the Python of the after suite, because `grade.py` runs `scripts/check_model.py` of the repository's current tree, which needs the current desflow.
2. The review cases hold. `.venv/bin/python scripts/skill/evals/review_cases/check.py --prepare <scratch>/rc --skill <scratch>/suite-a/desflow` writes each case of `review_cases/` into `<scratch>/rc/<case>/` as the reviewer reads it. Two fresh Opus reviewers each read every one of those folders as step 7 sets out, and each saves its replies as `<scratch>/rc-a/<case>.jsonl` and `<scratch>/rc-b/<case>.jsonl`. Then `check.py --reviews <scratch>/rc-a <scratch>/rc-b` finds no difference: each reviewer gives the known right counts, and the two find the same kinds in the runs whose counts are not known.

## Steps

1. Build the suite of each version, each from a checkout of the repository at that version, into a folder whose name does not tell the version: `.venv/bin/python scripts/skill/evals/make_suite.py --tree <before tree> --out <scratch>/suite-a`, and the same with `<after tree>` and `<scratch>/suite-b`. `before` is the tree before change 1 of skill-lessons.md; `after` is the tree after change 13 and both `desflow check` warnings. Write down which suite is which in `<scratch>/versions.txt`; no file the reviewer reads names it.
2. Run the cases, from the suite's folder (`cd <scratch>/suite-a`), once per model and version:

   ```bash
   claude plugin eval ./desflow --model sonnet --runs 3 --ablation with-without \
     --scaffold --keep-temp --allow-tools Bash Write Edit --no-publish --trust-plugin -j 4 \
     --output-dir <scratch>/results/a-sonnet --report <scratch>/results/a-sonnet.html \
     --json <scratch>/results/a-sonnet.json
   ```

   This is the "before" Sonnet suite, whose arm without the skill is the baseline, version `none`: both arms run `--runs` times. The other five commands use `--ablation none`: Sonnet on suite b with `--runs 3`, and Haiku and Opus on each suite with `--runs 1`. `--output-dir` and `--report` keep the results outside the skill copy, which every later run can read.

   The 12e3 "before" runs were made in `p12e3/before20/` (the suite `before20/desflow`) in parts, because the usage limit stopped them. Step 3 reads each file as follows:

   | Result file | How it was made | Step 3 arguments |
   | --- | --- | --- |
   | `result-sonnet-r1-baseline.json` | Sonnet, `--runs 1 --ablation with-without`: `before` run 1 (arm `with`) and `none` run 1 (arm `without`) | `--version before` |
   | `result-sonnet-r23.json` | Sonnet, `--runs 2 --ablation none`: `before` runs 2 and 3 | `--version before --first-run 2` |
   | `result-sonnet-baseline-r23.json` | Sonnet, `--runs 2 --ablation none` on `before20/noskill/desflow`, the suite with no `SKILL.md`, references, scripts or assets, because the command has no mode for the arm without the skill alone: `none` runs 2 and 3, in the arm `with`. `noskill/cmp_init.py` checked that a run's init message (model, tools, skills, plugins, commands, agents, MCP servers, permission mode) is the same as that of the arm `without` of run 1. | `--version none --first-run 2` |
   | `result-haiku.json` | Haiku, `--runs 1 --ablation none`: 9 runs with no error; the other 11 stopped at the usage limit and are not graded | `--version before` (exits 1 and prints `-` for the 11) |
   | `result-haiku-<case>.json` | Haiku, `--case <case>`: the 11 reruns | `--version before` |
   | `result-opus.json` | Opus, `--runs 1 --ablation none` | `--version before` |
3. Back in the repository's folder, list the kept runs of each result file: `.venv/bin/python scripts/skill/evals/kept_runs.py <scratch>/results/a-sonnet.json --version before`. Each line is the task (the case's name, such as `ds-a3`), the version, the run number and the kept folder `<root>`. `--version` names the version of the arm `with` (`none` for a file whose arm `with` ran with no skill), and `--first-run N` numbers a file's runs from N. For a run with an error, such as one whose `setup.sh` failed or that stopped at the usage limit, the script prints `-` and the error in place of the folder and exits 1; that run is not graded, and is run again with `--case <task>`.
4. Open each kept folder, which the command leaves at mode 500 with `sealed/` at 000: `chmod 700 <root> <root>/sealed`. Run nothing inside it, as the command warns, because a tool that reads its configuration from the folder would run the agent's files; `grade.py` copies the workspace before it runs anything.
5. Grade each run, giving it the next number `<n>` of a counter, and append `<n>`, the version, the model, the task and the run number to `<scratch>/review/index.tsv`, which no reviewer reads:

   ```bash
   <scratch>/suite-a/py/bin/python3.12 scripts/skill/evals/grade.py <root> --task <task> \
     --python-dir <scratch>/suite-a/py --check-python-dir <scratch>/suite-b/py \
     --skill <scratch>/suite-a/desflow \
     --other-skill <scratch>/suite-b/desflow --records-out <scratch>/review/<n> \
     --version <version> --model <model> --run <run> >> <scratch>/grades/<version>-<model>.jsonl
   ```

   `<task>` is the case's name as step 3 prints it. Use the prepared Python of the run's own suite for `--python-dir`, which runs the forms and the tests as the agent ran them. `--check-python-dir` is the Python of the after suite, for every run: with it `grade.py` runs each form through `scripts/check_model.py` of the current tree to set the seed (K4, K1) and to find draws desflow does not seed, as `check_model.py` does (K13), so both versions are measured by the same code. A run of version `none` uses the one of suite a, with no `--skill` or `--other-skill`, because that run had no skill, and `--api-page <scratch>/suite-a/desflow/references/api.md`, so that its K6 is judged against the page of the version it is compared with. A run with the skill has its K6 judged against `references/api.md` of its own `--skill`.
6. `grade.py` wrote `<scratch>/review/<n>/`, the folder `review_rubric.md`, What the reviewer gets, describes. Check that it holds `trace.jsonl`; a run without it is run again. Then `.venv/bin/python scripts/skill/evals/check_hidden.py <scratch>/review/*` must print nothing and exit 0: no file names `before20`, `noskill`, `suite-a`, `suite-b`, or `before` or `after` as a part of a path.
7. Start one fresh Opus reviewer per folder (`model: opus`), with this message, where `<rubric>` is the path of `review_rubric.md` and `<folder>` is `<scratch>/review/<n>`:

   ```text
   Read <rubric> in full. Then read every file of the folder <folder> as it sets out, including trace.jsonl in full, and judge the run against it. Run nothing and change no file. Reply with the JSON lines the rubric's Output section sets out, and nothing else.
   ```

   Save the reply as `<scratch>/reviews/<n>.jsonl`, setting on each line the task, version, model and run that `index.tsv` gives for `<n>`, in place of any the reviewer wrote. The task is the case's name (`ds-a3`), the only form `summarise.py` takes.
8. Make the table: `.venv/bin/python scripts/skill/evals/summarise.py --grades <scratch>/grades/*.jsonl --reviews <scratch>/reviews/*.jsonl`. It exits 1 and lists each run with a line missing or repeated, or not in the plan; such a run is graded or reviewed again, after its old lines are removed.

## Counts

Each suite holds the 20 cases of `cases/`. Sonnet runs each case three times on suite a with the skill (`before`, runs 1 to 3) and three times without it (`none`, runs 1 to 3), and three times on suite b (`after`): 180 runs. Haiku and Opus run each case once on each suite (run 1): 80 runs. All 260 are graded by `grade.py` and read by the reviewer. `summarise.py` expects exactly these runs, from the folder names of `cases/`.
