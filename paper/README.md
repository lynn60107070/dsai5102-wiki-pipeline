# Paper: how we write it

Overleaf collaboration costs money, so we write the paper in GitHub, like the code. **Edit only your own
section files in `paper/sections/` on your branch and open a PR.** Lynn compile-checks it, merges it, and
copies it into the Overleaf project, which makes the PDF.

Report requirements (from the brief + Dr. Bouguelia, 28 Oct): PDF, **5–10 pages excluding references and
appendices**, any template (we use a plain article), covering problem and dataset, architecture with a
diagram, design choices and justification, implementation, results and insights, discussion and conclusion.

## Who writes what

| File | Section | Owner |
|---|---|---|
| `main.tex` | Title, authors, packages, section order | Lynn |
| `sections/01_introduction.tex` | Problem, motivation, question, contributions | Sumaiya |
| `sections/02_dataset.tex` | The three sources + data profile | Sumaiya |
| `sections/03_architecture.tex` | Diagram + data flow | Lynn |
| `sections/04_design_choices.tex` | Tool table, design decisions | Lynn |
| `sections/05_implementation.tex` | Stream, batch, lake, warehouse, reproducibility | Lynn |
| `sections/06_results.tex` | Dashboard + insights | Sumaiya |
| `sections/07_discussion.tex` | Answer, limitations, extensions | Sumaiya |
| `refs.bib` | Shared bibliography (add entries, never edit others') | Both |
| `figures/` | Images (PDF or PNG), named `<section>_<what>.png`, e.g. `06_bot_share.png` | Both |

Each section file starts with `\todo{...}` notes saying what goes there. Replace them as you write.
They show in red in the PDF, so any left over are easy to spot.

## Workflow

1. `git checkout main && git pull`, then `git checkout -b sumaiya/paper-intro` (any name).
2. Edit **only your** section files (+ add your figures and bib entries).
3. Commit, push, open a PR into `main`.
4. Lynn compile-checks it, merges it, and copies it into the Overleaf project.

## Writing rules (so merges never clash)

- Labels: `sec:`, `fig:`, `tab:` + short name (`fig:bot-share`). Refer with `\cref{fig:bot-share}`.
- Cite with `\cite{key}`; add the entry to `refs.bib` in the same PR.
- Don't add `\usepackage` in a section file. Ask Lynn to add it to `main.tex`.
- Every figure and table gets a caption and is referred to in the text.
- All times in UTC; numbers from the frozen analysis window (28 Oct – 11 Nov 2026).

## Previewing your section (free)

Upload the `paper/` folder as a ZIP to your own free Overleaf project. It's just for viewing; the real
PDF only comes from Lynn's project.

- **macOS:** in Finder, right-click `paper` → *Compress "paper"*. Or in Terminal: `zip -r paper.zip paper`
- **Windows:** right-click `paper` → *Send to* → *Compressed (zipped) folder*
- Overleaf → *New Project* → *Upload Project* → choose the ZIP. Re-upload after big changes.
