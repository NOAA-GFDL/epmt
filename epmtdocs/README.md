Static Site Generator
----
This directory is where the static documentation site is generated using [MkDocs](https://www.mkdocs.org/).

Documentation source files live in `epmtdocs/docs/` and are referenced in `mkdocs.yml`.

A specific configuration worthy of note is:

`use_directory_urls: false`

This ensures hyperlinks generated lead to direct HTML files, not directories containing index.html files.

## Building Docs Locally

Install mkdocs:

```
pip install mkdocs
```

To serve a live-updating local preview:

```
mkdocs serve -f epmtdocs/mkdocs.yml
```

Then visit http://127.0.0.1:8000 in your browser.

To build the static site (outputs to `epmtdocs/site/`):

```
mkdocs build -f epmtdocs/mkdocs.yml
```

## GitHub Pages Deployment

Docs are automatically deployed to GitHub Pages when changes to `epmtdocs/` are pushed to `main`. The workflow is defined in `.github/workflows/deploy_docs.yml`.