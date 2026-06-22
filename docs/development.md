---
layout: page
title: Development
---

# Development

## Local Setup

```bash
git clone https://github.com/persiansushiant/xspatula.git
cd xspatula

python -m venv .venv
.venv\Scripts\activate

pip install -e .
```

---

## Running Tests

```bash
pytest
```

---

## Building Package

```bash
pip install build
python -m build
```

Artifacts:

```text
dist/
├── xspatula-<version>.tar.gz
└── xspatula-<version>-py3-none-any.whl
```

---

## Release Process

Commit changes:

```bash
git add .
git commit -m "Release preparation"
```

Push:

```bash
git push origin main
```

Create tag:

```bash
git tag v0.1.0
git push origin v0.1.0
```

Create GitHub Release from the published tag.

---

## Documentation

Documentation is built using:

- Jekyll
- GitHub Pages
- Markdown

Documentation source lives inside:

```text
docs/
```