---
layout: page
title: Installation
---

# Installation

## Clone

```bash
git clone https://github.com/persiansushiant/xspatula.git
cd xspatula
```

---

## Create Environment

Windows:

```bash
python -m venv .venv
.venv\Scripts\activate
```

Linux/macOS:

```bash
python -m venv .venv
source .venv/bin/activate
```

---

## Install

```bash
pip install -e .
```

---

## Verify Installation

```bash
python -c "import xspatula"
```

Expected result:

```text
No import errors
```

---

## Run Tests

```bash
pip install pytest
pytest