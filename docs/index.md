---
layout: home
title: Xspatula
---

# Xspatula

Compatibility-first packaging of the original Xspatula framework.

The purpose of this project is to make the original Xspatula codebase installable and importable as a Python package without breaking notebook workflows, process contracts, or JSON-based runtime configuration.

---

## Design Principles

- Compatibility First
- Preserve Existing Notebooks
- Preserve Existing JSON Contracts
- Preserve Existing Process Dispatch Logic
- Avoid Architectural Rewrites Until Behavior Is Documented

---

## Installation

```bash
git clone https://github.com/persiansushiant/xspatula.git
cd xspatula

python -m venv .venv
.venv\Scripts\activate

pip install -e .
```

---

## Quick Start

```python
from xspatula import Initiate_process

process = Initiate_process(
    notebook_FP,
    scheme_file,
    process_file
)
```

Database setup:

```python
from xspatula.setup import Initiate_database

Initiate_database(
    notebook_FP,
    scheme_file,
    job_file
)
```

---

## Legacy Compatibility

The following notebook imports remain supported:

```python
from src.lib import Initiate_process

from src_setup.lib_setup import (
    Initiate_database,
    Run_process
)
```

