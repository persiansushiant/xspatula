# Xspatula Architecture

## Purpose

Xspatula is being converted into an installable Python package with a compatibility-first approach.

The goal of the first phase is NOT redesign.

The goal is preserving all existing notebook workflows while making Xspatula importable as a Python library.

---

## Public API

### Core API

```python
from xspatula import Initiate_process
from xspatula.lib import Initiate_process
```

Legacy compatibility:

```python
from src.lib import Initiate_process
```

### Setup API

```python
from xspatula.setup import Initiate_database
from xspatula.setup import Run_process
```

Legacy compatibility:

```python
from src_setup.lib_setup import Initiate_database
from src_setup.lib_setup import Run_process
```

---

## Package Structure

```text
xspatula/
├── docs/
├── tests/
├── src/
│   ├── __init__.py
│   └── lib.py
├── src_setup/
│   ├── __init__.py
│   └── lib_setup.py
├── xspatula/
│   ├── __init__.py
│   ├── lib/
│   ├── postgres/
│   ├── setup/
│   └── utils/
├── pyproject.toml
├── README.md
└── .gitignore
```

---

## Entry Points

### Initiate_process

Primary runtime bootstrapper used by notebooks.

Responsibilities:

* Load scheme
* Resolve project paths
* Resolve process definitions
* Create runtime process structure

### Run_process

Primary process dispatcher.

Responsibilities:

* Execute configured process
* Execute jobs
* Execute pilot lists
* Execute pilot files

### Initiate_database

Database bootstrap entrypoint.

Responsibilities:

* Create schemas
* Create tables
* Create roles
* Delete database structures
* Setup environment

---

## Compatibility Rules

1. Preserve original function names.
2. Preserve original class names.
3. Preserve original JSON structures.
4. Preserve notebook imports.
5. Preserve process contracts.
6. Preserve scheme contracts.
7. Prefer compatibility over cleanup.

---

## Known Runtime Contracts

### Scheme

scheme JSON controls:

* project path
* database settings
* runtime configuration

### Process JSON

Process files describe:

* process flow
* execution order
* runtime parameters

### Job JSON

Job files describe:

* data import jobs
* translation jobs
* management jobs

### Pilot Contracts

Supported runtime dispatch patterns:

* pilot_list
* pilot_file
* single process

---

## Refactor Policy

Allowed:

* package extraction
* import cleanup
* dependency cleanup
* packaging

Not Allowed:

* process redesign
* notebook redesign
* JSON redesign
* dispatcher redesign

Until full compatibility coverage exists.
