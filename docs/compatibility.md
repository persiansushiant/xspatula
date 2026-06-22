---
layout: page
title: Compatibility
---

# Compatibility

## Supported New Imports

```python
from xspatula import Initiate_process

from xspatula.setup import (
    Initiate_database,
    Run_process
)
```

---

## Supported Legacy Imports

```python
from src.lib import Initiate_process

from src_setup.lib_setup import (
    Initiate_database,
    Run_process
)
```

---

## Compatibility Rules

1. Preserve notebook execution.
2. Preserve JSON contracts.
3. Preserve runtime conventions.
4. Preserve process dispatch behavior.
5. Preserve naming conventions.

---

## Refactoring Policy

Allowed:

- package extraction
- dependency cleanup
- packaging
- testing
- documentation

Not Allowed:

- process redesign
- notebook redesign
- JSON redesign
- dispatch redesign