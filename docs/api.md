---
layout: page
title: API
---

# API Reference

## Core API

### Initiate_process

```python
from xspatula import Initiate_process
```

Primary runtime bootstrapper.

Parameters depend on original project contracts and should remain unchanged.

---

### Structure_processes

```python
from xspatula.lib import Structure_processes
```

Creates runtime process structures used by process dispatching.

---

### Project_login

```python
from xspatula.lib import Project_login
```

Project login and session initialization utilities.

---

### Get_set_database_session

```python
from xspatula.lib.login import Get_set_database_session
```

Database session management helper.

---

## Setup API

### Initiate_database

```python
from xspatula.setup import Initiate_database
```

Database initialization entrypoint.

---

### Run_process

```python
from xspatula.setup import Run_process
```

Primary process execution dispatcher.