---
layout: page
title: Architecture
---

# Architecture

## Current Goal

The current goal is packaging.

The current goal is NOT redesign.

Xspatula should first become a stable importable Python package before any architectural refactoring occurs.

---

# High-Level Structure

```text
xspatula
├── lib
├── postgres
├── setup
├── utils
├── src
└── src_setup
```

---

# Runtime Entry Points

## Initiate_process

Primary runtime bootstrapper.

Responsibilities:

- Load scheme
- Resolve project path
- Resolve process definition
- Initialize runtime structures

---

## Run_process

Primary process dispatcher.

Responsibilities:

- Execute process files
- Execute pilot files
- Execute pilot lists
- Execute jobs

---

## Initiate_database

Database bootstrapper.

Responsibilities:

- Create schemas
- Create tables
- Create database roles
- Setup environments

---

# Runtime Contracts

## Scheme Contract

Scheme JSON defines:

- project path
- environment settings
- database configuration

---

## Process Contract

Process JSON defines:

- execution flow
- runtime parameters
- process dependencies

---

## Job Contract

Job JSON defines:

- import jobs
- translation jobs
- management jobs

---

# Compatibility Policy

Preserve:

- names
- signatures
- JSON structures
- notebook workflows
- process dispatch behavior