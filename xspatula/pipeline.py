from __future__ import annotations

import json
import re
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class PipelineNode:
    id: str
    label: str
    type: str
    status: str = "pending"
    expandable: bool = False
    metadata: Dict[str, Any] = field(default_factory=dict)
    trace: Dict[str, Any] = field(default_factory=dict)
    lineage: Dict[str, Any] = field(default_factory=dict)
    audit: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineEdge:
    source: str
    target: str
    label: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    trace: Dict[str, Any] = field(default_factory=dict)
    lineage: Dict[str, Any] = field(default_factory=dict)
    audit: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineGraph:
    nodes: List[PipelineNode]
    edges: List[PipelineEdge]
    execution: List[Dict[str, Any]]
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "nodes": [asdict(node) for node in self.nodes],
            "edges": [asdict(edge) for edge in self.edges],
            "execution": self.execution,
            "metadata": self.metadata,
        }


class XspatulaPipeline:
    """
    Lineage-ready pipeline graph builder.

    This builder does not execute Xspatula.
    It inspects the selected Scheme / Job / Pilot / Process Files and returns
    a graph contract that can support:

    - pipeline visualization
    - execution animation
    - dispatcher/action discovery
    - data lineage
    - audit trail
    - future DB registry lookup
    """

    def __init__(self, facade: Any):
        self.facade = facade
        self.created_at = datetime.now(timezone.utc).isoformat()

    def build(self) -> Dict[str, Any]:
        setup_path = Path(self.facade.setup_path).resolve()

        scheme_path = self._resolve_path(setup_path, self.facade.scheme_file)
        pilot_path = self._resolve_path(setup_path, self.facade.pilot_file)

        scheme_data = self._read_json_safe(scheme_path)
        pilot_data = self._read_json_safe(pilot_path)

        job_name = self._infer_job_name()

        process_files = self._discover_process_files(
            setup_path=setup_path,
            pilot_data=pilot_data,
        )

        process_json_files = self._discover_process_json_files(
            setup_path=setup_path,
            process_files=process_files,
        )

        action_inventory = self._discover_actions_from_process_json_files(
            process_json_files=process_json_files,
        )

        registry_inventory = self._discover_metadata_registry_from_actions(
            action_inventory=action_inventory,
        )

        process_id_counts = Counter(
            action["process_id"] for action in action_inventory if action.get("process_id")
        )

        database_objects = self._collect_database_objects(action_inventory)

        token_count = max(len(action_inventory), 1)

        nodes = [
            self._file_node(
                node_id="scheme",
                label="Scheme",
                node_type="scheme",
                path=scheme_path,
                setup_path=setup_path,
                requested_value=self.facade.scheme_file,
                description="Execution environment: database settings, users, project paths and defaults.",
                parsed_keys=list(scheme_data.keys()) if isinstance(scheme_data, dict) else [],
                lineage_outputs=["execution_context"],
            ),
            PipelineNode(
                id="job",
                label="Job",
                type="job",
                metadata={
                    "name": job_name,
                    "description": "The selected execution goal.",
                },
                trace=self._trace(
                    role="job",
                    source="inferred_from_selected_pilot",
                    setup_path=setup_path,
                    requested_value=self.facade.pilot_file,
                    resolved_path=None,
                    extra={
                        "selected_pilot": self.facade.pilot_file,
                        "resolved_name": job_name,
                    },
                ),
                lineage=self._lineage(
                    inputs=["execution_context"],
                    outputs=["selected_job"],
                    transforms=["pilot_name_to_job_name"],
                ),
                audit=self._audit(),
            ),
            self._file_node(
                node_id="pilot",
                label="Pilot",
                node_type="pilot",
                path=pilot_path,
                setup_path=setup_path,
                requested_value=self.facade.pilot_file,
                description="Execution order definition. It points Xspatula to process files.",
                parsed_keys=list(pilot_data.keys()) if isinstance(pilot_data, dict) else [],
                lineage_inputs=["selected_job"],
                lineage_outputs=["process_file_references"],
            ),
            PipelineNode(
                id="process_files",
                label="Process Files",
                type="process_files",
                expandable=True,
                metadata={
                    "count": len(process_files),
                    "files": process_files,
                    "path": process_files[0]["absolute_path"] if process_files else None,
                    "absolute_path": process_files[0]["absolute_path"] if process_files else None,
                    "description": "Process files that point to executable JSON process definitions.",
                },
                trace=self._trace(
                    role="process_files",
                    source="discovered_from_pilot",
                    setup_path=setup_path,
                    requested_value=None,
                    resolved_path=process_files[0]["absolute_path"] if process_files else None,
                    extra={
                        "resolved_count": len(process_files),
                    },
                ),
                lineage=self._lineage(
                    inputs=["process_file_references"],
                    outputs=["process_json_references"],
                    reads=[file["absolute_path"] for file in process_files],
                    transforms=["parse_process_files"],
                ),
                audit=self._audit(),
            ),
            PipelineNode(
                id="dispatcher",
                label="Dispatcher",
                type="dispatcher",
                expandable=True,
                metadata={
                    "description": "Reads process definitions and maps process_id values to executable Python actions.",
                    "action_count": len(action_inventory),
                    "unique_process_ids": sorted(process_id_counts.keys()),
                    "process_id_counts": dict(process_id_counts),
                    "dispatcher_table": self._build_dispatcher_table(
                        action_inventory=action_inventory,
                        registry_inventory=registry_inventory,
                    ),
                },
                trace=self._trace(
                    role="dispatcher",
                    source="xspatula_runtime_and_process_metadata",
                    setup_path=setup_path,
                    requested_value=None,
                    resolved_path=None,
                    extra={
                        "input_json_files": len(process_json_files),
                        "resolved_action_count": len(action_inventory),
                        "unique_process_id_count": len(process_id_counts),
                    },
                ),
                lineage=self._lineage(
                    inputs=["process_json_references"],
                    outputs=["action_calls"],
                    reads=[file["absolute_path"] for file in process_json_files],
                    transforms=["resolve_process_id_to_action"],
                ),
                audit=self._audit(),
            ),
            PipelineNode(
                id="python_actions",
                label="Python Actions",
                type="python_actions",
                expandable=True,
                metadata={
                    "count": len(action_inventory),
                    "actions": action_inventory,
                    "description": "Resolved Python-level operations inferred from process JSON definitions.",
                },
                trace=self._trace(
                    role="python_actions",
                    source="dispatcher_resolution",
                    setup_path=setup_path,
                    requested_value=None,
                    resolved_path=None,
                    extra={
                        "input_json_files": len(process_json_files),
                        "action_count": len(action_inventory),
                    },
                ),
                lineage=self._lineage(
                    inputs=["action_calls"],
                    outputs=["database_operations", "metadata_operations"],
                    reads=[file["absolute_path"] for file in process_json_files],
                    writes=database_objects,
                    database_objects=database_objects,
                    transforms=["execute_resolved_python_actions"],
                ),
                audit=self._audit(),
            ),
            PipelineNode(
                id="metadata_registry",
                label="Metadata Registry",
                type="metadata_registry",
                expandable=True,
                metadata={
                    "description": "Stores process definitions, parameters, permissions and mappings.",
                    "registry_source": "seed_json_and_future_database_registry",
                    "registered_process_count": len(registry_inventory.get("registered_processes", [])),
                    "registered_parameters_count": len(registry_inventory.get("registered_parameters", [])),
                    "registered_processes": registry_inventory.get("registered_processes", []),
                    "registered_parameters": registry_inventory.get("registered_parameters", []),
                    "database_objects": [
                        "process.root_process",
                        "process.process",
                        "process.process_parameter",
                        "process.process_parameter_set_value",
                        "process.process_parameter_minmax",
                        "process.process_parameter_schema_table",
                        "process.process_parameter_permission",
                        "process.process_parameter_default",
                    ],
                },
                trace=self._trace(
                    role="metadata_registry",
                    source="process_schema_seed_json",
                    setup_path=setup_path,
                    requested_value=None,
                    resolved_path=str(setup_path),
                    extra={
                        "future_source": "live_postgresql_registry",
                    },
                ),
                lineage=self._lineage(
                    inputs=["metadata_operations"],
                    outputs=["registered_process_metadata"],
                    writes=[
                        "process.root_process",
                        "process.process",
                        "process.process_parameter",
                        "process.process_parameter_set_value",
                        "process.process_parameter_minmax",
                        "process.process_parameter_schema_table",
                        "process.process_parameter_permission",
                        "process.process_parameter_default",
                    ],
                    database_objects=[
                        "process.root_process",
                        "process.process",
                        "process.process_parameter",
                        "process.process_parameter_set_value",
                        "process.process_parameter_minmax",
                        "process.process_parameter_schema_table",
                        "process.process_parameter_permission",
                        "process.process_parameter_default",
                    ],
                    transforms=["register_framework_metadata"],
                ),
                audit=self._audit(),
            ),
            PipelineNode(
                id="postgresql",
                label="PostgreSQL",
                type="postgresql",
                expandable=True,
                metadata={
                    "engine": "postgresql",
                    "description": "Final persisted database state.",
                    "database_objects": database_objects,
                    "database_object_count": len(database_objects),
                },
                trace=self._trace(
                    role="postgresql",
                    source="database_engine",
                    setup_path=setup_path,
                    requested_value=None,
                    resolved_path=str(setup_path),
                ),
                lineage=self._lineage(
                    inputs=["database_operations", "registered_process_metadata"],
                    outputs=["persisted_world_state"],
                    writes=database_objects,
                    database_objects=database_objects,
                    transforms=["persist_database_state"],
                ),
                audit=self._audit(),
            ),
        ]

        edges = [
            self._edge("scheme", "job", "selects", "execution_context_to_job"),
            self._edge("job", "pilot", "loads", "job_to_pilot"),
            self._edge("pilot", "process_files", "resolves", "pilot_to_process_files"),
            self._edge("process_files", "dispatcher", "dispatches", "process_files_to_dispatcher", token_count),
            self._edge("dispatcher", "python_actions", "maps", "dispatcher_to_python_actions", token_count),
            self._edge("python_actions", "metadata_registry", "registers", "actions_to_metadata_registry"),
            self._edge("metadata_registry", "postgresql", "persists", "metadata_registry_to_postgresql"),
        ]

        execution = [
            self._execution_step("scheme", "job", tokens=1),
            self._execution_step("job", "pilot", tokens=1),
            self._execution_step("pilot", "process_files", tokens=1),
            self._execution_step(
                "process_files",
                "dispatcher",
                tokens=token_count,
                parallel=True,
                burst=True,
                token_label="process_definition",
            ),
            self._execution_step(
                "dispatcher",
                "python_actions",
                tokens=token_count,
                parallel=True,
                token_label="action_call",
            ),
            self._execution_step(
                "python_actions",
                "metadata_registry",
                tokens=1,
                merge=True,
                token_label="metadata_update",
            ),
            self._execution_step(
                "metadata_registry",
                "postgresql",
                tokens=1,
                token_label="database_state",
            ),
        ]

        return PipelineGraph(
            nodes=nodes,
            edges=edges,
            execution=execution,
            metadata={
                "kind": "xspatula_pipeline_graph",
                "contract_version": "2.6.0",
                "setup_path": str(setup_path),
                "created_at": self.created_at,
                "traceability": True,
                "lineage_ready": True,
                "audit_ready": True,
                "dispatcher_ready": True,
                "action_discovery_ready": True,
                "job_name": job_name,
                "process_file_count": len(process_files),
                "process_json_file_count": len(process_json_files),
                "action_count": len(action_inventory),
                "unique_process_ids": sorted(process_id_counts.keys()),
                "database_object_count": len(database_objects),
                "capabilities": {
                    "graph_visualization": True,
                    "token_animation": True,
                    "node_expansion": True,
                    "traceability": True,
                    "dispatcher_action_discovery": True,
                    "data_lineage": True,
                    "audit_trail": True,
                    "graph_export": True,
                    "future_live_database_registry_lookup": True,
                },
            },
        ).to_dict()

    def _file_node(
        self,
        node_id: str,
        label: str,
        node_type: str,
        path: Optional[Path],
        setup_path: Path,
        requested_value: Optional[str],
        description: str,
        parsed_keys: Optional[List[str]] = None,
        lineage_inputs: Optional[List[str]] = None,
        lineage_outputs: Optional[List[str]] = None,
    ) -> PipelineNode:
        metadata = self._file_metadata(
            path=path,
            setup_path=setup_path,
            requested_value=requested_value,
            role=node_type,
            description=description,
            parsed_keys=parsed_keys or [],
        )

        return PipelineNode(
            id=node_id,
            label=label,
            type=node_type,
            metadata=metadata,
            trace=metadata["trace"],
            lineage=self._lineage(
                inputs=lineage_inputs or [],
                outputs=lineage_outputs or [],
                reads=[metadata["absolute_path"]] if metadata.get("absolute_path") else [],
            ),
            audit=self._audit(),
        )

    def _edge(
        self,
        source: str,
        target: str,
        label: str,
        movement: str,
        tokens: int = 1,
    ) -> PipelineEdge:
        return PipelineEdge(
            source=source,
            target=target,
            label=label,
            metadata={
                "tokens": tokens,
            },
            trace={
                "source_node": source,
                "target_node": target,
                "movement": movement,
                "created_at": self.created_at,
            },
            lineage={
                "enabled": True,
                "movement": movement,
                "tokens": tokens,
            },
            audit=self._audit(),
        )

    def _execution_step(
        self,
        source: str,
        target: str,
        tokens: int = 1,
        parallel: bool = False,
        burst: bool = False,
        merge: bool = False,
        token_label: Optional[str] = None,
    ) -> Dict[str, Any]:
        return {
            "from": source,
            "to": target,
            "tokens": tokens,
            "parallel": parallel,
            "burst": burst,
            "merge": merge,
            "token_label": token_label,
        }

    def _infer_job_name(self) -> str:
        pilot = str(self.facade.pilot_file or "").lower()

        if "setup_db" in pilot:
            return "setup_db"

        if "setup_process" in pilot:
            return "setup_processes"

        if "delete" in pilot:
            return "delete_db"

        return str(self.facade.pilot_file or "unknown_job")

    def _resolve_path(self, setup_path: Path, value: Optional[str]) -> Optional[Path]:
        if not value:
            return None

        raw = Path(value)

        if raw.is_absolute():
            return raw.resolve()

        direct = (setup_path / raw).resolve()
        if direct.exists():
            return direct

        matches = list(setup_path.rglob(raw.name))
        if matches:
            return matches[0].resolve()

        return direct

    def _read_json_safe(self, path: Optional[Path]) -> Dict[str, Any]:
        if not path or not path.exists():
            return {}

        try:
            with path.open("r", encoding="utf-8") as file:
                return json.load(file)
        except Exception:
            return {}

    def _read_text_safe(self, path: Optional[Path]) -> str:
        if not path or not path.exists():
            return ""

        try:
            return path.read_text(encoding="utf-8")
        except Exception:
            return ""

    def _discover_process_files(
        self,
        setup_path: Path,
        pilot_data: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        refs = self._extract_file_references(
            pilot_data,
            allowed_suffixes={".txt", ".json"},
        )

        files = []

        for ref in refs:
            resolved = self._resolve_path(setup_path, ref)
            if resolved:
                files.append(
                    self._file_metadata(
                        path=resolved,
                        setup_path=setup_path,
                        requested_value=ref,
                        role="process_file",
                        description="Discovered process file.",
                    )
                )

        return self._unique_files(files)

    def _discover_process_json_files(
        self,
        setup_path: Path,
        process_files: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        files = []

        for process_file in process_files:
            process_path = Path(process_file["absolute_path"])
            text = self._read_text_safe(process_path)

            refs = self._extract_file_references(
                text,
                allowed_suffixes={".json"},
            )

            for ref in refs:
                resolved = self._resolve_path(setup_path, ref)

                if resolved:
                    files.append(
                        self._file_metadata(
                            path=resolved,
                            setup_path=setup_path,
                            requested_value=ref,
                            role="process_json",
                            description="JSON process definition discovered from process file.",
                            discovered_from=str(process_path),
                        )
                    )

        return self._unique_files(files)

    def _discover_actions_from_process_json_files(
        self,
        process_json_files: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        actions = []

        for file_meta in process_json_files:
            path = Path(file_meta["absolute_path"])
            data = self._read_json_safe(path)

            raw_processes = data.get("process", [])

            if isinstance(raw_processes, dict):
                raw_processes = [raw_processes]

            if not isinstance(raw_processes, list):
                continue

            for index, process in enumerate(raw_processes):
                if not isinstance(process, dict):
                    continue

                action = self._normalise_process_action(
                    process=process,
                    file_meta=file_meta,
                    process_index=index,
                )

                actions.append(action)

        return actions

    def _normalise_process_action(
        self,
        process: Dict[str, Any],
        file_meta: Dict[str, Any],
        process_index: int,
    ) -> Dict[str, Any]:
        process_id = process.get("process_id") or process.get("process") or "unknown_process"

        parameters = process.get("parameters", {})
        if not isinstance(parameters, dict):
            parameters = {}

        schema = parameters.get("schema") or parameters.get("in_schema")
        table = parameters.get("table") or parameters.get("in_table")
        db = parameters.get("db")

        command = parameters.get("command")
        command_summary = self._summarise_command(command)

        database_object = self._database_object_name(schema=schema, table=table)

        audit_hint = self._extract_audit_hint(command)

        return {
            "id": f"{file_meta.get('name')}::{process_index}",
            "process_id": process_id,
            "python_function_candidate": self._process_id_to_function_candidate(process_id),
            "dispatcher_lookup_key": process_id,
            "dispatcher_strategy": "process_id_to_python_action",
            "source_file": file_meta.get("absolute_path"),
            "source_file_name": file_meta.get("name"),
            "source_relative_path": file_meta.get("relative_path"),
            "process_index": process_index,
            "overwrite": process.get("overwrite"),
            "delete": process.get("delete"),
            "db": db,
            "schema": schema,
            "table": table,
            "database_object": database_object,
            "parameters_keys": sorted(parameters.keys()),
            "command_summary": command_summary,
            "audit_hint": audit_hint,
            "lineage": {
                "reads": [file_meta.get("absolute_path")],
                "writes": [database_object] if database_object else [],
                "database_objects": [database_object] if database_object else [],
                "transforms": [f"{process_id}_definition_to_action"],
            },
        }

    def _process_id_to_function_candidate(self, process_id: str) -> str:
        clean = str(process_id or "unknown_process").strip()

        if not clean:
            clean = "unknown_process"

        parts = [part for part in clean.split("_") if part]
        pascal = "".join(part.capitalize() for part in parts)

        return f"{pascal}Action"

    def _summarise_command(self, command: Any) -> Dict[str, Any]:
        if isinstance(command, list):
            return {
                "type": "list",
                "item_count": len(command),
                "preview": command[:5],
            }

        if isinstance(command, dict):
            columns = command.get("columns", [])
            values = command.get("values", [])

            return {
                "type": "dict",
                "columns": columns,
                "column_count": len(columns) if isinstance(columns, list) else None,
                "row_count": len(values) if isinstance(values, list) else None,
            }

        if command is None:
            return {
                "type": "none",
            }

        return {
            "type": type(command).__name__,
            "preview": str(command)[:240],
        }

    def _extract_audit_hint(self, command: Any) -> Dict[str, Any]:
        result = {
            "created_by": None,
            "created_at": None,
            "updated_by": None,
            "updated_at": None,
            "columns": [],
        }

        if isinstance(command, dict):
            columns = command.get("columns", [])

            if isinstance(columns, list):
                result["columns"] = columns

                for creator_key in ("creator", "creator_id", "created_by", "user_name", "user_id"):
                    if creator_key in columns:
                        result["created_by"] = {
                            "source": "command.columns",
                            "field": creator_key,
                        }
                        break

                for created_key in ("create_timestamp", "created_at", "created"):
                    if created_key in columns:
                        result["created_at"] = {
                            "source": "command.columns",
                            "field": created_key,
                        }
                        break

                for updated_key in ("last_update_timestamp", "updated_at", "updated_by"):
                    if updated_key in columns:
                        result["updated_at"] = {
                            "source": "command.columns",
                            "field": updated_key,
                        }
                        break

        if isinstance(command, list):
            result["columns"] = command

            joined = " ".join(str(item).lower() for item in command)

            if "creator" in joined:
                result["created_by"] = {
                    "source": "ddl",
                    "field": "creator",
                }

            if "create_timestamp" in joined or "created_at" in joined:
                result["created_at"] = {
                    "source": "ddl",
                    "field": "create_timestamp",
                }

            if "last_update_timestamp" in joined or "updated_at" in joined:
                result["updated_at"] = {
                    "source": "ddl",
                    "field": "last_update_timestamp",
                }

        return result

    def _discover_metadata_registry_from_actions(
        self,
        action_inventory: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        registered_processes = []
        registered_parameters = []

        for action in action_inventory:
            if action.get("process_id") != "table_insert":
                continue

            if action.get("schema") != "process":
                continue

            source_file = action.get("source_file")
            data = self._read_json_safe(Path(source_file)) if source_file else {}
            raw_processes = data.get("process", [])

            if isinstance(raw_processes, dict):
                raw_processes = [raw_processes]

            for process in raw_processes:
                params = process.get("parameters", {})
                if not isinstance(params, dict):
                    continue

                if params.get("schema") != "process":
                    continue

                table = params.get("table")
                command = params.get("command", {})

                if not isinstance(command, dict):
                    continue

                columns = command.get("columns", [])
                values = command.get("values", [])

                if not isinstance(columns, list) or not isinstance(values, list):
                    continue

                for row in values:
                    if not isinstance(row, list):
                        continue

                    record = self._row_to_record(columns, row)
                    record["_source_file"] = source_file

                    if table == "process":
                        registered_processes.append(record)

                    if table == "process_parameter":
                        registered_parameters.append(record)

        return {
            "registered_processes": registered_processes,
            "registered_parameters": registered_parameters,
        }

    def _row_to_record(self, columns: List[str], row: List[Any]) -> Dict[str, Any]:
        record = {}

        for index, column in enumerate(columns):
            record[column] = row[index] if index < len(row) else None

        return record

    def _build_dispatcher_table(
        self,
        action_inventory: List[Dict[str, Any]],
        registry_inventory: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        registry_process_names = {
            item.get("process")
            for item in registry_inventory.get("registered_processes", [])
            if item.get("process")
        }

        grouped = defaultdict(list)

        for action in action_inventory:
            grouped[action.get("process_id")].append(action)

        table = []

        for process_id, actions in sorted(grouped.items()):
            table.append(
                {
                    "process_id": process_id,
                    "python_function_candidate": self._process_id_to_function_candidate(process_id),
                    "call_count": len(actions),
                    "registry_status": "registered" if process_id in registry_process_names else "runtime_or_core_action",
                    "example_source_file": actions[0].get("source_file") if actions else None,
                    "database_objects": sorted(
                        {
                            action.get("database_object")
                            for action in actions
                            if action.get("database_object")
                        }
                    ),
                }
            )

        return table

    def _collect_database_objects(self, action_inventory: List[Dict[str, Any]]) -> List[str]:
        objects = []

        for action in action_inventory:
            database_object = action.get("database_object")

            if database_object:
                objects.append(database_object)

        return sorted(set(objects))

    def _database_object_name(
        self,
        schema: Optional[str],
        table: Optional[str],
    ) -> Optional[str]:
        if schema and table:
            return f"{schema}.{table}"

        if table:
            return str(table)

        if schema:
            return str(schema)

        return None

    def _extract_file_references(
        self,
        data: Any,
        allowed_suffixes: set[str],
    ) -> List[str]:
        results: List[str] = []

        def inspect_string(value: str):
            normalized = value.replace("\\", "/")
            candidates = re.findall(r"[A-Za-z0-9_\-./]+(?:\.json|\.txt)", normalized)

            for candidate in candidates:
                candidate = candidate.strip().strip('"').strip("'").strip(",")

                if any(candidate.endswith(suffix) for suffix in allowed_suffixes):
                    results.append(candidate)

        def walk(value: Any):
            if isinstance(value, dict):
                for child in value.values():
                    walk(child)
            elif isinstance(value, list):
                for child in value:
                    walk(child)
            elif isinstance(value, str):
                inspect_string(value)

        walk(data)
        return results

    def _file_metadata(
        self,
        path: Optional[Path],
        setup_path: Path,
        requested_value: Optional[str],
        role: str,
        description: str,
        parsed_keys: Optional[List[str]] = None,
        discovered_from: Optional[str] = None,
    ) -> Dict[str, Any]:
        exists = bool(path and path.exists())
        absolute_path = str(path.resolve()) if path else None
        relative_path = self._relative_path(path, setup_path) if path else None

        trace = self._trace(
            role=role,
            source="file_system",
            setup_path=setup_path,
            requested_value=requested_value,
            resolved_path=absolute_path,
            extra={
                "relative_path": relative_path,
                "exists": exists,
                "discovered_from": discovered_from,
            },
        )

        return {
            "role": role,
            "name": path.name if path else None,
            "path": absolute_path,
            "absolute_path": absolute_path,
            "relative_path": relative_path,
            "exists": exists,
            "size": path.stat().st_size if exists else None,
            "suffix": path.suffix if path else None,
            "description": description,
            "parsed_keys": parsed_keys or [],
            "trace": trace,
        }

    def _trace(
        self,
        role: str,
        source: str,
        setup_path: Path,
        requested_value: Optional[str],
        resolved_path: Optional[str],
        extra: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        trace = {
            "role": role,
            "source": source,
            "requested_value": requested_value,
            "resolved_path": resolved_path,
            "setup_path": str(setup_path),
            "created_at": self.created_at,
        }

        if extra:
            trace.update(extra)

        return trace

    def _lineage(
        self,
        enabled: bool = True,
        inputs: Optional[List[str]] = None,
        outputs: Optional[List[str]] = None,
        reads: Optional[List[str]] = None,
        writes: Optional[List[str]] = None,
        transforms: Optional[List[str]] = None,
        database_objects: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        return {
            "enabled": enabled,
            "inputs": inputs or [],
            "outputs": outputs or [],
            "reads": reads or [],
            "writes": writes or [],
            "transforms": transforms or [],
            "database_objects": database_objects or [],
        }

    def _audit(self) -> Dict[str, Any]:
        return {
            "enabled": True,
            "events": [],
            "created_at": self.created_at,
            "created_by": None,
            "updated_at": None,
            "updated_by": None,
        }

    def _relative_path(self, path: Optional[Path], setup_path: Path) -> Optional[str]:
        if not path:
            return None

        try:
            return str(path.resolve().relative_to(setup_path.resolve()))
        except Exception:
            return str(path)

    def _unique_files(self, files: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        seen = set()
        unique = []

        for file in files:
            key = file.get("absolute_path") or file.get("path") or file.get("name")

            if key not in seen:
                seen.add(key)
                unique.append(file)

        return unique