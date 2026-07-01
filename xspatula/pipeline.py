from __future__ import annotations

import json
import re
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

    This class does not render UI.
    It only builds a structured graph contract that can later support:
    - pipeline visualization
    - execution animation
    - data lineage
    - audit trail
    - graph export
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
        process_files = self._discover_process_files(setup_path, pilot_data)
        process_json_files = self._discover_process_json_files(setup_path, process_files)

        token_count = max(len(process_json_files), 1)

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
                metadata={
                    "description": "Reads process definitions and maps them to Python actions.",
                    "input_file_count": len(process_json_files),
                },
                trace=self._trace(
                    role="dispatcher",
                    source="xspatula_runtime",
                    setup_path=setup_path,
                    requested_value=None,
                    resolved_path=None,
                    extra={
                        "input_files": len(process_json_files),
                    },
                ),
                lineage=self._lineage(
                    inputs=["process_json_references"],
                    outputs=["action_calls"],
                    reads=[file["absolute_path"] for file in process_json_files],
                    transforms=["dispatch_process_json_to_action_call"],
                ),
                audit=self._audit(),
            ),
            PipelineNode(
                id="python_actions",
                label="Python Actions",
                type="python_actions",
                expandable=True,
                metadata={
                    "count": len(process_json_files),
                    "files": process_json_files,
                    "description": "Resolved Python-level operations executed by Xspatula.",
                },
                trace=self._trace(
                    role="python_actions",
                    source="dispatcher_resolution",
                    setup_path=setup_path,
                    requested_value=None,
                    resolved_path=None,
                    extra={
                        "input_files": len(process_json_files),
                    },
                ),
                lineage=self._lineage(
                    inputs=["action_calls"],
                    outputs=["database_operations", "metadata_operations"],
                    transforms=["execute_python_actions"],
                ),
                audit=self._audit(),
            ),
            PipelineNode(
                id="metadata_registry",
                label="Metadata Registry",
                type="metadata_registry",
                metadata={
                    "description": "Stores process definitions, parameters, permissions and mappings.",
                    "database_objects": [
                        "root_process",
                        "process",
                        "parameter",
                        "permissions",
                        "schema_table_mappings",
                    ],
                },
                trace=self._trace(
                    role="metadata_registry",
                    source="xspatula_database_metadata",
                    setup_path=setup_path,
                    requested_value=None,
                    resolved_path=str(setup_path),
                ),
                lineage=self._lineage(
                    inputs=["metadata_operations"],
                    outputs=["registered_process_metadata"],
                    writes=[
                        "root_process",
                        "process",
                        "parameter",
                        "permissions",
                        "schema_table_mappings",
                    ],
                    database_objects=[
                        "root_process",
                        "process",
                        "parameter",
                        "permissions",
                        "schema_table_mappings",
                    ],
                    transforms=["register_framework_metadata"],
                ),
                audit=self._audit(),
            ),
            PipelineNode(
                id="postgresql",
                label="PostgreSQL",
                type="postgresql",
                metadata={
                    "engine": "postgresql",
                    "description": "Final persisted database state.",
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
                    writes=["postgresql_database"],
                    database_objects=["postgresql_database"],
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
                "contract_version": "2.5.0",
                "setup_path": str(setup_path),
                "created_at": self.created_at,
                "traceability": True,
                "lineage_ready": True,
                "audit_ready": True,
                "job_name": job_name,
                "process_file_count": len(process_files),
                "process_json_file_count": len(process_json_files),
                "capabilities": {
                    "graph_visualization": True,
                    "token_animation": True,
                    "node_expansion": True,
                    "traceability": True,
                    "data_lineage": True,
                    "audit_trail": True,
                    "graph_export": True,
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