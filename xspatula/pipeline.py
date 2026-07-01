from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional


@dataclass
class PipelineNode:
    id: str
    label: str
    type: str
    status: str = "pending"
    metadata: Dict[str, Any] = field(default_factory=dict)
    expandable: bool = False


@dataclass
class PipelineEdge:
    source: str
    target: str
    label: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineGraph:
    nodes: List[PipelineNode]
    edges: List[PipelineEdge]
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "nodes": [asdict(node) for node in self.nodes],
            "edges": [asdict(edge) for edge in self.edges],
            "metadata": self.metadata,
        }


class XspatulaPipeline:
    def __init__(self, facade: Any):
        self.facade = facade

    def build(self) -> Dict[str, Any]:
        setup_path = self._get_setup_path()

        scheme_path = self._resolve_path(setup_path, self.facade.scheme_file)
        pilot_path = self._resolve_path(setup_path, self.facade.pilot_file)

        scheme_data = self._read_json_safe(scheme_path)
        pilot_data = self._read_json_safe(pilot_path)

        process_files = self._discover_process_files(setup_path, pilot_data)
        job_bundle_files = self._discover_job_bundle_files(setup_path, process_files)

        nodes = [
            PipelineNode(
                id="scheme",
                label="Scheme",
                type="scheme",
                metadata=self._file_metadata_with_trace(
                    path=scheme_path,
                    setup_path=setup_path,
                    role="scheme",
                    requested_value=self.facade.scheme_file,
                    parsed_keys=list(scheme_data.keys()) if isinstance(scheme_data, dict) else [],
                ),
            ),
            PipelineNode(
                id="pilot",
                label="Pilot Configuration",
                type="pilot",
                metadata=self._file_metadata_with_trace(
                    path=pilot_path,
                    setup_path=setup_path,
                    role="pilot",
                    requested_value=self.facade.pilot_file,
                    parsed_keys=list(pilot_data.keys()) if isinstance(pilot_data, dict) else [],
                ),
            ),
            PipelineNode(
                id="process_file",
                label="Process File",
                type="process_file",
                metadata={
                    "count": len(process_files),
                    "files": process_files,
                    "path": process_files[0]["path"] if process_files else None,
                    "absolute_path": process_files[0]["absolute_path"] if process_files else None,
                    "trace": {
                        "role": "process_file",
                        "source": "discovered_from_pilot_configuration",
                        "setup_path": str(setup_path),
                        "resolved_count": len(process_files),
                    },
                },
                expandable=True,
            ),
            PipelineNode(
                id="job_bundle",
                label="Job Bundle",
                type="job_bundle",
                metadata={
                    "count": len(job_bundle_files),
                    "files": job_bundle_files,
                    "path": job_bundle_files[0]["path"] if job_bundle_files else None,
                    "absolute_path": job_bundle_files[0]["absolute_path"] if job_bundle_files else None,
                    "trace": {
                        "role": "job_bundle",
                        "source": "discovered_from_process_files",
                        "setup_path": str(setup_path),
                        "resolved_count": len(job_bundle_files),
                    },
                },
                expandable=True,
            ),
            PipelineNode(
                id="postgresql_action",
                label="PostgreSQL Action",
                type="postgresql_action",
                metadata={
                    "engine": "postgresql",
                    "path": str(setup_path),
                    "absolute_path": str(setup_path),
                    "trace": {
                        "role": "postgresql_action",
                        "source": "xspatula_execution_engine",
                        "setup_path": str(setup_path),
                    },
                },
            ),
        ]

        edges = [
            PipelineEdge(source="scheme", target="pilot"),
            PipelineEdge(source="pilot", target="process_file"),
            PipelineEdge(source="process_file", target="job_bundle"),
            PipelineEdge(source="job_bundle", target="postgresql_action"),
        ]

        return PipelineGraph(
            nodes=nodes,
            edges=edges,
            metadata={
                "kind": "xspatula_pipeline_graph",
                "version": "2.3.1",
                "setup_path": str(setup_path),
                "traceability": True,
            },
        ).to_dict()

    def _get_setup_path(self) -> Path:
        return Path(self.facade.setup_path).resolve()

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
        candidates = self._extract_file_references(
            pilot_data,
            allowed_suffixes={".txt", ".json"},
        )

        files = []

        for candidate in candidates:
            resolved = self._resolve_path(setup_path, candidate)

            if resolved:
                files.append(
                    self._file_metadata_with_trace(
                        path=resolved,
                        setup_path=setup_path,
                        role="process_file",
                        requested_value=candidate,
                    )
                )

        return self._unique_files(files)

    def _discover_job_bundle_files(
        self,
        setup_path: Path,
        process_files: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        files = []

        for process_file in process_files:
            process_path = Path(process_file["absolute_path"])
            text = self._read_text_safe(process_path)

            references = self._extract_file_references(
                text,
                allowed_suffixes={".json"},
            )

            for reference in references:
                resolved = self._resolve_path(setup_path, reference)

                if resolved:
                    files.append(
                        self._file_metadata_with_trace(
                            path=resolved,
                            setup_path=setup_path,
                            role="job_file",
                            requested_value=reference,
                            discovered_from=str(process_path),
                        )
                    )

        return self._unique_files(files)

    def _extract_file_references(
        self,
        data: Any,
        allowed_suffixes: set[str],
    ) -> List[str]:
        results = []

        def add_if_file_reference(value: str):
            clean = value.strip().strip('"').strip("'").strip(",")

            for suffix in allowed_suffixes:
                if clean.endswith(suffix):
                    results.append(clean)

        def walk(value: Any):
            if isinstance(value, dict):
                for child in value.values():
                    walk(child)

            elif isinstance(value, list):
                for child in value:
                    walk(child)

            elif isinstance(value, str):
                if "\n" in value:
                    for line in value.splitlines():
                        for part in line.replace("\\", "/").split():
                            add_if_file_reference(part)
                else:
                    add_if_file_reference(value)

        walk(data)

        return results

    def _file_metadata_with_trace(
        self,
        path: Optional[Path],
        setup_path: Path,
        role: str,
        requested_value: Optional[str] = None,
        parsed_keys: Optional[List[str]] = None,
        discovered_from: Optional[str] = None,
    ) -> Dict[str, Any]:
        exists = bool(path and path.exists())
        absolute_path = str(path.resolve()) if path else None

        return {
            "role": role,
            "name": path.name if path else None,
            "path": absolute_path,
            "absolute_path": absolute_path,
            "relative_path": self._relative_path(path, setup_path) if path else None,
            "exists": exists,
            "size": path.stat().st_size if exists else None,
            "suffix": path.suffix if path else None,
            "parsed_keys": parsed_keys or [],
            "trace": {
                "role": role,
                "requested_value": requested_value,
                "resolved_path": absolute_path,
                "relative_path": self._relative_path(path, setup_path) if path else None,
                "exists": exists,
                "discovered_from": discovered_from,
                "setup_path": str(setup_path),
            },
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

        for item in files:
            key = item.get("absolute_path") or item.get("path")

            if key not in seen:
                seen.add(key)
                unique.append(item)

        return unique