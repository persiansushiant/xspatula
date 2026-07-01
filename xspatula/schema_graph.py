from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple


@dataclass
class SchemaNode:
    id: str
    label: str
    type: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SchemaEdge:
    source: str
    target: str
    label: str
    type: str
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class SchemaGraph:
    nodes: List[SchemaNode]
    edges: List[SchemaEdge]
    metadata: Dict[str, Any] = field(default_factory=dict)
    warnings: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "nodes": [asdict(node) for node in self.nodes],
            "edges": [asdict(edge) for edge in self.edges],
            "metadata": self.metadata,
            "warnings": self.warnings,
        }


class XspatulaSchemaGraph:
    """
    Builds a database schema graph from Xspatula JSON setup files.

    This does not execute the database.
    It only inspects setup JSON files and returns a graph contract for:

    - schema exploration
    - table dependency visualization
    - foreign-key / reference discovery
    - future database hardening
    - documentation
    """

    def __init__(self, facade: Any):
        self.facade = facade

    def build(self) -> Dict[str, Any]:
        setup_path = Path(self.facade.setup_path).resolve()

        json_files = self._discover_json_files(setup_path)

        table_nodes: Dict[str, SchemaNode] = {}
        schema_nodes: Dict[str, SchemaNode] = {}
        column_nodes: Dict[str, SchemaNode] = {}
        edges: List[SchemaEdge] = []
        warnings: List[Dict[str, Any]] = []

        table_definitions = []

        for json_file in json_files:
            data = self._read_json_safe(json_file)
            process_items = self._normalise_process_items(data)

            for process_index, process in enumerate(process_items):
                process_id = process.get("process_id")
                parameters = process.get("parameters", {})

                if not isinstance(parameters, dict):
                    continue

                schema_name = parameters.get("schema")
                table_name = parameters.get("table")
                command = parameters.get("command")

                if not schema_name or not table_name:
                    continue

                table_id = self._table_id(schema_name, table_name)
                schema_id = self._schema_id(schema_name)

                schema_nodes.setdefault(
                    schema_id,
                    SchemaNode(
                        id=schema_id,
                        label=schema_name,
                        type="schema",
                        metadata={
                            "schema": schema_name,
                        },
                    ),
                )

                table_nodes.setdefault(
                    table_id,
                    SchemaNode(
                        id=table_id,
                        label=f"{schema_name}.{table_name}",
                        type="table",
                        metadata={
                            "schema": schema_name,
                            "table": table_name,
                            "source_files": [],
                            "process_ids": [],
                            "columns": [],
                            "insert_record_count": 0,
                        },
                    ),
                )

                table_nodes[table_id].metadata["source_files"].append(str(json_file))
                table_nodes[table_id].metadata["process_ids"].append(process_id)

                edges.append(
                    SchemaEdge(
                        source=schema_id,
                        target=table_id,
                        label="contains",
                        type="schema_contains_table",
                        metadata={
                            "source_file": str(json_file),
                        },
                    )
                )

                columns = self._extract_columns(command)

                for column in columns:
                    column_id = self._column_id(schema_name, table_name, column)

                    column_nodes.setdefault(
                        column_id,
                        SchemaNode(
                            id=column_id,
                            label=column,
                            type="column",
                            metadata={
                                "schema": schema_name,
                                "table": table_name,
                                "column": column,
                            },
                        ),
                    )

                    if column not in table_nodes[table_id].metadata["columns"]:
                        table_nodes[table_id].metadata["columns"].append(column)

                    edges.append(
                        SchemaEdge(
                            source=table_id,
                            target=column_id,
                            label="has column",
                            type="table_has_column",
                            metadata={
                                "source_file": str(json_file),
                            },
                        )
                    )

                insert_count = self._extract_insert_record_count(command)
                table_nodes[table_id].metadata["insert_record_count"] += insert_count

                table_definitions.append(
                    {
                        "schema": schema_name,
                        "table": table_name,
                        "table_id": table_id,
                        "columns": columns,
                        "source_file": str(json_file),
                        "process_id": process_id,
                        "process_index": process_index,
                    }
                )

        dependency_edges, dependency_warnings = self._infer_table_dependencies(table_definitions)

        edges.extend(dependency_edges)
        warnings.extend(dependency_warnings)

        nodes = (
            list(schema_nodes.values())
            + list(table_nodes.values())
            + list(column_nodes.values())
        )

        return SchemaGraph(
            nodes=nodes,
            edges=self._unique_edges(edges),
            warnings=warnings,
            metadata={
                "kind": "xspatula_schema_graph",
                "contract_version": "1.0.0",
                "setup_path": str(setup_path),
                "json_file_count": len(json_files),
                "schema_count": len(schema_nodes),
                "table_count": len(table_nodes),
                "column_count": len(column_nodes),
                "dependency_count": len(dependency_edges),
                "capabilities": {
                    "schema_visualization": True,
                    "table_dependency_graph": True,
                    "column_explorer": True,
                    "foreign_key_inference": True,
                    "risk_warnings": True,
                    "future_live_database_introspection": True,
                },
            },
        ).to_dict()

    def build_for_schema(self, schema_name: str) -> Dict[str, Any]:
        graph = self.build()

        allowed_node_ids = set()

        for node in graph["nodes"]:
            metadata = node.get("metadata", {})

            if metadata.get("schema") == schema_name or node["id"] == self._schema_id(schema_name):
                allowed_node_ids.add(node["id"])

        filtered_nodes = [
            node for node in graph["nodes"]
            if node["id"] in allowed_node_ids
        ]

        filtered_edges = [
            edge for edge in graph["edges"]
            if edge["source"] in allowed_node_ids and edge["target"] in allowed_node_ids
        ]

        graph["nodes"] = filtered_nodes
        graph["edges"] = filtered_edges
        graph["metadata"]["filtered_schema"] = schema_name

        return graph

    def _discover_json_files(self, setup_path: Path) -> List[Path]:
        if not setup_path.exists():
            return []

        return sorted(setup_path.rglob("*.json"))

    def _read_json_safe(self, path: Path) -> Dict[str, Any]:
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def _normalise_process_items(self, data: Dict[str, Any]) -> List[Dict[str, Any]]:
        raw = data.get("process", [])

        if isinstance(raw, dict):
            return [raw]

        if isinstance(raw, list):
            return [item for item in raw if isinstance(item, dict)]

        return []

    def _extract_columns(self, command: Any) -> List[str]:
        if isinstance(command, dict):
            columns = command.get("columns", [])

            if isinstance(columns, list):
                return [str(column) for column in columns]

        if isinstance(command, list):
            return self._extract_columns_from_sql_like_list(command)

        return []

    def _extract_columns_from_sql_like_list(self, command: List[Any]) -> List[str]:
        text = "\n".join(str(item) for item in command)

        matches = re.findall(
            r"^\s*([a-zA-Z_][a-zA-Z0-9_]*)\s+",
            text,
            flags=re.MULTILINE,
        )

        ignored = {
            "create",
            "primary",
            "foreign",
            "constraint",
            "unique",
            "check",
        }

        return [
            match
            for match in matches
            if match.lower() not in ignored
        ]

    def _extract_insert_record_count(self, command: Any) -> int:
        if isinstance(command, dict):
            values = command.get("values", [])

            if isinstance(values, list):
                return len(values)

        return 0

    def _infer_table_dependencies(
        self,
        table_definitions: List[Dict[str, Any]],
    ) -> Tuple[List[SchemaEdge], List[Dict[str, Any]]]:
        known_tables = {
            definition["table"]: definition
            for definition in table_definitions
        }

        known_full_tables = {
            f"{definition['schema']}.{definition['table']}": definition
            for definition in table_definitions
        }

        edges = []
        warnings = []

        for definition in table_definitions:
            schema = definition["schema"]
            table = definition["table"]
            source_table_id = definition["table_id"]

            for column in definition["columns"]:
                if not column.endswith("_id"):
                    continue

                referenced_table_name = column[:-3]

                target_definition = self._find_reference_target(
                    current_schema=schema,
                    referenced_table_name=referenced_table_name,
                    known_tables=known_tables,
                    known_full_tables=known_full_tables,
                )

                if target_definition:
                    target_table_id = target_definition["table_id"]

                    if target_table_id != source_table_id:
                        edges.append(
                            SchemaEdge(
                                source=source_table_id,
                                target=target_table_id,
                                label=f"{column} → {target_definition['table']}",
                                type="inferred_foreign_key",
                                metadata={
                                    "column": column,
                                    "source_schema": schema,
                                    "source_table": table,
                                    "target_schema": target_definition["schema"],
                                    "target_table": target_definition["table"],
                                    "confidence": "inferred_from_column_name",
                                    "source_file": definition["source_file"],
                                },
                            )
                        )
                else:
                    warnings.append(
                        {
                            "type": "unresolved_reference",
                            "schema": schema,
                            "table": table,
                            "column": column,
                            "expected_table": referenced_table_name,
                            "source_file": definition["source_file"],
                            "message": f"Column {schema}.{table}.{column} looks like a reference, but no table named {referenced_table_name} was found.",
                        }
                    )

        return edges, warnings

    def _find_reference_target(
        self,
        current_schema: str,
        referenced_table_name: str,
        known_tables: Dict[str, Dict[str, Any]],
        known_full_tables: Dict[str, Dict[str, Any]],
    ) -> Optional[Dict[str, Any]]:
        same_schema_key = f"{current_schema}.{referenced_table_name}"

        if same_schema_key in known_full_tables:
            return known_full_tables[same_schema_key]

        if referenced_table_name in known_tables:
            return known_tables[referenced_table_name]

        return None

    def _schema_id(self, schema: str) -> str:
        return f"schema:{schema}"

    def _table_id(self, schema: str, table: str) -> str:
        return f"table:{schema}.{table}"

    def _column_id(self, schema: str, table: str, column: str) -> str:
        return f"column:{schema}.{table}.{column}"

    def _unique_edges(self, edges: List[SchemaEdge]) -> List[SchemaEdge]:
        seen: Set[Tuple[str, str, str, str]] = set()
        unique = []

        for edge in edges:
            key = (edge.source, edge.target, edge.type, edge.label)

            if key in seen:
                continue

            seen.add(key)
            unique.append(edge)

        return unique