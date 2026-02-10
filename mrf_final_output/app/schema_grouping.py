"""Group files by schema.json content and move into schema-based subdirectories."""
from __future__ import annotations

import hashlib
import json
import logging
import shutil
from pathlib import Path
from typing import Iterable, List

LOG = logging.getLogger("app.schema_grouping")


def _schema_hash(schema_path: Path) -> str:
    data = json.loads(schema_path.read_text(encoding="utf-8"))
    normalized = json.dumps(data, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()


def _unique_destination(dest: Path) -> Path:
    if not dest.exists():
        return dest
    stem = dest.stem
    suffix = "".join(dest.suffixes)
    counter = 1
    while True:
        candidate = dest.with_name(f"{stem}_{counter}{suffix}")
        if not candidate.exists():
            return candidate
        counter += 1


def group_schema_directories(base_dir: Path) -> List[Path]:
    """
    Group directories by schema.json content under base_dir.

    For each schema file found under base_dir, a group directory is created:
      base_dir / group_<hash>
    All files and subfolders from the schema file's directory are moved into
    the group directory (including split subfolders).
    """
    if not base_dir.exists() or not base_dir.is_dir():
        return []

    schema_files = list(base_dir.rglob("*schema.json"))
    if not schema_files:
        return []

    group_dirs: List[Path] = []
    for schema_path in schema_files:
        try:
            schema_dir = schema_path.parent
            schema_hash = _schema_hash(schema_path)
            group_dir = base_dir / f"group_{schema_hash[:12]}"
            group_dir.mkdir(parents=True, exist_ok=True)

            # Skip if already grouped
            if schema_dir == group_dir:
                group_dirs.append(group_dir)
                continue

            # Move all items (including subfolders) into group directory
            for item in list(schema_dir.iterdir()):
                if item == group_dir:
                    continue
                dest = _unique_destination(group_dir / item.name)
                shutil.move(str(item), str(dest))

            # Remove empty schema_dir if possible
            if schema_dir != base_dir:
                try:
                    schema_dir.rmdir()
                except OSError:
                    pass

            # Ensure only one schema file named main_schema.json in group_dir
            schema_candidates = sorted(group_dir.glob("*schema.json"))
            if schema_candidates:
                main_schema = group_dir / "main_schema.json"
                # Keep the first schema file, rename to main_schema.json
                keep = schema_candidates[0]
                if keep != main_schema:
                    try:
                        if main_schema.exists():
                            main_schema.unlink()
                        keep.rename(main_schema)
                    except Exception:  # noqa: BLE001
                        pass
                # Remove all other schema files
                for extra in schema_candidates[1:]:
                    try:
                        if extra.exists() and extra != main_schema:
                            extra.unlink()
                    except Exception:  # noqa: BLE001
                        pass

            group_dirs.append(group_dir)
        except Exception as exc:  # noqa: BLE001
            LOG.warning("Failed to group schema at %s: %s", schema_path, exc)
            continue

    return sorted(set(group_dirs))


def group_schema_directories_in_paths(paths: Iterable[Path]) -> List[Path]:
    """Run schema grouping for multiple base paths."""
    grouped: List[Path] = []
    for path in paths:
        grouped.extend(group_schema_directories(path))
    return sorted(set(grouped))


def move_schema_files_to_directory(
    structure_roots: Iterable[Path],
    target_dir: Path,
    target_names: Iterable[str],
) -> List[Path]:
    """
    Copy schema files from structure roots into target_dir by matching filenames.
    Returns list of copied schema file paths in target_dir.
    """
    target_dir.mkdir(parents=True, exist_ok=True)
    names = {n for n in target_names if n}
    if not names:
        return []
    copied: List[Path] = []
    for root in structure_roots:
        if not root or not root.exists() or not root.is_dir():
            continue
        for name in names:
            for match in root.rglob(name):
                if not match.is_file():
                    continue
                dest = target_dir / match.name
                if dest.exists():
                    continue
                try:
                    shutil.copy2(str(match), str(dest))
                    copied.append(dest)
                except Exception:  # noqa: BLE001
                    continue
    return copied
