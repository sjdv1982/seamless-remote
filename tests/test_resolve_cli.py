from __future__ import annotations

import os
import subprocess
import sys


def _write_fake_modules(root):
    seamless_pkg = root / "seamless"
    seamless_config_pkg = root / "seamless_config"
    seamless_remote_pkg = root / "seamless_remote"
    seamless_pkg.mkdir()
    seamless_config_pkg.mkdir()
    seamless_remote_pkg.mkdir()

    (seamless_pkg / "__init__.py").write_text(
        """
class Buffer:
    content = b"payload"

    def get_value(self, celltype):
        return self.content.decode()


class Checksum:
    def __init__(self, value):
        if len(value) != 64 or any(c not in "0123456789abcdefABCDEF" for c in value):
            raise ValueError(value)
        self.value = value.lower()

    def hex(self):
        return self.value

    def resolve(self):
        return Buffer()


def close():
    pass
"""
    )
    (seamless_pkg / "config.py").write_text(
        "def set_stage(*args, **kwargs):\n    pass\n"
    )
    (seamless_config_pkg / "__init__.py").write_text(
        """
def change_stage():
    pass


def set_workdir(path):
    pass
"""
    )
    (seamless_config_pkg / "select.py").write_text(
        """
def select_project(project):
    pass


def select_subproject(subproject):
    pass


def select_execution(execution):
    pass


def get_selected_cluster():
    return "cluster"
"""
    )
    (seamless_config_pkg / "extern_clients.py").write_text(
        "def set_remote_clients_from_env(include_dask=False):\n    return True\n"
    )
    (seamless_config_pkg / "config_files.py").write_text(
        "def load_config_files():\n    pass\n"
    )
    (seamless_remote_pkg / "__init__.py").write_text("")
    (seamless_remote_pkg / "database_remote.py").write_text("DISABLED = False\n")


def test_resolve_prefers_literal_checksum_over_same_named_file(tmp_path):
    fake_modules = tmp_path / "fake_modules"
    fake_modules.mkdir()
    _write_fake_modules(fake_modules)

    workdir = tmp_path / "work"
    workdir.mkdir()
    checksum = "1" * 64
    (workdir / checksum).write_text("not a checksum")
    output = workdir / "out.bin"
    env = os.environ.copy()
    env["PYTHONPATH"] = str(fake_modules)

    script = os.path.join(
        os.path.dirname(__file__), "..", "bin", "seamless-resolve"
    )
    proc = subprocess.run(
        [sys.executable, script, checksum, "--output", str(output)],
        cwd=workdir,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )

    assert proc.returncode == 0, proc.stderr
    assert output.read_bytes() == b"payload"
