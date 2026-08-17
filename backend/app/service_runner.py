from __future__ import annotations

import argparse
import ctypes
import hashlib
import importlib
import os
import sys
import tempfile
from collections.abc import Sequence
from contextlib import contextmanager, nullcontext
from pathlib import Path

import uvicorn
from fastapi import FastAPI

from app.core.service_shutdown import install_shutdown_endpoint


class PidFileInUseError(RuntimeError):
    pass


def _load_app(import_path: str) -> FastAPI:
    module_name, separator, attribute_name = import_path.partition(":")
    if not separator or not module_name or not attribute_name:
        raise ValueError("application must use the module:attribute form")
    application = getattr(importlib.import_module(module_name), attribute_name)
    if not isinstance(application, FastAPI):
        raise TypeError(f"{import_path!r} is not a FastAPI application")
    return application


def create_server(import_path: str, *, host: str, port: int) -> uvicorn.Server:
    application = _load_app(import_path)
    server = uvicorn.Server(uvicorn.Config(application, host=host, port=port))
    install_shutdown_endpoint(
        application,
        request_shutdown=lambda: setattr(server, "should_exit", True),
    )
    return server


def _pid_file_identity(path: Path) -> tuple[int, int, int]:
    stat = path.stat()
    return stat.st_dev, stat.st_ino, stat.st_ctime_ns


@contextmanager
def _windows_pid_lock(path: Path):
    from ctypes import wintypes

    mutex_key = hashlib.sha256(str(path.resolve()).casefold().encode("utf-8")).hexdigest()
    mutex_name = f"Global\\GaoshouPlatform-ServiceRunner-{mutex_key}"
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    create_mutex = kernel32.CreateMutexW
    create_mutex.argtypes = (ctypes.c_void_p, wintypes.BOOL, wintypes.LPCWSTR)
    create_mutex.restype = wintypes.HANDLE
    wait_for_single_object = kernel32.WaitForSingleObject
    wait_for_single_object.argtypes = (wintypes.HANDLE, wintypes.DWORD)
    wait_for_single_object.restype = wintypes.DWORD
    release_mutex = kernel32.ReleaseMutex
    release_mutex.argtypes = (wintypes.HANDLE,)
    release_mutex.restype = wintypes.BOOL
    close_handle = kernel32.CloseHandle
    close_handle.argtypes = (wintypes.HANDLE,)
    close_handle.restype = wintypes.BOOL

    handle = create_mutex(None, False, mutex_name)
    if not handle:
        raise ctypes.WinError(ctypes.get_last_error())
    acquired = False
    try:
        wait_result = wait_for_single_object(handle, 0)
        if wait_result in (0x00000000, 0x00000080):
            acquired = True
        elif wait_result == 0x00000102:
            raise PidFileInUseError(f"PID file is already owned: {path}")
        else:
            raise ctypes.WinError(ctypes.get_last_error())
        yield
    finally:
        if acquired:
            release_mutex(handle)
        close_handle(handle)


@contextmanager
def _posix_pid_lock(path: Path):
    import fcntl

    lock_path = path.with_name(f".{path.name}.lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+b") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise PidFileInUseError(f"PID file is already owned: {path}") from exc
        try:
            yield
        finally:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


@contextmanager
def _exclusive_pid_lock(path: Path):
    lock = _windows_pid_lock(path) if os.name == "nt" else _posix_pid_lock(path)
    with lock:
        yield


@contextmanager
def _owned_pid_file(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    process_id = str(os.getpid())
    descriptor, temporary_name = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
        text=True,
    )
    temporary_path = Path(temporary_name)
    owned_identity: tuple[int, int, int] | None = None
    try:
        with os.fdopen(descriptor, "w", encoding="ascii", newline="") as handle:
            handle.write(process_id)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary_path, path)
        owned_identity = _pid_file_identity(path)
        yield
    finally:
        temporary_path.unlink(missing_ok=True)
        try:
            current_identity = _pid_file_identity(path)
            current_process_id = path.read_text(encoding="ascii")
            confirmed_identity = _pid_file_identity(path)
        except FileNotFoundError:
            current_identity = None
            current_process_id = None
            confirmed_identity = None
        if (
            owned_identity is not None
            and current_identity == owned_identity == confirmed_identity
            and current_process_id == process_id
        ):
            path.unlink(missing_ok=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run a GaoshouPlatform FastAPI service")
    parser.add_argument("application", help="ASGI application in module:attribute form")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--pid-file", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    pid_lock = _exclusive_pid_lock(args.pid_file) if args.pid_file else nullcontext()
    try:
        with pid_lock:
            pid_file_context = (
                _owned_pid_file(args.pid_file) if args.pid_file else nullcontext()
            )
            with pid_file_context:
                server = create_server(args.application, host=args.host, port=args.port)
                server.run()
    except PidFileInUseError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0 if server.started else 1


if __name__ == "__main__":
    raise SystemExit(main())
