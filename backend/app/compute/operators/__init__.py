"""算子注册表 — L0-L3 四级算子体系"""
from app.compute.operators.registry import OperatorRegistry


def auto_discover():
    """自动发现并注册所有算子模块"""
    import importlib
    import pkgutil

    from app.compute import operators as pkg

    for _importer, modname, _ispkg in pkgutil.iter_modules(pkg.__path__):
        if modname in ("base", "registry", "__init__"):
            continue
        importlib.import_module(f"app.compute.operators.{modname}")


__all__ = ["OperatorRegistry", "auto_discover"]
