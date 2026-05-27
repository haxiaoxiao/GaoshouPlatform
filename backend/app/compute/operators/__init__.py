"""算子注册表 — L0-L3 四级算子体系"""
from app.compute.operators.registry import OperatorRegistry


def auto_discover(force_reload: bool = False):
    """自动发现并注册所有算子模块"""
    import importlib
    import pkgutil
    import sys

    from app.compute import operators as pkg

    for _importer, modname, _ispkg in pkgutil.iter_modules(pkg.__path__):
        if modname in ("base", "registry", "__init__", "indicator_ops"):
            continue
        module_name = f"app.compute.operators.{modname}"
        if force_reload and module_name in sys.modules:
            importlib.reload(sys.modules[module_name])
        else:
            importlib.import_module(module_name)


__all__ = ["OperatorRegistry", "auto_discover"]
