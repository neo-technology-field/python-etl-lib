import importlib
import inspect
import pkgutil

import etl_lib.task


def test_all_task_modules_import_with_resolvable_annotations():
    """Guards against modules that import but cannot be introspected.

    Annotations are evaluated eagerly before Python 3.14, so a missing import in an annotation
    makes the module un-importable there and only degrades introspection on 3.14. Touching the
    annotations of every class defined in the package covers both cases.
    """
    modules = pkgutil.walk_packages(etl_lib.task.__path__, prefix="etl_lib.task.")
    for module_info in modules:
        module = importlib.import_module(module_info.name)
        for value in vars(module).values():
            if inspect.isclass(value) and value.__module__ == module.__name__:
                inspect.get_annotations(value.__init__)
