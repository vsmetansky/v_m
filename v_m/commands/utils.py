import re
from pathlib import Path


def populate_template(template: str, variable: str) -> str:
    return template.replace('{}', variable)


def as_python_file(file: str) -> str:
    return file + '.py'


def as_python_module(path: Path) -> str:
    return str(path).replace('/', '.').replace('.py', '')


def as_module_path(root_package: str, path: Path) -> str:
    return re.sub(fr'([.\w]*){root_package}', root_package, as_python_module(path))


def is_magic_module(module_path: Path) -> bool:
    return bool(re.findall(r'^__\w+__(.py)?$', module_path.name))
