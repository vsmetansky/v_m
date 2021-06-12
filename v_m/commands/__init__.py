import logging
from pathlib import Path
from importlib import import_module

import click

import v_m.settings as settings
from v_m.commands.utils import as_module_path, as_python_file, populate_template, is_magic_module

BASE_DIR = Path(__file__).parent
TEMPLATES_DIR = BASE_DIR.joinpath('templates')
FLOWS_DIR = BASE_DIR.parent.joinpath('flows')
ROOT_PACKAGE = 'v_m'

logger = logging.getLogger(__name__)


@click.group()
def cli():
    pass


@click.command()
@click.argument('pipeline_name')
def generate_pipeline(pipeline_name: str):
    for template in TEMPLATES_DIR.iterdir():
        result_text = populate_template(template.read_text(), pipeline_name)
        result = BASE_DIR.parent.joinpath(template.name, as_python_file(pipeline_name))
        result.touch()
        result.write_text(result_text)


@click.command()
@click.argument('flow_name')
def register_one(flow_name: str):
    flow_module = import_module(as_module_path(ROOT_PACKAGE, FLOWS_DIR.joinpath(flow_name)))
    flow_module.flow.register(project_name=settings.PROJECT_NAME)


@click.command()
def register_all():
    for flow in FLOWS_DIR.iterdir():
        if is_magic_module(flow):
            continue
        flow_module = import_module(as_module_path(ROOT_PACKAGE, flow))
        flow_module.flow.register(project_name=settings.PROJECT_NAME)


@click.command()
@click.argument('flow_name')
def run_one(flow_name: str):
    flow_module = import_module(as_module_path(ROOT_PACKAGE, FLOWS_DIR.joinpath(flow_name)))
    flow_module.flow.run(run_on_schedule=False)


cli.add_command(generate_pipeline)
cli.add_command(register_one)
cli.add_command(register_all)
cli.add_command(run_one)
