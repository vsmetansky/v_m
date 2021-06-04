import click

import v_m.settings as settings

TEMPLATES_DIR_PATH = 'v_m/commands/templates'
FLOW_MODULE_PATH = 'v_m.flows.{}'


@click.group()
def cli():
    pass


@click.command()
@click.argument('pipeline_name')
def generate_pipeline(pipeline_name: str):
    from pathlib import Path

    for template_path in Path(TEMPLATES_DIR_PATH).iterdir():
        result_text = template_path.read_text().replace('{}', pipeline_name)
        x = Path(f'v_m/{template_path.name}/{pipeline_name}.py')
        x.touch()
        x.write_text(result_text)


@click.command()
@click.argument('flow_name')
def register_one(flow_name: str):
    from importlib import import_module

    flow_module = import_module(FLOW_MODULE_PATH.format(flow_name))

    flow_module.flow.register(project_name=settings.PROJECT_NAME)


@click.command()
def register_all():
    pass


@click.command()
@click.argument('flow_name')
def run_one(flow_name: str):
    from importlib import import_module

    flow_module = import_module(FLOW_MODULE_PATH.format(flow_name))

    flow_module.flow.run()


cli.add_command(generate_pipeline)
cli.add_command(register_one)
cli.add_command(register_all)
cli.add_command(run_one)
