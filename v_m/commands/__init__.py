from pathlib import Path

import click

TEMPLATES_DIR_PATH = 'v_m/commands/templates'


@click.group()
def cli():
    pass


@click.command()
@click.argument('name')
def generate_pipeline(name: str):
    for template_path in Path(TEMPLATES_DIR_PATH).iterdir():
        result_text = template_path.read_text().replace('{}', name)
        x = Path(f'v_m/{template_path.name}/{name}.py')
        x.touch()
        x.write_text(result_text)


cli.add_command(generate_pipeline)
