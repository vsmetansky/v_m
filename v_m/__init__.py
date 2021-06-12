import logging

from v_m.commands import cli


def init():
    logging.basicConfig()


def run():
    cli()


def main():
    init()
    run()
