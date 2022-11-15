from lesscli import add_subcommand, run
from commondao.codegen import run_codegen


@add_subcommand('codegen', run_codegen)
def entry():
    """
    mysql service and toolkit for lessweb
    """
    pass


def main():
    run(entry)
