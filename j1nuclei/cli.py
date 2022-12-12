import argparse
import os
import sys

import j1nuclei.config
import j1nuclei.runner


def _get_api_key(env: str) -> str:
    """
    :param env: Environment key to retrieve
    :return: value of the environment variable
    """
    return os.getenv(env)


def _set_global_config(config_namespace: argparse.Namespace) -> None:
    """
    Set global configuration state
    :param config_namespace: argparse namespace
    :return: None
    """
    j1nuclei.config.j1_account = config_namespace.account
    j1nuclei.config.nb_nuclei_concurrent = config_namespace.concurrent
    j1nuclei.config.nuclei_extra_parameters = config_namespace.nuclei_params
    j1nuclei.config.nuclei_report_path = config_namespace.nuclei_report_path
    j1nuclei.config.query_file = config_namespace.query_file
    j1nuclei.config.j1_api_key = _get_api_key(config_namespace.apikey_env)
    j1nuclei.config.persister_scope = config_namespace.scope

    if not j1nuclei.config.j1_api_key:
        print(
            f"Error retrieving API key from environment variable {config_namespace.apikey_env}. The api key must be set")
        raise RuntimeError(f"Unable to retrieve api key from {config_namespace.apikey_env}")


class CLI:
    def __init__(self, prog=None):
        """
        :param prog: Program name
        """
        self.prog = prog
        self.parser = self._build_parser()

    def _build_parser(self) -> argparse.ArgumentParser:
        """
        :return: argument parser.
        """
        parser = argparse.ArgumentParser(
            prog=self.prog,
            description="J1Nuclei demonstrates how you can use JupiterOne to automate running ProjectDiscovery's Nuclei security scanner.",
            epilog="For more information on JupiterOne's API visit https://community.askj1.com/kb/articles/794-jupiterone-api"
        )
        parser.add_argument(
            '-a',
            '--account',
            type=str,
            required=True,
            help=(
                'Required - Set the JupiterOne account to collect targets and store results.'
                'To find the ID in your JupiterOne account go to Settings > Integration > {integration name} > {configuration name} > value in the ID field.'
            ),
        )
        parser.add_argument(
            '-c',
            "--concurrent",
            type=int,
            default=5,
            help=(
                'Optional - Number of concurrent nuclei instance to run '
            ),
        )
        parser.add_argument(
            '-n',
            '--nuclei-params',
            type=str,
            default=None,
            help=('Optional - Additional nuclei parameters to pass.'
                  'The tool makes use of the -u, -json, -o parameters so they cannot be overwritten'),
        )
        parser.add_argument(
            '-q',
            '--query-file',
            type=str,
            default=f"{os.path.dirname(os.path.abspath(__file__))}/targets_discovery.json",
            help='Optional - The file containing targeting queries. By default targets_discovery.json is used',
        )
        parser.add_argument(
            '-apikey-env',
            type=str,
            default="J1_API_TOKEN",
            help=(
                'Optional - They environment variable used to retrieve the JupiterOne API Key. By default J1_API_TOKEN is used'
                'To create an API key follow instructions from:'
                'https://community.askj1.com/kb/articles/785-creating-user-and-account-api-keys#create-account-level-api-keys'
            ),
        )
        parser.add_argument(
            '-r',
            '--nuclei-report-path',
            type=str,
            default=os.path.join(os.getcwd(), "reports/"),
            help='Optional - Path where to store nuclei reports. Default is reports',
        )
        parser.add_argument(
            '-s',
            '--scope',
            type=str,
            default="j1nuclei",
            help='Optional - J1 Persister scope. Default is j1nuclei',
        )
        return parser

    def main(self, argv: str) -> None:
        """
        Entrypoint for the command line interface
        :param argv: The parameters supplied to the command line program.
        :return: None
        """
        _set_global_config(self.parser.parse_args(argv))

        # nuclei doesn't check if the output folder exist and will error out
        # making sure it does
        os.makedirs(j1nuclei.config.nuclei_report_path, exist_ok=True)

        j1nuclei.runner.run()

        print("Vulnerability scan completed!")


def main(argv=None) -> None:
    """
    Main entry point to cli
    :param argv: command line argument data
    :return: None
    """
    argv = argv if argv is not None else sys.argv[1:]
    sys.exit(CLI(prog='j1nuclei').main(argv))
