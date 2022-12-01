import concurrent.futures
import logging
import subprocess

import j1nuclei.config

logger = logging.getLogger(__name__)


def run_nuclei_concurrent(targets, max_workers):
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        for result in executor.map(run_nuclei, targets):
            logging.info("Execution completed with result {}".format(result))


def run_nuclei(j1_target_context):
    nuclei_cmd = "nuclei -u {} -json -o {} {}".format(j1_target_context["target"],
                                                      j1_target_context["nuclei_report_file"],
                                                      j1nuclei.config.nuclei_extra_parameters).split()

    logging.debug("Running nuclei with arg - {}".format(nuclei_cmd))

    ret_code = subprocess.run(nuclei_cmd, shell=False)

    logger.debug("{} -> ret code for {}".format(ret_code, j1_target_context["target"]))
    return ret_code
