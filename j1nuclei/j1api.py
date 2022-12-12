import concurrent
import logging
import requests
import time
from jupiterone import JupiterOneClient

import j1nuclei.config
from j1nuclei.j1auth import get_auth_headers
from typing import Dict


logger = logging.getLogger(__name__)


def ingest_data_and_finalize(job_id: str, payload: Dict) -> None:
    """
    Ingest data and finalize persister job
    :param job_id: persister job id
    :param payload: data to ingest
    :return: None
    """

    print(f"Uploading findings to JupiterOne - jobid {job_id}")
    call_persister_upload(job_id, payload)
    wait_for_job(job_id, "AWAITING_UPLOADS", 1)

    logger.debug(f"Completed upload, finalizing job {job_id}")
    call_persister_job_finalize(job_id)

    print(f"Uploading completed merge in progress - jobid {job_id}")
    print(f"Waiting for merge to complete - jobid {job_id}")
    wait_for_job(job_id, "FINISHED", 1)


def start_ingestion_job(payload: Dict) -> None:
    """
    Start persister ingestion job
    :param payload: data to ingest
    :return: None
    """
    job_id = create_persister_job()

    if job_id is None:
        raise Exception("Unable to create job id")

    logger.debug(f"Created persister job id {job_id}")
    logger.debug(f"Pushing payload {payload}")

    call_persister_upload(job_id, payload)
    wait_for_job(job_id, "AWAITING_UPLOADS", 1)

    logger.debug(f"Completed upload, finalizing job {job_id}")
    call_persister_job_finalize(job_id)
    wait_for_job(job_id, "FINISHED", 1)


def wait_for_job(job_id: str, status_to_wait_for: str, wait_time_sec: int) -> None:
    """
    Wait for a specific persister job state
    :param job_id: persister job id
    :param status_to_wait_for: state to wait for
    :param wait_time_sec: time to waite before pulling state update
    :return: None
    """
    # wait for persister job to reach expected state
    while True:
        r = get_job_status(job_id)
        if r == status_to_wait_for:
            break

        time.sleep(wait_time_sec)


def get_job_status(job_id: str) -> str:
    """
    Get status for a persister job
    :param job_id: persister job id
    :rtype: str
    :return: state
    """
    api_url = f"https://api.us.jupiterone.io/persister/synchronization/jobs/{job_id}"
    response = requests.get(api_url, headers=get_auth_headers())
    response.raise_for_status()
    logger.debug(f"get job status response for {job_id} - {response.json()}")

    return response.json()["job"]["status"]


def call_persister_upload(job_id: str, payload: Dict) -> None:
    """
    Upload data to persister
    :param job_id: persister job id
    :param payload: data to upload
    :return: None
    """
    api_url = f"https://api.us.jupiterone.io/persister/synchronization/jobs/{job_id}/upload"
    response = requests.post(api_url, headers=get_auth_headers(), json=payload)
    response.raise_for_status()
    logger.debug(f"call_persister_upload for {job_id} - {response.json()}")


def call_persister_job_finalize(job_id: str) -> None:
    """
    Finalize persister job
    :param job_id: persister job id
    :return: None
    """
    api_url = f"https://api.us.jupiterone.io/persister/synchronization/jobs/{job_id}/finalize"
    response = requests.post(api_url, headers=get_auth_headers())
    response.raise_for_status()
    logger.debug(f"call_job_finalize for {job_id} - {response.json()}")


def create_persister_job() -> str:
    """
    Create persister job
    :rtype: str
    :return: id of newly created job
    """
    api_url = "https://api.us.jupiterone.io/persister/synchronization/jobs"
    payload = {"source": "api", "scope": "j1nuclei", "syncMode": "DIFF"}
    response = requests.post(api_url, headers=get_auth_headers(), json=payload)
    response.raise_for_status()

    return response.json()["job"]["id"]


def graph_query(query: str) -> Dict:
    """
    Query JupiterOne
    :param query: query
    :return: results
    """
    j1_client = JupiterOneClient(j1nuclei.config.j1_account, j1nuclei.config.j1_api_key)
    return j1_client.query_v1(query)

