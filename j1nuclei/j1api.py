import concurrent
import logging
import requests
import time
from j1nuclei.j1auth import get_auth_headers
from typing import Dict, List


logger = logging.getLogger(__name__)


def ingest_data_and_finalize(job_id: str, payload: Dict) -> None:
    call_persister_upload(job_id, payload)
    wait_for_job(job_id, "AWAITING_UPLOADS", 1)

    logger.debug(f"Completed upload, finalizing job {job_id}")
    call_persister_job_finalize(job_id)
    wait_for_job(job_id, "FINISHED", 1)


def start_ingestion_job(payload: Dict) -> None:
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

    # wait for persister job to reach expected state
    while True:
        r = get_job_status(job_id)
        if r == status_to_wait_for:
            break

        time.sleep(wait_time_sec)


def get_job_status(job_id: str) -> str:
    api_url = f"https://api.us.jupiterone.io/persister/synchronization/jobs/{job_id}"
    response = requests.get(api_url, headers=get_auth_headers())

    logger.debug(f"get job status response for {job_id} - {response.json()}")

    return response.json()["job"]["status"]


def call_persister_upload(job_id: str, payload: Dict) -> None:
    api_url = f"https://api.us.jupiterone.io/persister/synchronization/jobs/{job_id}/upload"
    response = requests.post(api_url, headers=get_auth_headers(), json=payload)

    logger.debug(f"call_persister_upload for {job_id} - {response.json()}")

    if response.status_code != 200:
        raise Exception(f"Error uploading payload to J1 got code error {response.status_code}")


def call_persister_job_finalize(job_id: str) -> None:
    api_url = f"https://api.us.jupiterone.io/persister/synchronization/jobs/{job_id}/finalize"
    response = requests.post(api_url, headers=get_auth_headers())

    logger.debug(f"call_job_finalize for {job_id} - {response.json()}")


def create_persister_job() -> str:
    api_url = "https://api.us.jupiterone.io/persister/synchronization/jobs"
    payload = {"source": "api", "scope": "j1nuclei", "syncMode": "DIFF"}
    response = requests.post(api_url, headers=get_auth_headers(), json=payload)

    if response.status_code == 200:
        return response.json()["job"]["id"]
    else:
        raise Exception(f"Error creating J1 job got code error {response.status_code}")


def graph_query(query: str, query_variables: ()) -> ():
    api_url = "https://api.us.jupiterone.io/graphql"

    inner_query = """
        query J1QL(
          $query: String!
          $variables: JSON
        ) {
          queryV1(
            query: $query
            variables: $variables){ data }
        }
    """
    variables = {
                "query": query,
                "variables": query_variables
    }

    payload = {"query": inner_query,
               "variables": variables}

    response = requests.post(api_url, json=payload, headers=get_auth_headers())

    if response.status_code != 200:
        logging.error(f"ERROR - [graph_query] - Got status code {response.status_code} with {response.json()}")
        return None
    else:
        return response.json()

