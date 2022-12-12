import json
import uuid
import logging
import os

from j1nuclei.j1api import graph_query, create_persister_job, ingest_data_and_finalize
from j1nuclei.nucleirunner import run_nuclei_concurrent
from typing import List, Dict

import j1nuclei.config

logger = logging.getLogger(__name__)


def run() -> None:
    """
    Run j1nuclei main flow
    :return: None
    """
    process_targets(j1nuclei.config.query_file,
                    j1nuclei.config.nuclei_report_path,
                    j1nuclei.config.nb_nuclei_concurrent)


def process_targets(query_file_path: str,
                    nuclei_report_folder: str,
                    nb_concurrent: int) -> None:
    """
    Process specific target
    :param query_file_path: query file path
    :param nuclei_report_folder: folder to store nuclei results
    :param nb_concurrent: number of maximum concurrent nuclei to run
    :return: None
    """

    with open(query_file_path, "r") as query_file:
        queries = json.load(query_file)

    targets = []
    target_keys = dict()
    expected_keys = {"key", "target", "scope", "source"}

    for q in queries["queries"]:
        logger.debug(f"Processing query {q['name']}")

        data = graph_query(q["query"])

        q_target_count = 0

        if data:
            # avoid duplicates
            for r in data["data"]:

                # validate that the query return the expected attributes
                if not expected_keys.issubset(r.keys()):
                    logger.debug(f"Expected attributes {expected_keys} not present in result for query - {q['query']}")
                    continue

                # check to see if the query returned a target. Some may return emtpy string in query
                if "target" in r.keys():
                    if r["target"] != "":
                        q_target_count = q_target_count + 1
                        if not r["key"] in target_keys:
                            r["nuclei_report_file"] = os.path.join(nuclei_report_folder, str(uuid.uuid4()) + ".json")

                            j1_target_context = r.copy()

                            # in some case, the target may be an array based on the node property
                            # we only scan the first host/ip
                            if isinstance(j1_target_context["target"], list):
                                j1_target_context["target"] = j1_target_context["target"][0]

                            targets.append(j1_target_context)

                            target_keys[r["key"]] = ""
        else:
            logger.error(f"Error retrieving results for query name {q['name']}")

        print(f"Query - {q['name']} found {q_target_count} targets")

    print(f"Found a total of {len(targets)} targets to scan")

    # saving data map
    logger.info(f"Saving target map file for {len(targets)} targets")
    save_runner_mapping(targets, "report_map.json")

    run_nuclei_concurrent(targets, nb_concurrent)

    logger.info("Scanning completed, ingesting results")
    process_runner_map("report_map.json")


def save_runner_mapping(runner_map: Dict, filepath) -> None:
    """
    Save scan target to report mapping
    :param runner_map: mapping between target and nuclei report file
    :param filepath: where to save mapping
    :return: None
    """
    logger.debug(f"Saving runner mapping to {filepath}")
    with open(filepath, "w") as outfile:
        json.dump(runner_map, outfile)


def process_runner_map(filepath: str) -> None:
    """
    Process target reports
    :param filepath: target to report map
    :return: None
    """
    with open(filepath, "r") as runner_map_file:
        runner_map = json.load(runner_map_file)

    job_id = create_persister_job()

    if job_id is None:
        raise Exception("Unable to create job id")

    job_keys = dict()  # used to keep track of all entity and relationships keys and avoid duplicate entry

    job_payload = dict()
    job_payload["entities"] = []
    job_payload["relationships"] = []

    for j1_target_context in runner_map:
        logger.debug(
            f"Ingesting data for target {j1_target_context['target']} from {j1_target_context['nuclei_report_file']}")

        target_payload = parse_target_report(j1_target_context, job_keys)
        if target_payload:
            job_payload["entities"].extend(target_payload["entities"])
            job_payload["relationships"].extend(target_payload["relationships"])

    print(f"Merging findings back to JupiterOne account {j1nuclei.config.j1_account}")

    ingest_data_and_finalize(job_id, job_payload)

    print("Merging complete!")
    logger.debug(f"Done processing runner map {filepath}")


def marshal_nuclei_to_j1payload(j1_target_context: Dict, nuclei_findings: Dict, job_keys: Dict) -> Dict:
    """
    Convert nuclei data to JupiterOne
    :param j1_target_context: target context
    :param nuclei_findings: findings
    :param job_keys: unique entity and relationships keys
    :return:
    """
    entities = []
    relationships = []

    # schema
    # Target - HAS -> Nuclei Finding -> Is Nuclei Vulnerability
    for nuclei_finding in nuclei_findings:
        # create properties
        # define entity for Weakness

        matcher_name = nuclei_finding.get("matcher-name") or ''

        # TO Improve - cheap work around
        # nuclei is not consistent on where it puts additional data
        # making creating unique keys per cases not trivial and case by case
        # for example, some are in matcher_name, some are in extracted_results
        # cheap workound for now is if we hit duplicate keys, we skip the entity
        finding_entity_key = "nuclei_{}_{}_{}".format(j1_target_context["key"],
                                                      nuclei_finding["template-id"],
                                                      matcher_name)

        # if we already have the entity in the batch upload we can skip
        # this happens when entity has multiple findings
        # we only add 1 instance, but we add all relationships/findings to it
        if finding_entity_key not in job_keys:
            job_keys[finding_entity_key] = ""

            finding_entity = dict()

            finding_entity["_key"] = finding_entity_key
            finding_entity["_type"] = "nuclei_finding"
            finding_entity["_class"] = "Finding"
            finding_entity["owner"] = "nuclei"
            finding_entity["matcher-name"] = matcher_name
            finding_entity["displayName"] = nuclei_finding["info"]["name"]
            finding_entity["severity"] = nuclei_finding["info"]["severity"]
            finding_entity["nuclei_type"] = nuclei_finding["type"]
            entities.append(finding_entity)

        # Vulnerability entity
        vul_entity_key = f"nuclei_id_{nuclei_finding['template-id']}"

        # if we already created it we skip
        if vul_entity_key not in job_keys:
            job_keys[vul_entity_key] = ""

            vul_entity = dict()
            vul_entity["_key"] = vul_entity_key
            vul_entity["_type"] = "nuclei_vulnerability"
            vul_entity["_class"] = "Vulnerability"
            vul_entity["owner"] = "nuclei"
            vul_entity["name"] = nuclei_finding["info"].get("name")
            vul_entity["displayName"] = vul_entity["name"]
            vul_entity["author"] = nuclei_finding["info"].get("author")
            vul_entity["description"] = nuclei_finding["info"].get("description")
            vul_entity["severity"] = nuclei_finding["info"]["severity"]
            vul_entity["nuclei_type"] = nuclei_finding["type"]
            vul_entity["template"] = nuclei_finding["template"]
            vul_entity["template-id"] = nuclei_finding["template-id"]
            vul_entity["template-url"] = nuclei_finding["template-url"]

            entities.append(vul_entity)

        # https://community.askj1.com/kb/articles/1157-creating-relationships-between-assets-you-own-and-assets-you-do-not
        # relationship

        # Target - HAS - Finding
        has_relationship_key = f"{j1_target_context['key']}_{finding_entity_key}"

        if has_relationship_key not in job_keys:
            job_keys[has_relationship_key] = ""

            has_relationship = dict()
            has_relationship["_key"] = has_relationship_key
            has_relationship["_type"] = "nuclei_has"
            has_relationship["_class"] = "HAS"
            has_relationship["displayName"] = has_relationship["_class"]

            # https://community.askj1.com/kb/articles/1157-creating-relationships-between-assets-you-own-and-assets-you-do-not
            # to create relationships to entities we didn't create, we need to provide its source and scope.
            has_relationship["_fromEntitySource"] = j1_target_context["source"]
            has_relationship["_fromEntityScope"] = j1_target_context["scope"]
            has_relationship["_fromEntityKey"] = j1_target_context["key"]
            has_relationship["_toEntitySource"] = "api"
            has_relationship["_toEntityScope"] = j1nuclei.config.persister_scope
            has_relationship["_toEntityKey"] = finding_entity_key
            relationships.append(has_relationship)

        # Finding - IS - Vulnerability
        is_relationship_key = f"{finding_entity_key}_{vul_entity_key}"

        if is_relationship_key not in job_keys:
            job_keys[is_relationship_key] = ""

            is_relationship = dict()
            is_relationship["_key"] = is_relationship_key
            is_relationship["_type"] = "nuclei_is"
            is_relationship["_class"] = "IS"
            is_relationship["displayName"] = is_relationship["_class"]

            is_relationship["_fromEntitySource"] = "api"
            is_relationship["_fromEntityScope"] = j1nuclei.config.persister_scope
            is_relationship["_fromEntityKey"] = finding_entity_key
            is_relationship["_toEntitySource"] = "api"
            is_relationship["_toEntityScope"] = j1nuclei.config.persister_scope
            is_relationship["_toEntityKey"] = vul_entity_key

            relationships.append(is_relationship)

    payload = dict()
    payload["entities"] = entities
    payload["relationships"] = relationships

    return payload


def parse_target_report(j1_target_context: Dict, job_keys: Dict) -> Dict:
    """
    Parse nuclei report
    :param j1_target_context: target context
    :param job_keys: job keys
    :return: jupiterone formatted report data
    """
    findings = []

    nuclei_report_filename = j1_target_context["nuclei_report_file"]

    logger.debug(f"Processing {nuclei_report_filename}")

    if os.path.exists(nuclei_report_filename):
        with open(nuclei_report_filename, "r") as nuclei_report:
            # nuclei json report writes json object per line but is not using correct structure of array causing
            # json.load(file) to fail. We must reach line by line and load json.loads()
            for line in nuclei_report.readlines():
                findings.append(json.loads(line))

        if len(findings) > 0:
            print(f"Target key {j1_target_context['key']} has {len(findings)} issues")
            return marshal_nuclei_to_j1payload(j1_target_context, findings, job_keys)
    else:
        return None
