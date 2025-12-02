#!/usr/bin/env python3
"""Main module of kube-schedule-scaler"""

import json
import logging
import os
from concurrent.futures import FIRST_EXCEPTION, ThreadPoolExecutor, wait
from dataclasses import dataclass
from datetime import datetime, timedelta
from queue import Queue, ShutDown
from signal import SIGINT, SIGTERM, signal, strsignal
from time import sleep
from types import FrameType

import dateutil.tz
import pykube
from croniter import croniter

logging.basicConfig(
    level=os.environ.get("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s - %(message)s",
    datefmt="%d-%m-%Y %H:%M:%S",
)


@dataclass(frozen=True)
class ScaleTarget:
    name: str
    namespace: str
    schedule_actions: list[dict[str, str]]


def handle_shutdown(signum: int, frame: FrameType | None) -> None:
    """Handle shutdown signals.

    This will wait for the shutdown period for the queue to be
    emptied before calling shutdown on the queue, terminating
    the processor thread.
    """
    sig_str = strsignal(signum)
    sig_str = sig_str.split(":")[0] if sig_str else "Unknown"
    logging.info(f"Received {sig_str} signal, gracefully exiting.")
    global shutdown
    shutdown = True
    elapsed = 0
    while elapsed < SHUTDOWN_TIMEOUT_SECONDS:
        if queue.qsize() == 0:
            break
        sleep(0.1)
        elapsed += 0.1
    queue.shutdown()


def sleep_thread(seconds: float):
    """Sleep for the given number of seconds.

    If shutdown flag is set, exit the sleep early.
    """
    global shutdown
    end_time = datetime.now().timestamp() + seconds
    while datetime.now().timestamp() <= end_time:
        if shutdown:
            break
        sleep(0.1)


def get_kube_api() -> pykube.HTTPClient:
    """Initiating the API from Service Account or when running locally from ~/.kube/config"""
    return pykube.HTTPClient(config=pykube.KubeConfig.from_env(), timeout=10)


def deployments_to_scale(queue) -> None:
    """Gets the deployments configured for schedule scaling and puts them on the queue"""
    logging.info("Fetching scaling tasks")
    scale_targets: list[ScaleTarget] = []

    for deployment in pykube.Deployment.objects(api).filter(namespace=pykube.all):
        namespace = deployment.namespace
        name = deployment.name

        annotations = deployment.metadata.get("annotations", {})
        schedule_actions = parse_schedules(
            annotations.get("zalando.org/schedule-actions", "[]"), (name, namespace)
        )

        if not schedule_actions:
            continue

        scale_targets.append(
            ScaleTarget(
                name=name, namespace=namespace, schedule_actions=schedule_actions
            )
        )

    if not scale_targets:
        logging.info("No deployment is configured for schedule scaling")
    else:
        logging.info("Found %s deployments to scale", len(scale_targets))

    for item in scale_targets:
        queue.put(item)


def parse_schedules(
    schedules: str, identifier: tuple[str, str]
) -> list[dict[str, str]]:
    """Parse the JSON schedule"""
    try:
        return json.loads(schedules)
    except (TypeError, json.JSONDecodeError) as err:
        logging.error("%s - Error in parsing JSON %s", identifier, schedules)
        logging.exception(err)
        return []


def get_delta_sec(schedule: str, timezone_name: str | None = None) -> int:
    """Returns the number of seconds passed since last occurence of the given cron expression"""
    # localize the time to the provided timezone, if specified
    if not timezone_name:
        tz = None
    else:
        tz = dateutil.tz.gettz(timezone_name)

    # get current time
    now = datetime.now(tz)
    # get the last previous occurrence of the cron expression
    time = croniter(schedule, now).get_prev()
    # convert now to unix timestamp
    now_ts = now.timestamp()
    # return the delta
    return int(now_ts - time)


def get_wait_sec() -> float:
    """Return the number of seconds to wait before the next minute"""
    now = datetime.now()
    future = datetime(now.year, now.month, now.day, now.hour, now.minute) + timedelta(
        minutes=1
    )
    return (future - now).total_seconds()


def process_deployment(scale_target: ScaleTarget) -> None:
    """Determine actions to run for the given deployment and list of schedules"""
    namespace = scale_target.namespace
    name = scale_target.name
    for schedule in scale_target.schedule_actions:
        # when provided, convert the values to int
        replicas = schedule.get("replicas", None)
        if replicas is not None:
            replicas = int(replicas)
        min_replicas = schedule.get("minReplicas", None)
        if min_replicas is not None:
            min_replicas = int(min_replicas)
        max_replicas = schedule.get("maxReplicas", None)
        if max_replicas is not None:
            max_replicas = int(max_replicas)

        schedule_expr = schedule.get("schedule", None)

        if not schedule_expr:
            return

        schedule_timezone = schedule.get("tz", None)
        logging.debug("%s/%s %s", namespace, name, schedule)

        # if less than 60 seconds have passed from the trigger
        if get_delta_sec(schedule_expr, schedule_timezone) < 60:
            if replicas is not None:
                scale_deployment(name, namespace, replicas)
            if min_replicas is not None or max_replicas is not None:
                scale_hpa(name, namespace, min_replicas, max_replicas)


def scale_deployment(name: str, namespace: str, replicas: int) -> None:
    """Scale the deployment to the given number of replicas"""
    try:
        deployment = (
            pykube.Deployment.objects(api).filter(namespace=namespace).get(name=name)
        )
    except pykube.exceptions.ObjectDoesNotExist:
        logging.warning("Deployment %s/%s does not exist", namespace, name)
        return

    if replicas == deployment.replicas:
        return

    try:
        deployment.patch({"spec": {"replicas": replicas}}, subresource="scale")
        logging.info(
            "Deployment %s/%s scaled to %s replicas", namespace, name, replicas
        )

    except pykube.exceptions.HTTPError as err:
        logging.error(
            "Exception raised while patching deployment %s/%s", namespace, name
        )
        logging.exception(err)


def scale_hpa(
    name: str, namespace: str, min_replicas: int | None, max_replicas: int | None
) -> None:
    """Adjust hpa min and max number of replicas"""

    try:
        hpa = (
            pykube.HorizontalPodAutoscaler.objects(api)
            .filter(namespace=namespace)
            .get(name=name)
        )
    except pykube.exceptions.ObjectDoesNotExist:
        logging.warning("HPA %s/%s does not exist", namespace, name)
        return

    patch = {}

    spec = hpa.obj["spec"]
    if min_replicas is not None and min_replicas != spec["minReplicas"]:
        patch["minReplicas"] = min_replicas

    if max_replicas is not None and max_replicas != spec["maxReplicas"]:
        patch["maxReplicas"] = max_replicas

    if not patch:
        return

    try:
        hpa.patch({"spec": patch})
        if min_replicas:
            logging.info(
                "HPA %s/%s minReplicas set to %s", namespace, name, min_replicas
            )
        if max_replicas:
            logging.info(
                "HPA %s/%s maxReplicas set to %s", namespace, name, max_replicas
            )
    except pykube.exceptions.HTTPError as err:
        logging.error("Exception raised while patching HPA %s/%s", namespace, name)
        logging.exception(err)


def collector(queue: Queue) -> None:
    """Collects scaling actions.

    Every minute on the minute, call the deployments_to_scale function
    which finds deployments to scale and places the scaling target on
    the queue.
    """
    logging.info("Starting collector thread")
    global shutdown
    while True:
        wait_sec = get_wait_sec()
        logging.debug("Waiting %ss until next collection time", wait_sec)
        sleep_thread(wait_sec)

        if shutdown:
            logging.info("Shutdown signal received, exiting")
            return
        logging.debug("collector:calling deployments_to_scale")
        deployments_to_scale(queue)


def processor(queue: Queue) -> None:
    """Processes scaling requests.

    If a ShutDown object is placed on the queue, the
    thread will terminate when that object is processed.
    """
    logging.info("Starting processor thread")
    while True:
        try:
            item = queue.get()
        except ShutDown:
            logging.info("Shutdown signal received, exiting")
            return
        process_deployment(item)
        if queue.qsize() > 10:
            logging.warning(f"Queue size: {queue.qsize()}")


shutdown = False
SHUTDOWN_TIMEOUT_SECONDS = 60
queue: Queue[ScaleTarget] = Queue()
api = get_kube_api()

signal(SIGTERM, handle_shutdown)
signal(SIGINT, handle_shutdown)

if __name__ == "__main__":
    logging.info("Starting main thread")
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(collector, queue), executor.submit(processor, queue)]

        done, _ = wait(futures, return_when=FIRST_EXCEPTION)

        for future in done:
            try:
                future.result()
            except Exception:
                logging.exception("Application encountered fatal error")
                handle_shutdown(3, None)

                break

    logging.info("Exiting main thread")
