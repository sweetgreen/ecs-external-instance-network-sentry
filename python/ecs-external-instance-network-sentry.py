import argparse
import logging
import textwrap
import time
import docker
import sys
import socket
import ssl

# configure environment:
#   - external variables..
ap = argparse.ArgumentParser(
     prog="ecs-external-instance-network-sentry",
     formatter_class=argparse.RawDescriptionHelpFormatter,
     description=textwrap.dedent('''\
     Purpose:
     --------------
     For use on ECS Anywhere external hosts:
     Configures ECS orchestrated containers to automatically restart
     on failure when on-region ecs control-plane is detected to be unreachable.'''))
ap.add_argument("-r", "--region", required=True,
   help="AWS region where ecs cluster is located.")
ap.add_argument("-i", "--interval", default=20, required=False,
   help="Interval in seconds sentry will sleep between connectivity checks.")
ap.add_argument("-n", "--retries", default=0, required=False,
   help="Number of times Docker will restart a crashing container.")
ap.add_argument("-l", "--logfile", default="/tmp/ecs-external-instance-network-sentry.log", required=False,
   help="Logfile name & location.")
ap.add_argument("-k", "--loglevel", default="DEBUG", required=False,
   help="Log data event severity.")
ap.add_argument("-s", "--restart-strategy", default="cleanup", required=False,
   choices=['cleanup', 'preserve', 'graceful-cutover', 'manual'],
   help="Strategy for handling restarted containers when connectivity returns. 'cleanup' (default): stop and remove restarted containers; 'preserve': keep restarted containers running; 'graceful-cutover': wait for ECS replacements before stopping; 'manual': require manual intervention.")
ap.add_argument("-t", "--cutover-timeout", default=300, required=False, type=int,
   help="Timeout in seconds for graceful-cutover strategy to wait for replacement containers (default: 300).")
args = vars(ap.parse_args())
#   - internal variables..
client = docker.from_env()
context = ssl.create_default_context()
ecs_host = ("ecs." + str(args["region"]) + ".amazonaws.com")
ecs_request_data = "GET / HTTP/1.1\r\nHost: " + ecs_host + "\r\nAccept: text/html\r\n\r\n"
port = 443
all_data=[]
#   - state tracking for graceful-cutover..
cutover_in_progress = False
cutover_start_time = None
restarted_containers = {}  # maps container_id -> container metadata

# logging:
#   - configure logging..
from logging.handlers import RotatingFileHandler
logging.basicConfig(
    handlers=[
        RotatingFileHandler(
          args["logfile"],
          maxBytes=5242880,
          backupCount=5
        )
    ],
    level=args["loglevel"],
    format='%(asctime)s %(levelname)s PID_%(process)d %(message)s'
)
#   - commence logging..
logging.info("[startup] ecs-external-instance-network-sentry - starting..")
logging.info("[startup] arg - aws region: " + str(args["region"]))
logging.info("[startup] arg - interval: " + str(args["interval"]))
logging.info("[startup] arg - retries: " + str(args["retries"]))
logging.info("[startup] arg - logfile: " + str(args["logfile"]))
logging.info("[startup] arg - loglevel: logging." + str(args["loglevel"]))
logging.info("[startup] arg - restart-strategy: " + str(args["restart_strategy"]))
logging.info("[startup] arg - cutover-timeout: " + str(args["cutover_timeout"]))

# main logic as infinite loop..
while True:
    # test connectivity to ecs on-region..
    logging.info("[begin] connectivity test..")
    socket_err = 0
    logging.info("[connect] connecting to ecs at " + str(args["region"]) + "..")
    
    logging.info("[connect] create network socket..")
    try:
        sock = socket.create_connection((ecs_host, port))
    except socket.error as e:
        logging.error("[connect] error creating network socket: %s" % e)
        socket_err = 2

    logging.info("[connect] connecting to host..")
    if socket_err != 2:
        try:
            ssock = context.wrap_socket(sock, server_hostname=ecs_host)
            ssock.settimeout(10)
        except socket.gaierror as e:
            logging.error("[connect] name/address error connecting to host: %s" % e)
            socket_err = 1
        except socket.error as e:
            logging.error("[connect] error connecting to host: %s" % e)
            socket_err = 1

        logging.info("[connect] send/receive data..")
        if socket_err == 0:
            all_data = []
            try:
                ssock.send(ecs_request_data.encode())
                while True:
                    data = ssock.recv(1024)
                    all_data.append(data)
                    if not data:
                        break
                logging.debug("[connect] data: " + str(all_data))
            except socket.error as e:
                logging.error("[connect] error send/receive data: %s" % e)
                socket_err = 1

        if socket_err == 0:
            logging.info("[connect] ecs at " + str(args["region"]) + " is available..")

    # docker configuration:
    #   - error connecting to ecs on-region: update restart policy for ecs managed containers..
    if socket_err != 0:

        logging.info("[ecs-offline] ecs unreachable, configuring container restart policy..")
        for container in client.containers.list():

            # pause the ecs agent..
            if container.name == "ecs-agent":
                if (container.attrs["State"]["Status"]) != "paused":
                    container.pause()
                    logging.info("[ecs-offline] ecs agent paused..")

            # update restart policy for ecs managed containers..
            if container.name != "ecs-agent":
                if "com.amazonaws.ecs.cluster" in container.labels:
                    if (container.attrs["HostConfig"]["RestartPolicy"]["Name"]) != "on-failure":
                        container.update(restart_policy={"Name": "on-failure", "MaximumRetryCount": int(args["retries"])})
                        container.reload()
                        logging.info("[ecs-offline] container name: " + str(container.name))                        
                        logging.info("[ecs-offline] ecs cluster: " + str(container.labels["com.amazonaws.ecs.cluster"]))
                        logging.info("[ecs-offline] set container restart policy: " + str(container.attrs["HostConfig"]["RestartPolicy"]))

    #   - no error connecting to ecs on-region: clean-up if post network outage..
    else:

        logging.info("[ecs-online] ecs is reachable..")

        # strategy: cleanup (default - original behavior)
        if args["restart_strategy"] == "cleanup":
            logging.info("[ecs-online] using 'cleanup' strategy - will stop and remove restarted containers..")
            for container in client.containers.list():

                if container.name != "ecs-agent":
                    if "com.amazonaws.ecs.cluster" in container.labels:

                        # update ecs managed containers:
                        #   - stop & remove containers that have restarted..
                        if (container.attrs["HostConfig"]["RestartPolicy"]["Name"]) == "on-failure":
                            if container.attrs["RestartCount"] > 0:
                                logging.info("[ecs-online] container name: " + str(container.name))
                                logging.info("[ecs-online] ecs cluster: " + str(container.labels["com.amazonaws.ecs.cluster"]))
                                logging.info("[ecs-online] container has been restarted by docker, stopping & removing..")
                                container.stop()
                                container.remove()
                        #   - update restart policy for containers that have not restarted..
                            else:
                                container.update(restart_policy={"Name": "no"})
                                container.reload()
                                logging.info("[ecs-online] container name: " + str(container.name))
                                logging.info("[ecs-online] ecs cluster: " + str(container.labels["com.amazonaws.ecs.cluster"]))
                                logging.info("[ecs-online] set container restart policy: " + str(container.attrs["HostConfig"]["RestartPolicy"]))

                # unpause the ecs agent..
                if container.name == "ecs-agent":
                    if (container.attrs["State"]["Status"]) == "paused":
                        container.unpause()
                        logging.info("[ecs-online] ecs agent unpaused..")

        # strategy: preserve (keep restarted containers running)
        elif args["restart_strategy"] == "preserve":
            logging.info("[ecs-online] using 'preserve' strategy - keeping all restarted containers running..")
            for container in client.containers.list():

                if container.name != "ecs-agent":
                    if "com.amazonaws.ecs.cluster" in container.labels:

                        # update restart policy for all ecs managed containers back to "no"
                        if (container.attrs["HostConfig"]["RestartPolicy"]["Name"]) == "on-failure":
                            container.update(restart_policy={"Name": "no"})
                            container.reload()
                            logging.info("[ecs-online] container name: " + str(container.name))
                            logging.info("[ecs-online] ecs cluster: " + str(container.labels["com.amazonaws.ecs.cluster"]))
                            if container.attrs["RestartCount"] > 0:
                                logging.info("[ecs-online] container was restarted during outage - preserving (RestartCount: " + str(container.attrs["RestartCount"]) + ")")
                            logging.info("[ecs-online] set container restart policy: " + str(container.attrs["HostConfig"]["RestartPolicy"]))

                # unpause the ecs agent..
                if container.name == "ecs-agent":
                    if (container.attrs["State"]["Status"]) == "paused":
                        container.unpause()
                        logging.info("[ecs-online] ecs agent unpaused..")

        # strategy: manual (require manual intervention)
        elif args["restart_strategy"] == "manual":
            logging.info("[ecs-online] using 'manual' strategy - keeping agent paused, manual intervention required..")
            restarted_found = False
            for container in client.containers.list():

                if container.name != "ecs-agent":
                    if "com.amazonaws.ecs.cluster" in container.labels:

                        # update restart policy for all ecs managed containers back to "no"
                        if (container.attrs["HostConfig"]["RestartPolicy"]["Name"]) == "on-failure":
                            container.update(restart_policy={"Name": "no"})
                            container.reload()
                            logging.info("[ecs-online] container name: " + str(container.name))
                            logging.info("[ecs-online] ecs cluster: " + str(container.labels["com.amazonaws.ecs.cluster"]))
                            if container.attrs["RestartCount"] > 0:
                                restarted_found = True
                                logging.info("[ecs-online] container was restarted during outage (RestartCount: " + str(container.attrs["RestartCount"]) + ")")
                            logging.info("[ecs-online] set container restart policy: " + str(container.attrs["HostConfig"]["RestartPolicy"]))

                # DO NOT unpause the ecs agent in manual mode

            if restarted_found:
                logging.warning("[ecs-online] MANUAL INTERVENTION REQUIRED: Containers were restarted during outage. ECS agent remains PAUSED. Review containers and manually unpause agent when ready.")
            else:
                logging.info("[ecs-online] No containers were restarted during outage. Unpausing agent..")
                for container in client.containers.list():
                    if container.name == "ecs-agent":
                        if (container.attrs["State"]["Status"]) == "paused":
                            container.unpause()
                            logging.info("[ecs-online] ecs agent unpaused..")

        # strategy: graceful-cutover (wait for ECS to launch replacements)
        elif args["restart_strategy"] == "graceful-cutover":

            # first time detecting connectivity after outage - identify restarted containers
            if not cutover_in_progress:
                restarted_found = False
                for container in client.containers.list():
                    if container.name != "ecs-agent":
                        if "com.amazonaws.ecs.cluster" in container.labels:
                            if (container.attrs["HostConfig"]["RestartPolicy"]["Name"]) == "on-failure":
                                if container.attrs["RestartCount"] > 0:
                                    restarted_found = True
                                    # track restarted containers
                                    restarted_containers[container.id] = {
                                        "name": container.name,
                                        "cluster": container.labels.get("com.amazonaws.ecs.cluster", "unknown"),
                                        "task_arn": container.labels.get("com.amazonaws.ecs.task-arn", "unknown"),
                                        "restart_count": container.attrs["RestartCount"]
                                    }
                                    logging.info("[ecs-online] identified restarted container: " + str(container.name) + " (RestartCount: " + str(container.attrs["RestartCount"]) + ")")

                if restarted_found:
                    cutover_in_progress = True
                    cutover_start_time = time.time()
                    logging.info("[ecs-online] using 'graceful-cutover' strategy - starting cutover process..")
                    logging.info("[ecs-online] keeping agent paused and waiting for ECS to launch replacement containers (timeout: " + str(args["cutover_timeout"]) + "s)..")
                else:
                    # no containers were restarted, just restore normal operation
                    logging.info("[ecs-online] using 'graceful-cutover' strategy - no containers were restarted, resuming normal operation..")
                    for container in client.containers.list():
                        if container.name != "ecs-agent":
                            if "com.amazonaws.ecs.cluster" in container.labels:
                                if (container.attrs["HostConfig"]["RestartPolicy"]["Name"]) == "on-failure":
                                    container.update(restart_policy={"Name": "no"})
                                    container.reload()
                        if container.name == "ecs-agent":
                            if (container.attrs["State"]["Status"]) == "paused":
                                container.unpause()
                                logging.info("[ecs-online] ecs agent unpaused..")

            # cutover in progress - check if we should complete it
            if cutover_in_progress:
                elapsed_time = time.time() - cutover_start_time
                logging.info("[ecs-online] cutover in progress (elapsed: " + str(int(elapsed_time)) + "s / timeout: " + str(args["cutover_timeout"]) + "s)..")

                # check if timeout has been reached
                if elapsed_time >= args["cutover_timeout"]:
                    logging.warning("[ecs-online] cutover timeout reached! Manual intervention may be required.")
                    logging.warning("[ecs-online] Restarted containers are still running. Review and manually stop/remove them if needed.")
                    # reset restart policies and unpause agent anyway
                    for container in client.containers.list():
                        if container.name != "ecs-agent":
                            if "com.amazonaws.ecs.cluster" in container.labels:
                                if (container.attrs["HostConfig"]["RestartPolicy"]["Name"]) == "on-failure":
                                    container.update(restart_policy={"Name": "no"})
                                    container.reload()
                        if container.name == "ecs-agent":
                            if (container.attrs["State"]["Status"]) == "paused":
                                container.unpause()
                                logging.info("[ecs-online] ecs agent unpaused (after timeout)..")
                    cutover_in_progress = False
                    restarted_containers = {}
                else:
                    # check if ECS has launched replacement containers
                    # look for new containers with same task labels but different IDs
                    current_containers = client.containers.list()
                    all_restarted_ids = set(restarted_containers.keys())

                    # find containers that might be replacements
                    # (have ECS cluster label, not in our restarted list, and not the agent)
                    potential_replacements = []
                    for container in current_containers:
                        if container.name != "ecs-agent":
                            if "com.amazonaws.ecs.cluster" in container.labels:
                                if container.id not in all_restarted_ids:
                                    # this might be a replacement - check if it's running and healthy
                                    if container.attrs["State"]["Status"] == "running":
                                        # check uptime - consider it stable if running for at least 30 seconds
                                        started_at = container.attrs["State"]["StartedAt"]
                                        # we'll consider any new running container as a potential replacement
                                        potential_replacements.append(container)

                    # if we found potential replacements, perform cutover
                    if len(potential_replacements) > 0:
                        logging.info("[ecs-online] found " + str(len(potential_replacements)) + " potential replacement container(s), performing cutover..")

                        # stop and remove restarted containers
                        for container_id, metadata in restarted_containers.items():
                            try:
                                container = client.containers.get(container_id)
                                logging.info("[ecs-online] stopping and removing restarted container: " + str(metadata["name"]))
                                container.stop()
                                container.remove()
                            except Exception as e:
                                logging.error("[ecs-online] error stopping/removing container " + str(metadata["name"]) + ": " + str(e))

                        # reset restart policies for remaining containers
                        for container in client.containers.list():
                            if container.name != "ecs-agent":
                                if "com.amazonaws.ecs.cluster" in container.labels:
                                    if (container.attrs["HostConfig"]["RestartPolicy"]["Name"]) == "on-failure":
                                        container.update(restart_policy={"Name": "no"})
                                        container.reload()

                        # unpause the ecs agent
                        for container in client.containers.list():
                            if container.name == "ecs-agent":
                                if (container.attrs["State"]["Status"]) == "paused":
                                    container.unpause()
                                    logging.info("[ecs-online] ecs agent unpaused - cutover complete!")

                        cutover_in_progress = False
                        restarted_containers = {}
                    else:
                        logging.info("[ecs-online] no replacement containers detected yet, waiting.. (agent remains paused)")

    logging.info("[end] sleeping for " + str(args["interval"]) + " seconds..")
    time.sleep(int(args["interval"]))
