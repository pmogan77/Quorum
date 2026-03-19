#!/usr/bin/env python3

import argparse
import json
from pathlib import Path


def generate_lab(base_dir, cfg):

    server_count = cfg["server_count"]
    coord_count = cfg["coordinator_count"]

    server_prefix = cfg["server_name_prefix"]
    server_start = cfg["server_start_index"]

    coord_prefix = cfg["coordinator_name_prefix"]
    coord_start = cfg["coordinator_start_index"]

    network = cfg["network_name"]
    subnet = cfg["subnet_cidr"].split("/")[1]

    grpc_port = cfg["grpc_port"]

    image = cfg["device_image"]
    mem = cfg["device_memory"]
    cpus = cfg["device_cpus"]

    base_ip = cfg["server_ip_base"]

    lab_lines = []

    cluster = {"nodes": {}, "coordinators": {}}

    # storage nodes
    for i in range(server_count):

        node_index = server_start + i
        name = f"{server_prefix}{node_index}"
        ip = f"{base_ip}{node_index}"

        lab_lines.append(f'{name}[0]="{network}"')
        lab_lines.append(f'{name}[image]="{image}"')
        lab_lines.append(f'{name}[mem]="{mem}"')
        lab_lines.append(f'{name}[cpus]="{cpus}"')

        startup = f"""#!/bin/bash
set -e
ip addr add {ip}/{subnet} dev eth0
ip link set eth0 up
python -u /shared/server.py --host {ip} --port {grpc_port} --node-id {name} >/tmp/kv-server.log 2>&1 &
"""

        (base_dir / f"{name}.startup").write_text(startup)

        (base_dir / name).mkdir(exist_ok=True)

        cluster["nodes"][name] = {"host": ip, "port": grpc_port}

    # coordinators
    coord_base_ip = cfg["coordinator_ip_base"]

    for i in range(coord_count):

        coord_index = coord_start + i
        name = f"{coord_prefix}{coord_index}"

        ip = f"{coord_base_ip}{100 + coord_index}"

        lab_lines.append(f'{name}[0]="{network}"')
        lab_lines.append(f'{name}[image]="{image}"')
        lab_lines.append(f'{name}[mem]="{mem}"')
        lab_lines.append(f'{name}[cpus]="{cpus}"')

        startup = f"""#!/bin/bash
set -e
ip addr add {ip}/{subnet} dev eth0
ip link set eth0 up
python -u /shared/coordinator.py --node-id {name} >/tmp/coordinator.log 2>&1 &
"""

        (base_dir / f"{name}.startup").write_text(startup)

        (base_dir / name).mkdir(exist_ok=True)

        cluster["coordinators"][name] = {"host": ip, "port": 6000}

    # client node
    client_name = cfg["client_name"]
    client_ip = cfg["client_ip"]

    lab_lines.append(f'{client_name}[0]="{network}"')
    lab_lines.append(f'{client_name}[image]="{image}"')
    lab_lines.append(f'{client_name}[mem]="{mem}"')
    lab_lines.append(f'{client_name}[cpus]="{cpus}"')

    client_startup = f"""#!/bin/bash
set -e
ip addr add {client_ip}/{subnet} dev eth0
ip link set eth0 up
"""

    (base_dir / f"{client_name}.startup").write_text(client_startup)

    (base_dir / client_name).mkdir(exist_ok=True)

    # redis node
    redis_name = cfg["redis_name"]
    redis_ip = cfg["redis_ip"]
    redis_port = cfg["redis_port"]
    redis_image = cfg["redis_image"]

    lab_lines.append(f'{redis_name}[0]="{network}"')
    lab_lines.append(f'{redis_name}[image]="{redis_image}"')
    lab_lines.append(f'{redis_name}[mem]="{mem}"')
    lab_lines.append(f'{redis_name}[cpus]="{cpus}"')

    redis_startup = f"""#!/bin/bash
set -e
ip addr add {redis_ip}/{subnet} dev eth0
ip link set eth0 up
"""

    (base_dir / f"{redis_name}.startup").write_text(redis_startup)
    (base_dir / redis_name).mkdir(exist_ok=True)

    cluster["redis"] = {"host": redis_ip, "port": redis_port}

    # calculate quorum values
    W = (server_count // 2) + 1
    R = server_count - W + 1

    shift = cfg["adaptive_quorum_ratio_update"]["policy_shift"]

    W_read = W + shift
    R_read = R - shift

    cluster["quorum_policies"] = {
        "write_opt": {"R": R, "W": W},
        "read_opt": {"R": R_read, "W": W_read},
    }

    cluster["adaptive_policy_ratio_update"] = cfg["adaptive_quorum_ratio_update"]
    cluster["adaptive_policy_timer_exponential_update"] = cfg["adaptive_quorum_timer_exponential_update"]

    cluster["tombstone"] = cfg["tombstone"]
    cluster["adaptive_mode"] = cfg["adaptive_mode"]

    cluster["metrics"] = cfg["metrics"]

    # auto-generate cluster.json
    shared = base_dir / "shared"
    shared.mkdir(exist_ok=True)

    (shared / "cluster.json").write_text(json.dumps(cluster, indent=2))

    lab_lines.insert(0, f'LAB_NAME="{cfg["lab_name"]}"')
    lab_lines.insert(1, f'LAB_DESCRIPTION="{cfg["lab_description"]}"')
    lab_lines.insert(2, f'LAB_VERSION="{cfg["lab_version"]}"')
    lab_lines.insert(3, f'LAB_AUTHOR="{cfg["lab_author"]}"')

    (base_dir / "lab.conf").write_text("\n".join(lab_lines))


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument("--config", default="config/lab_config.json")

    parser.add_argument("--base-dir", default=".")

    args = parser.parse_args()

    base = Path(args.base_dir)

    cfg = json.load(open(args.config))

    generate_lab(base, cfg)


if __name__ == "__main__":
    main()
