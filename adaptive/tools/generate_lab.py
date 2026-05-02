#!/usr/bin/env python3

import argparse
import json
from pathlib import Path


def add_common_device_lines(lab_lines, name, image, mem, cpus):
    lab_lines.append(f'{name}[image]="{image}"')
    lab_lines.append(f'{name}[mem]="{mem}"')
    lab_lines.append(f'{name}[cpus]="{cpus}"')


def add_internal_device_lines(lab_lines, name, network, image, mem, cpus):
    lab_lines.append(f'{name}[0]="{network}"')
    add_common_device_lines(lab_lines, name, image, mem, cpus)


def write_startup(base_dir, name, contents):
    (base_dir / f"{name}.startup").write_text(contents)
    (base_dir / name).mkdir(exist_ok=True)


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
    coord_base_ip = cfg["coordinator_ip_base"]

    client_name = cfg["client_name"]
    client_ip = cfg["client_ip"]

    redis_name = cfg["redis_name"]
    redis_ip = cfg["redis_ip"]
    redis_port = cfg["redis_port"]
    redis_image = cfg["redis_image"]

    collector_name = cfg["collector_name"]
    collector_ip = cfg["collector_ip"]
    collector_image = cfg["collector_image"]

    jaeger_name = cfg["jaeger_name"]
    jaeger_ip = cfg["jaeger_ip"]
    jaeger_image = cfg["jaeger_image"]

    collector_otlp_grpc_port = cfg["collector_otlp_grpc_port"]
    collector_otlp_http_port = cfg["collector_otlp_http_port"]

    jaeger_ui_port = cfg["jaeger_ui_port"]
    # should be one larger than ui port
    jaeger_expose_port = cfg["jaeger_ui_port"] + 1
    jaeger_otlp_grpc_port = cfg["jaeger_otlp_grpc_port"]
    jaeger_otlp_http_port = cfg["jaeger_otlp_http_port"]

    lab_lines = []

    cluster = {
        "nodes": {},
        "coordinators": {},
    }

    shared = base_dir / "shared"
    shared.mkdir(exist_ok=True)
    (shared / "logs").mkdir(exist_ok=True)

    # storage nodes
    for i in range(server_count):
        node_index = server_start + i
        name = f"{server_prefix}{node_index}"
        ip = f"{base_ip}{node_index}"

        add_internal_device_lines(lab_lines, name, network, image, mem, cpus)

        startup = f"""#!/bin/bash
set -e
ip addr add {ip}/{subnet} dev eth0
ip link set eth0 up
mkdir -p /shared/logs/{name}
python -u /shared/server.py --host {ip} --port {grpc_port} --node-id {name} >/shared/logs/{name}/kv-server.log 2>&1 &
"""

        write_startup(base_dir, name, startup)

        cluster["nodes"][name] = {
            "host": ip,
            "port": grpc_port,
        }

    # coordinators
    for i in range(coord_count):
        coord_index = coord_start + i
        name = f"{coord_prefix}{coord_index}"
        ip = f"{coord_base_ip}{100 + coord_index}"

        add_internal_device_lines(lab_lines, name, network, image, mem, cpus)

        startup = f"""#!/bin/bash
set -e
ip addr add {ip}/{subnet} dev eth0
ip link set eth0 up
mkdir -p /shared/logs/{name}
python -u /shared/coordinator.py --node-id {name} >/shared/logs/{name}/coordinator.log 2>&1 &
"""

        write_startup(base_dir, name, startup)

        cluster["coordinators"][name] = {
            "host": ip,
            "port": 6000,
        }

    # client node
    add_internal_device_lines(lab_lines, client_name, network, image, mem, cpus)

    client_startup = f"""#!/bin/bash
set -e
ip addr add {client_ip}/{subnet} dev eth0
ip link set eth0 up
mkdir -p /shared/logs/{client_name}
"""

    write_startup(base_dir, client_name, client_startup)

    # redis node
    add_internal_device_lines(lab_lines, redis_name, network, redis_image, mem, cpus)

    redis_startup = f"""#!/bin/bash
set -e
ip addr add {redis_ip}/{subnet} dev eth0
ip link set eth0 up
mkdir -p /shared/logs/{redis_name}
"""

    write_startup(base_dir, redis_name, redis_startup)

    cluster["redis"] = {
        "host": redis_ip,
        "port": redis_port,
    }

    # otel collector
    add_internal_device_lines(lab_lines, collector_name, network, collector_image, mem, cpus)

    collector_startup = f"""#!/bin/bash
set -e
ip addr add {collector_ip}/{subnet} dev eth0
ip link set eth0 up
mkdir -p /shared/logs/{collector_name}
otelcol-contrib --config=/shared/otel/collector-config.yaml >/shared/logs/{collector_name}/collector.log 2>&1 &
"""

    write_startup(base_dir, collector_name, collector_startup)

    # jaeger
    add_common_device_lines(lab_lines, jaeger_name, jaeger_image, mem, cpus)
    lab_lines.append(f'{jaeger_name}[port]="{jaeger_expose_port}:{jaeger_ui_port}/tcp"')
    lab_lines.append(f'{jaeger_name}[bridged]="true"')

    jaeger_startup = f"""#!/bin/bash
set -e
mkdir -p /shared/logs/{jaeger_name}
jaeger --config=/shared/otel/jaeger-config.yaml >/shared/logs/{jaeger_name}/jaeger.log 2>&1 &
"""

    write_startup(base_dir, jaeger_name, jaeger_startup)

    cluster["observability"] = {
        "collector": {
            "name": collector_name,
            "host": collector_ip,
            "otlp_grpc_port": collector_otlp_grpc_port,
            "otlp_http_port": collector_otlp_http_port,
        },
        "jaeger": {
            "name": jaeger_name,
            "host": jaeger_ip,
            "ui_port": jaeger_ui_port,
            "otlp_grpc_port": jaeger_otlp_grpc_port,
            "otlp_http_port": jaeger_otlp_http_port,
        },
    }

    # quorum values
    w_default = (server_count // 2) + 1
    r_default = server_count - w_default + 1

    shift = cfg["adaptive_quorum"]["policy_shift"]
    w_read = w_default + shift
    r_read = r_default - shift

    cluster["quorum_policies"] = {
        "write_opt": {
            "R": r_default,
            "W": w_default,
        },
        "read_opt": {
            "R": r_read,
            "W": w_read,
        },
    }

    cluster["adaptive_policy"] = cfg["adaptive_quorum"]
    cluster["tombstone"] = cfg["tombstone"]

    # auto-generate cluster.json
    (shared / "cluster.json").write_text(json.dumps(cluster, indent=2))

    # auto-generate lab.conf
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