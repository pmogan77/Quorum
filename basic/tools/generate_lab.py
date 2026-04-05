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

    collector_name = cfg["collector_name"]
    collector_ip = cfg["collector_ip"]
    collector_image = cfg["collector_image"]

    jaeger_name = cfg["jaeger_name"]
    jaeger_ip = cfg["jaeger_ip"]
    jaeger_image = cfg["jaeger_image"]

    collector_otlp_grpc_port = cfg["collector_otlp_grpc_port"]
    collector_otlp_http_port = cfg["collector_otlp_http_port"]

    jaeger_ui_port = cfg["jaeger_ui_port"]
    jaeger_otlp_grpc_port = cfg["jaeger_otlp_grpc_port"]
    jaeger_otlp_http_port = cfg["jaeger_otlp_http_port"]

    lab_lines = []

    cluster = {
        "nodes": {},
        "coordinators": {}
    }

    
    # storage servers
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
mkdir -p /shared/logs/{name}
python -u /shared/server.py --host {ip} --port {grpc_port} --node-id {name} >/shared/logs/{name}/kv-server.log 2>&1 &
"""

        (base_dir / f"{name}.startup").write_text(startup)

        (base_dir / name).mkdir(exist_ok=True)

        cluster["nodes"][name] = {
            "host": ip,
            "port": grpc_port
        }

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
mkdir -p /shared/logs/{name}
python -u /shared/coordinator.py --node-id {name} >/shared/logs/{name}/coordinator.log 2>&1 &
"""

        (base_dir / f"{name}.startup").write_text(startup)

        (base_dir / name).mkdir(exist_ok=True)

        cluster["coordinators"][name] = {
            "host": ip,
            "port": 6000
        }

    # client nodes
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


    # otel collector
    lab_lines.append(f'{collector_name}[0]="{network}"')
    lab_lines.append(f'{collector_name}[image]="{collector_image}"')
    lab_lines.append(f'{collector_name}[mem]="{mem}"')
    lab_lines.append(f'{collector_name}[cpus]="{cpus}"')

    collector_startup = f"""#!/bin/bash
set -e
ip addr add {collector_ip}/{subnet} dev eth0
ip link set eth0 up
mkdir -p /shared/logs/{collector_name}
otelcol-contrib --config=/shared/otel/collector-config.yaml >/shared/logs/{collector_name}/collector.log 2>&1 &
"""

    (base_dir / f"{collector_name}.startup").write_text(collector_startup)
    (base_dir / collector_name).mkdir(exist_ok=True)


    # jaeger
    lab_lines.append(f'{jaeger_name}[image]="{jaeger_image}"')
    lab_lines.append(f'{jaeger_name}[mem]="{mem}"')
    lab_lines.append(f'{jaeger_name}[cpus]="{cpus}"')
    lab_lines.append(f'{jaeger_name}[port]="{jaeger_ui_port}:{jaeger_ui_port}/tcp"')
    lab_lines.append(f'{jaeger_name}[bridged]="true"')

    jaeger_startup = f"""#!/bin/bash
set -e
mkdir -p /shared/logs/{jaeger_name}
jaeger --config=/shared/otel/jaeger-config.yaml >/shared/logs/jaeger/jaeger.log 2>&1 &
"""

    (base_dir / f"{jaeger_name}.startup").write_text(jaeger_startup)
    (base_dir / jaeger_name).mkdir(exist_ok=True)

    cluster["observability"] = {
        "collector": {
            "name": collector_name,
            "host": collector_ip,
            "otlp_grpc_port": collector_otlp_grpc_port,
            "otlp_http_port": collector_otlp_http_port
        },
        "jaeger": {
            "name": jaeger_name,
            "host": jaeger_ip,
            "ui_port": jaeger_ui_port,
            "otlp_grpc_port": jaeger_otlp_grpc_port,
            "otlp_http_port": jaeger_otlp_http_port
        }
    }



    # quorum settings
    cluster["R"] = (server_count // 2) + 1
    cluster["W"] = (server_count // 2) + 1

    cluster["tombstone"] = cfg["tombstone"]

    # auto-generate cluster.json
    shared = base_dir / "shared"
    shared.mkdir(exist_ok=True)

    (shared / "cluster.json").write_text(
        json.dumps(cluster, indent=2)
    )

    # auto generate lab.conf
    lab_lines.insert(0, f'LAB_NAME="{cfg["lab_name"]}"')
    lab_lines.insert(1, f'LAB_DESCRIPTION="{cfg["lab_description"]}"')
    lab_lines.insert(2, f'LAB_VERSION="{cfg["lab_version"]}"')
    lab_lines.insert(3, f'LAB_AUTHOR="{cfg["lab_author"]}"')

    (base_dir / "lab.conf").write_text(
        "\n".join(lab_lines)
    )


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