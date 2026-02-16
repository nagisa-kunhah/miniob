#!/usr/bin/env python3
import os
import shutil
import subprocess
import sys
import time

import __init__
from miniob.miniob_client import MiniObClient


def wait_for_socket(path: str, timeout_seconds: float = 10.0) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        if os.path.exists(path):
            return True
        time.sleep(0.1)
    return False


def main() -> int:
    repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
    observer = os.environ.get("OBSERVER_BIN", "")
    if not observer:
        candidate = os.path.join(repo_root, "build_debug", "bin", "observer")
        if os.path.isfile(candidate):
            observer = candidate
        else:
            observer = os.path.join(repo_root, "build_noasan", "bin", "observer")
    config = os.path.join(repo_root, "etc", "observer.ini")

    if not os.path.isfile(observer):
        print("observer not found.")
        print("set OBSERVER_BIN to a valid observer binary path.")
        return 2

    data_dir = "/tmp/miniob_repro_data"
    sock = "/tmp/miniob_repro.sock"

    if os.path.exists(sock):
        os.remove(sock)

    shutil.rmtree(data_dir, ignore_errors=True)
    os.makedirs(data_dir, exist_ok=True)

    proc = subprocess.Popen(
        [observer, "-f", config, "-s", sock],
        cwd=data_dir,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    try:
        if not wait_for_socket(sock, 10.0):
            print("server socket not ready")
            return 3

        client = MiniObClient(server_socket=sock, time_limit=5.0)

        def run(sql: str) -> bool:
            ok, data = client.run_sql(sql)
            if not ok:
                print(f"SQL failed: {sql}")
                print(f"reason: {data}")
            return ok

        if not run("create table create_view_t1(id int, age int, name char(10)) storage format=pax;"):
            return 4
        if not run("set execution_mode='chunk_iterator';"):
            return 5

        for i in range(1, 201):
            name = f"N{i:03d}"
            if not run(f"insert into create_view_t1 values({i}, {i}, '{name}');"):
                return 6

        if not run("create materialized view create_view_v1 as select * from create_view_t1;"):
            return 7

        if not run("select * from create_view_v1;"):
            return 8

        print("completed without client-side error")
        return 0
    finally:
        try:
            client.close()
        except Exception:
            pass
        try:
            proc.terminate()
            proc.wait(timeout=5)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass


if __name__ == "__main__":
    sys.exit(main())
