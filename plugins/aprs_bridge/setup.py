import os
import subprocess
import sys

PACKAGES = ["kiss3", "ax253", "aprs3"]


def run() -> None:
    for pkg in PACKAGES:
        result = subprocess.run(
            [sys.executable, "-m", "pip", "install", "--quiet", pkg],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            sys.stderr.write(f"aprs_bridge setup: failed to install {pkg}: {result.stderr}\n")
            sys.exit(1)

    sentinel = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".setup_complete")
    with open(sentinel, "w") as f:
        f.write("1")


if __name__ == "__main__":
    run()
