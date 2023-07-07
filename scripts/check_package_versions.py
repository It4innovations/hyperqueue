import json
import subprocess

if __name__ == "__main__":
    """
    Checks that HyperQueue and its binding have the same version.
    """
    output = subprocess.check_output(["cargo", "metadata", "--no-deps", "-q"]).decode().strip()
    metadata = json.loads(output)
    packages = metadata["packages"]
    hq_version = [p["version"] for p in packages if p["name"] == "hyperqueue"][0]
    pyhq_version = [p["version"] for p in packages if p["name"] == "pyhq"][0]

    if hq_version != pyhq_version:
        raise Exception(f"Hyperqueue has a different version ({hq_version}) than its Python binding ({pyhq_version})")
