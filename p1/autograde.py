from subprocess import check_output
import traceback, os, json

def os_test():
    with open("os.txt") as f:
        if "Ubuntu 22.04.2 LTS" in f.read():
            return None
    return "could not find Ubuntu 22.04.2 LTS in os.txt"

def cpu_test():
    with open("cpu.txt") as f:
        if "x86_64" in f.read():
            return None
    return "could not find x86_64 in cpu.txt"

def docker_test():
    with open("docker.txt") as f:
        if "24.0.5" in f.read():
            return None
    return "could not find 24.0.5 in docker.txt"

def compose_test():
    with open("compose.txt") as f:
        if "v2.20.2" in f.read():
            return None
    return "could not find v2.20.2 in compose.txt"

def executable_test():
    if not os.path.exists("count.sh"):
        return "missing count.sh"
    if not os.access("count.sh", os.X_OK):
        return "count.sh does not have executable permissions"
    return None

def shebang_test():
    with open("count.sh") as f:
        line = f.readline()
    if line.startswith("#!") and "bash" in line:
        return None
    return "count.sh does not appear to have a bash shebang line"

def build_test():
    os.system("docker rmi -f p1")
    _ = check_output(["docker", "build", ".", "-t", "p1"])

def run_test():
    out = check_output(["docker", "run", "p1"])
    if "2493" in str(out, "utf-8"):
        return None
    return "did not find 2493 in output of Docker container"

def main():    
    results = {
        "score": 0,
    }

    tests = [
        (os_test, 15),
        (cpu_test, 10),
        (docker_test, 15),
        (compose_test, 10),
        (executable_test, 10),
        (shebang_test, 15),
        (build_test, 10),
        (run_test, 15),
    ]

    for fn, possible in tests:
        name = fn.__name__
        points = 0
        try:
            result = fn()
            if not result:
                points = possible
                result = f"PASS ({points}/{possible})"
        except Exception as e:
            result = traceback.format_exception(e)
            print(f"Exception in {name}:\n")
            print("\n".join(result) + "\n")
        results["score"] += points
        results[name] = result
    assert(results["score"] <= 100)
    print(results)
    with open("test.json", "w") as f:
        json.dump(results, f)

if __name__ == "__main__":
    main()
