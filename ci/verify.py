#!/usr/bin/env python3
"""Verify the fork's local modules and mandatory behavior without provider secrets."""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess
import sys


class VerificationError(RuntimeError):
    pass


def load_manifest(root):
    manifest = json.loads((root / "ci/verification.json").read_text(encoding="utf-8"))
    if not isinstance(manifest, dict):
        raise VerificationError("verification manifest must be a JSON object")
    upstream = manifest.get("upstream", {})
    if not isinstance(upstream, dict) or not all(
        isinstance(upstream.get(key), str) and upstream[key]
        for key in ("repository", "commit", "version")
    ):
        raise VerificationError("upstream repository, commit and version are required")
    if not re.fullmatch(r"[0-9a-f]{40}", upstream["commit"]):
        raise VerificationError("upstream commit must be a full lowercase Git SHA")
    required = manifest.get("required_tests")
    if not isinstance(required, dict) or not required:
        raise VerificationError("required_tests must contain at least one package")
    for package, names in required.items():
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]*", package) or any(
            part in ("", ".", "..") for part in package.split("/")
        ):
            raise VerificationError(f"invalid required package: {package!r}")
        if not isinstance(names, list) or not names or any(
            not isinstance(name, str) or not re.fullmatch(r"Test[A-Za-z0-9_]+", name)
            for name in names
        ):
            raise VerificationError(f"{package}: required tests must be nonempty Test names")
        if len(names) != len(set(names)):
            raise VerificationError(f"{package}: duplicate required test names")
    return manifest


def run(command, root, env=None):
    return subprocess.run(
        command, cwd=root, env=env, check=True, stdout=subprocess.PIPE,
        text=True, encoding="utf-8",
    ).stdout


def workspace(root):
    """Discover modules once and prove Go uses this checkout for every one."""
    root = root.resolve()
    module_files = [root / name / "go.mod" for name in ("core", "framework", "transports")]
    module_files.extend(sorted((root / "plugins").glob("*/go.mod")))
    modules = {}
    for module_file in module_files:
        declaration = re.search(
            r'^\s*module\s+(?:"([^"]+)"|([^\s]+))',
            module_file.read_text(encoding="utf-8"), re.MULTILINE,
        )
        if declaration is None:
            raise VerificationError(f"missing module declaration: {module_file}")
        name = declaration.group(1) or declaration.group(2)
        if name in modules:
            raise VerificationError(f"duplicate local module: {name}")
        modules[name] = module_file.parent.resolve()

    env = {**os.environ, "GOWORK": str(root / "go.work")}
    if not (root / "go.work").exists():
        run(["go", "work", "init"], root, env)
    run(["go", "work", "use", *[str(path) for path in modules.values()]], root, env)
    for name, expected in modules.items():
        result = run(["go", "list", "-m", "-f", "{{.Dir}}|{{.Main}}", name], root, env).strip()
        directory, _, main = result.rpartition("|")
        if main != "true" or Path(directory).resolve() != expected:
            raise VerificationError(f"{name}: expected local main module {expected}, got {result!r}")
    print(f"Verified {len(modules)} local workspace modules", flush=True)
    return env, modules


def changed_go_files(root, manifest):
    base = manifest["upstream"]["commit"]
    exists = subprocess.run(
        ["git", "cat-file", "-e", f"{base}^{{commit}}"], cwd=root,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    if exists.returncode:
        run(["git", "fetch", "--depth=1", manifest["upstream"]["repository"], base], root)
    changed = subprocess.check_output(
        ["git", "diff", "--name-only", "-z", "--no-renames", "--diff-filter=ACDMRT",
         base, "--", "*.go"], cwd=root,
    )
    return [os.fsdecode(path) for path in changed.split(b"\0") if path]


def check_format(root, manifest):
    paths = [path for path in changed_go_files(root, manifest) if (root / path).is_file()]
    unformatted = []
    for offset in range(0, len(paths), 100):
        output = run(["gofmt", "-l", "--", *paths[offset:offset + 100]], root)
        if output:
            unformatted.append(output.rstrip())
    if unformatted:
        raise VerificationError("gofmt required:\n" + "\n".join(unformatted))
    print(f"Verified formatting of {len(paths)} Go files changed from {manifest['upstream']['commit']}", flush=True)


def changed_test_packages(root, manifest, env, modules):
    directories = sorted({(root / path).parent.resolve() for path in changed_go_files(root, manifest)})
    packages = []
    for directory in directories:
        # A removed package has no runnable corpus. Other directories are loaded
        # by Go itself, including its build constraints and external test files.
        if not any(directory.glob("*.go")):
            continue
        module = next((name for name, path in sorted(modules.items(), key=lambda item: len(item[1].parts), reverse=True)
                       if directory.is_relative_to(path)), None)
        if module is None:
            continue
        package = json.loads(run([
            "go", "list", "-json=Dir,ImportPath,Module,TestGoFiles,XTestGoFiles",
            "./" + directory.relative_to(root.resolve()).as_posix(),
        ], root, env))
        if (Path(package["Dir"]).resolve() != directory
                or package.get("Module", {}).get("Path") != module
                or package.get("Module", {}).get("Main") is not True):
            raise VerificationError(f"changed package did not resolve to local module {module}: {directory}")
        if package.get("TestGoFiles") or package.get("XTestGoFiles"):
            packages.append(package["ImportPath"])
    if not packages:
        raise VerificationError("no changed local Go packages with tests")
    return packages


def test_command(package, required):
    command = ["go", "test", "-json", "-count=1", "-race", "-timeout=5m", "-skip=^$"]
    if package.endswith("/plugins/semanticcache"):
        command.extend(["-short", "-run=."])
    else:
        command.extend(["-short=false", "-run=^(" + "|".join(required) + ")$"])
    return [*command, package]


class TestReport:
    """Consume Go JSON events; a parent pass cannot hide a skipped required child."""

    def __init__(self, package, required):
        self.package = package
        self.required = set(required)
        self.passed = set()
        self.skipped = set()
        self.failed = set()
        self.package_passed = False
        self.tests_seen = False
        self.invalid = []

    def accept(self, line, output=None):
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            self.invalid.append(f"invalid go test JSON: {line[:200].rstrip()}")
            return
        if not isinstance(event, dict):
            self.invalid.append("go test event must be a JSON object")
            return
        if output is not None and isinstance(event.get("Output"), str):
            print(event["Output"], end="", file=output, flush=True)
        if event.get("Package") != self.package:
            return
        action, name = event.get("Action"), event.get("Test")
        if name is None:
            if action == "pass":
                self.package_passed = True
            elif action in ("fail", "skip"):
                self.failed.add(f"package {action}")
        elif isinstance(name, str):
            if action in ("run", "pass", "fail", "skip"):
                self.tests_seen = True
            if action == "pass" and name in self.required:
                self.passed.add(name)
            elif action == "fail":
                self.failed.add(name)
            elif action == "skip" and name.split("/", 1)[0] in self.required:
                self.skipped.add(name)

    def errors(self, returncode):
        errors = list(self.invalid)
        if returncode:
            errors.append(f"go test exited {returncode}")
        if not self.package_passed:
            errors.append("package did not pass")
        if not self.tests_seen:
            errors.append("no tests executed")
        if self.failed:
            errors.append("failed: " + ", ".join(sorted(self.failed)))
        if self.skipped:
            errors.append("required tests or subtests skipped: " + ", ".join(sorted(self.skipped)))
        if self.required - self.passed:
            errors.append("required tests did not pass: " + ", ".join(sorted(self.required - self.passed)))
        return errors


def run_test_package(root, env, package, command, required=()):
    report = TestReport(package, required)
    with subprocess.Popen(
        command, cwd=root, env=env,
        stdout=subprocess.PIPE, text=True, encoding="utf-8",
    ) as process:
        for line in process.stdout:
            report.accept(line, sys.stdout)
        returncode = process.wait()
    errors = report.errors(returncode)
    if errors:
        raise VerificationError(f"{package}:\n" + "\n".join(errors))


def check_unit(root, manifest):
    env, modules = workspace(root)
    packages = changed_test_packages(root, manifest, env, modules)
    for package in packages:
        print(f"Testing full short corpus: {package}", flush=True)
        # Complete native HTTP suites exceed five minutes even while individual
        # tests make progress. Keep their aggregate budget separate from the
        # targeted mandatory-regression budget in test_command.
        run_test_package(root, env, package, [
            "go", "test", "-json", "-short", "-race", "-count=1", "-timeout=15m", package,
        ])
    print(f"All {len(packages)} changed package short suites passed", flush=True)


def check_tests(root, manifest):
    env, modules = workspace(root)
    for package, required in manifest["required_tests"].items():
        module = next((name for name in sorted(modules, key=len, reverse=True)
                       if package == name or package.startswith(name + "/")), None)
        if module is None:
            raise VerificationError(f"required package is outside the local workspace: {package}")
        expected = modules[module] / package[len(module):].lstrip("/")
        result = run([
            "go", "list", "-f", "{{.Dir}}|{{.Module.Path}}|{{.Module.Main}}", package,
        ], root, env).strip()
        parts = result.rsplit("|", 2)
        if len(parts) != 3 or parts[1:] != [module, "true"] or Path(parts[0]).resolve() != expected.resolve():
            raise VerificationError(f"{package}: expected local package {expected}, got {result!r}")
        print(f"Testing {package} ({len(required)} mandatory tests)", flush=True)
        run_test_package(root, env, package, test_command(package, required), required)
    print("All required tests passed", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("workspace", "format", "unit", "test"))
    args = parser.parse_args()
    root = Path(__file__).resolve().parent.parent
    try:
        manifest = load_manifest(root)
        if args.command == "workspace":
            workspace(root)
        elif args.command == "format":
            check_format(root, manifest)
        elif args.command == "unit":
            check_unit(root, manifest)
        else:
            check_tests(root, manifest)
    except (VerificationError, OSError, ValueError, subprocess.CalledProcessError) as error:
        print(f"Verification failed: {error}", file=sys.stderr)
        if isinstance(error, subprocess.CalledProcessError) and error.stdout:
            print(error.stdout, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
