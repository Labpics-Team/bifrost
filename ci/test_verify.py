"""Fast checks for the verifier's refusal and local-checkout guarantees."""

from contextlib import redirect_stdout
import copy
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest.mock import patch

import verify


def manifest():
    return {
        "upstream": {"repository": "https://example.invalid/upstream.git", "commit": "a" * 40, "version": "1.0.0"},
        "required_tests": {"example.org/core": ["TestRequired"]},
    }


def git(root, *args):
    return subprocess.check_output(
        ["git", "-c", "core.autocrlf=false", "-c", "user.name=Verifier Test", "-c", "user.email=verifier@example.invalid", *args],
        cwd=root, text=True, encoding="utf-8", stderr=subprocess.PIPE,
    ).strip()


class ManifestTests(unittest.TestCase):
    def test_manifest_rejects_missing_or_ambiguous_tests(self):
        variants = [
            ({}, "at least one package"),
            ({"example.org/core": []}, "nonempty"),
            ({"example.org/core": ["TestRequired", "TestRequired"]}, "duplicate"),
            ({"example.org/core": ["TestRequired/child"]}, "Test names"),
            ({"../core": ["TestRequired"]}, "invalid required package"),
        ]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "ci").mkdir()
            for roster, message in variants:
                with self.subTest(roster=roster):
                    value = manifest()
                    value["required_tests"] = roster
                    (root / "ci/verification.json").write_text(json.dumps(value), encoding="utf-8")
                    with self.assertRaisesRegex(verify.VerificationError, message):
                        verify.load_manifest(root)

    def test_manifest_preserves_roster_and_requires_full_upstream_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "ci").mkdir()
            value = manifest()
            (root / "ci/verification.json").write_text(json.dumps(value), encoding="utf-8")
            self.assertEqual(verify.load_manifest(root), value)
            for key in ("repository", "commit", "version"):
                with self.subTest(key=key):
                    broken = copy.deepcopy(value)
                    broken["upstream"][key] = ""
                    (root / "ci/verification.json").write_text(json.dumps(broken), encoding="utf-8")
                    with self.assertRaises(verify.VerificationError):
                        verify.load_manifest(root)


class TestEventTests(unittest.TestCase):
    package = "example.org/core"

    def report(self, events):
        report = verify.TestRun({self.package: ["TestRequired"]})
        for action, name in events:
            event = {"Package": self.package, "Action": action}
            if name is not None:
                event["Test"] = name
            report.accept(json.dumps(event))
        return report

    def test_missing_skipped_failed_or_incomplete_required_test_is_rejected(self):
        variants = [
            [("pass", None)],
            [("skip", "TestRequired"), ("pass", None)],
            [("fail", "TestRequired"), ("fail", None)],
            [("pass", "TestRequired")],
            [("pass", "TestRequired"), ("skip", None)],
            [("skip", "TestRequired/nested/child"), ("pass", "TestRequired"), ("pass", None)],
            [("fail", "TestOther"), ("pass", "TestRequired"), ("pass", None)],
        ]
        for events in variants:
            with self.subTest(events=events):
                self.assertTrue(self.report(events).errors(0))

    def test_unrelated_backend_skip_is_allowed(self):
        report = self.report([
            ("skip", "TestExternalBackend"),
            ("skip", "TestRequiredPrefix/child"),
            ("pass", "TestRequired/child"),
            ("pass", "TestRequired"),
            ("pass", None),
        ])
        self.assertEqual(report.errors(0), [])
        self.assertTrue(report.errors(1))

    def test_events_from_another_package_cannot_satisfy_required_test(self):
        report = self.report([("pass", None)])
        report.accept(json.dumps({"Package": "example.org/other", "Action": "pass", "Test": "TestRequired"}))
        self.assertTrue(report.errors(0))

    def test_malformed_json_cannot_report_success(self):
        for line in ("not json", "[]"):
            with self.subTest(line=line):
                report = self.report([("pass", "TestRequired"), ("pass", None)])
                report.accept(line)
                self.assertTrue(report.errors(0))

    def test_interleaved_packages_must_each_finish_their_own_corpus(self):
        one, two = "example.org/one", "example.org/two"
        result = verify.TestRun({one: ["TestRequired"], two: ["TestRequired"]})
        def accept(package, action, name=None):
            event = {"Package": package, "Action": action}
            if name is not None:
                event["Test"] = name
            result.accept(json.dumps(event))

        accept(one, "run", "TestRequired")
        accept(two, "run", "TestRequired")
        accept(one, "pass", "TestRequired")
        accept(one, "pass")
        self.assertTrue(any(two in error for error in result.errors(0)))
        self.assertTrue(any("required tests did not pass" in error for error in result.errors(0)))
        accept(two, "pass", "TestRequired")
        accept(two, "pass")
        self.assertEqual(result.errors(0), [])
        self.assertTrue(result.errors(1))
        result.accept("invalid")
        self.assertTrue(result.errors(0))

    def test_empty_package_roster_is_rejected(self):
        with self.assertRaisesRegex(verify.VerificationError, "empty test package roster"):
            verify.TestRun({})

    def test_semanticcache_runs_the_full_short_corpus_and_vectorstore_is_not_short(self):
        semantic = verify.mandatory_test_flags("github.com/maximhq/bifrost/plugins/semanticcache", ["TestRequired"])
        self.assertIn("-short", semantic)
        self.assertIn("-run=.", semantic)
        vectorstore = verify.mandatory_test_flags("github.com/maximhq/bifrost/framework/vectorstore", ["TestRequired", "TestOther"])
        self.assertIn("-short=false", vectorstore)
        self.assertIn("-run=^(TestRequired|TestOther)$", vectorstore)
        for command in (semantic, vectorstore):
            self.assertIn("-count=1", command)
            self.assertIn("-race", command)
            self.assertIn("-skip=^$", command)


@unittest.skipUnless(shutil.which("go"), "Go is needed for local workspace checks")
class WorkspaceTests(unittest.TestCase):
    def test_removed_plugin_is_pruned_from_an_existing_workspace(self):
        with tempfile.TemporaryDirectory() as directory, redirect_stdout(io.StringIO()):
            root = Path(directory).resolve()
            for name in ("core", "framework", "transports", "plugins/retired"):
                module = root / name
                module.mkdir(parents=True)
                (module / "go.mod").write_text(f"module example.org/{name}\n\ngo 1.20\n", encoding="utf-8")
            env = {"GOTOOLCHAIN": "local", "GOPROXY": "off", "GOSUMDB": "off", "GOFLAGS": ""}
            with patch.dict(os.environ, env):
                verify.workspace(root)
                shutil.rmtree(root / "plugins/retired")
                local_env, modules = verify.workspace(root)
                actual = subprocess.check_output(
                    ["go", "list", "-m", "-f", "{{.Path}}", "all"],
                    cwd=root, env=local_env, text=True, encoding="utf-8",
                ).splitlines()
            self.assertCountEqual(actual, modules)

    def test_new_plugin_is_discovered_and_an_external_workspace_is_ignored(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            for name in ("core", "framework", "transports"):
                (root / name).mkdir()
                (root / name / "go.mod").write_text(f"module example.org/{name}\n\ngo 1.20\n", encoding="utf-8")
            env = {"GOWORK": str(root / "external.work"), "GOTOOLCHAIN": "local", "GOPROXY": "off", "GOSUMDB": "off", "GOFLAGS": ""}
            with patch.dict(os.environ, env), redirect_stdout(io.StringIO()):
                verify.workspace(root)
                plugin = root / "plugins/new-plugin"
                plugin.mkdir(parents=True)
                (plugin / "go.mod").write_text("module example.org/plugins/new-plugin\n\ngo 1.20\n", encoding="utf-8")
                local_env, modules = verify.workspace(root)
            self.assertEqual(local_env["GOWORK"], str(root / "go.work"))
            self.assertEqual(len(modules), 4)
            result = json.loads(subprocess.check_output(
                ["go", "list", "-m", "-json", "example.org/plugins/new-plugin"],
                cwd=root, env=local_env, text=True, encoding="utf-8",
            ))
            self.assertTrue(result["Main"])
            self.assertEqual(Path(result["Dir"]).resolve(), plugin)
            self.assertFalse((root / "external.work").exists())


class FormatToolchainTests(unittest.TestCase):
    def test_formatter_comes_from_selected_go_toolchain(self):
        with tempfile.TemporaryDirectory() as directory, redirect_stdout(io.StringIO()):
            root = Path(directory).resolve()
            (root / "changed.go").write_text("package fixture\n", encoding="utf-8")
            selected_root = root / "selected toolchain"
            formatter = selected_root / "bin" / ("gofmt.exe" if os.name == "nt" else "gofmt")

            def execute(command, cwd):
                self.assertEqual(cwd, root)
                if command == ["go", "env", "GOROOT"]:
                    return str(selected_root) + "\n"
                # A bare gofmt would use the older bundled formatter on PATH.
                self.assertEqual(command, [str(formatter), "-l", "--", "changed.go"])
                return "changed.go\n"

            with patch.object(verify, "changed_go_files", return_value=["changed.go"]), \
                    patch.object(verify, "run", side_effect=execute):
                with self.assertRaisesRegex(verify.VerificationError, "changed.go"):
                    verify.check_format(root, manifest())


@unittest.skipUnless(shutil.which("git") and shutil.which("go") and shutil.which("gofmt"), "Git and Go are needed for format checks")
class FormatTests(unittest.TestCase):
    def initial_repository(self, root):
        root.mkdir()
        git(root, "init", "--quiet", "--initial-branch=labpics")
        (root / "untouched.go").write_text("package fixture\nfunc untouched(){}\n", encoding="utf-8")
        git(root, "add", ".")
        git(root, "commit", "--quiet", "-m", "Baseline")
        value = manifest()
        value["upstream"]["commit"] = git(root, "rev-parse", "HEAD")
        value["upstream"]["repository"] = root.as_uri()
        return value

    def test_clean_committed_checkout_checks_changed_go_files_with_spaces(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve() / "repo"
            value = self.initial_repository(root)
            changed = root / "changed file.go"
            changed.write_text("package fixture\nfunc changed(){}\n", encoding="utf-8")
            git(root, "add", ".")
            git(root, "commit", "--quiet", "-m", "Change")
            self.assertEqual(git(root, "status", "--porcelain"), "")
            with self.assertRaisesRegex(verify.VerificationError, "changed file.go"):
                verify.check_format(root, value)
            subprocess.run(["gofmt", "-w", str(changed)], check=True)
            with redirect_stdout(io.StringIO()):
                verify.check_format(root, value)
            self.assertIn("untouched.go", subprocess.check_output(
                ["gofmt", "-l", "untouched.go"], cwd=root, text=True, encoding="utf-8",
            ))

    def test_untracked_go_files_are_checked_but_ignored_files_are_not(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve() / "repo"
            value = self.initial_repository(root)
            (root / ".gitignore").write_text("ignored.go\n", encoding="utf-8")
            changed = root / "new file.go"
            changed.write_text("package fixture\nfunc added(){}\n", encoding="utf-8")
            (root / "ignored.go").write_text("package ignored\nfunc ignored(){}\n", encoding="utf-8")
            self.assertEqual(verify.changed_go_files(root, value), ["new file.go"])
            with self.assertRaisesRegex(verify.VerificationError, "new file.go"):
                verify.check_format(root, value)
            subprocess.run(["gofmt", "-w", str(changed)], check=True)
            with redirect_stdout(io.StringIO()):
                verify.check_format(root, value)

    def test_shallow_checkout_fetches_the_missing_upstream_base(self):
        with tempfile.TemporaryDirectory() as directory:
            parent = Path(directory).resolve()
            source = parent / "upstream"
            value = self.initial_repository(source)
            (source / "changed.go").write_bytes(b"package fixture\n\nfunc changed() {}\n")
            git(source, "add", ".")
            git(source, "commit", "--quiet", "-m", "Change")
            checkout = parent / "checkout"
            git(parent, "clone", "--quiet", "--depth=1", source.as_uri(), str(checkout))
            with self.assertRaises(subprocess.CalledProcessError):
                git(checkout, "cat-file", "-e", value["upstream"]["commit"])
            with redirect_stdout(io.StringIO()):
                verify.check_format(checkout, value)
            git(checkout, "cat-file", "-e", value["upstream"]["commit"])


@unittest.skipUnless(shutil.which("git") and shutil.which("go"), "Git and Go are needed for package discovery")
class ChangedPackageTests(unittest.TestCase):
    def write_files(self, root, files):
        for name, contents in files.items():
            path = root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(contents.encode("utf-8"))

    def baseline(self, root, files):
        git(root, "init", "--quiet", "--initial-branch=labpics")
        self.write_files(root, {
            **{f"{name}/go.mod": f"module example.org/{name}\n\ngo 1.20\n" for name in ("core", "framework", "transports")},
            ".gitignore": "go.work\ngo.work.sum\n",
            **files,
        })
        git(root, "add", ".")
        git(root, "commit", "--quiet", "-m", "Baseline")
        value = manifest()
        value["upstream"]["commit"] = git(root, "rev-parse", "HEAD")
        value["upstream"]["repository"] = root.as_uri()
        return value

    def setUp(self):
        environment = patch.dict(os.environ, {"GOTOOLCHAIN": "local", "GOPROXY": "off", "GOSUMDB": "off", "GOFLAGS": ""})
        environment.start()
        self.addCleanup(environment.stop)

    def test_new_packages_plugins_and_external_tests_run_without_name_filters(self):
        with tempfile.TemporaryDirectory() as directory, redirect_stdout(io.StringIO()):
            root = Path(directory).resolve()
            value = self.baseline(root, {
                "core/current/current_test.go": "package current\nimport \"testing\"\nfunc TestExisting(t *testing.T) {}\n",
                "core/untouched/untouched_test.go": "package untouched\nimport \"testing\"\nfunc TestUntouched(t *testing.T) {}\n",
                "framework/no_tests/source.go": "package no_tests\nvar Value = 1\n",
            })
            self.write_files(root, {
                "core/current/current_test.go": "package current\nimport \"testing\"\nfunc TestExisting(t *testing.T) {}\nfunc TestNew(t *testing.T) {}\n",
                "core/current/more_test.go": "package current\nimport \"testing\"\nfunc TestMore(t *testing.T) {}\n",
                "core/new_package/external_test.go": "package new_package_test\nimport \"testing\"\nfunc TestNewPackage(t *testing.T) {}\n",
                "framework/no_tests/source.go": "package no_tests\nvar Value = 2\n",
                "plugins/new_plugin/go.mod": "module example.org/plugins/new_plugin\n\ngo 1.20\n",
                "plugins/new_plugin/plugin_test.go": "package new_plugin\nimport \"testing\"\nfunc TestPlugin(t *testing.T) {}\n",
                "tools/outside/outside_test.go": "package outside\nimport \"testing\"\nfunc TestOutside(t *testing.T) {}\n",
            })
            git(root, "add", ".")
            git(root, "commit", "--quiet", "-m", "Add tests")
            self.assertEqual(git(root, "status", "--porcelain"), "")
            with patch.object(verify, "run_test_packages") as execute:
                verify.check_unit(root, value)
            execute.assert_called_once()
            roster, command = execute.call_args.args[2:]
            self.assertCountEqual(roster, [
                "example.org/core/current", "example.org/core/new_package", "example.org/plugins/new_plugin",
            ])
            self.assertTrue({"-short", "-race", "-count=1", "-timeout=15m"}.issubset(command))
            self.assertFalse(any(arg.startswith("-run") for arg in command))

    def test_untracked_new_package_is_included_in_unit_verification(self):
        with tempfile.TemporaryDirectory() as directory, redirect_stdout(io.StringIO()):
            root = Path(directory).resolve()
            value = self.baseline(root, {"core/source.go": "package core\nvar Value = 1\n"})
            self.write_files(root, {
                "core/new_package/new_test.go": "package new_package\nimport \"testing\"\nfunc TestNew(t *testing.T) { t.Fatal(\"must run\") }\n",
            })
            with patch.object(verify, "run_test_packages") as execute:
                verify.check_unit(root, value)
            execute.assert_called_once()
            self.assertEqual(list(execute.call_args.args[2]), ["example.org/core/new_package"])

    def test_native_go_package_list_executes_each_expected_package(self):
        with tempfile.TemporaryDirectory() as directory, redirect_stdout(io.StringIO()):
            root = Path(directory).resolve()
            self.write_files(root, {
                "go.mod": "module example.org/proof\n\ngo 1.20\n",
                "one/one_test.go": 'package one\nimport "testing"\nfunc TestRequired(t *testing.T) {}\n',
                "two/two_test.go": 'package two\nimport "testing"\nfunc TestRequired(t *testing.T) {}\n',
            })
            packages = {f"example.org/proof/{name}": ["TestRequired"] for name in ("one", "two")}
            env = {**os.environ, "GOWORK": "off", "CGO_ENABLED": "0"}
            flags = ["-count=1", "-p=2", "-timeout=30s"]
            verify.run_test_packages(root, env, packages, flags)
            self.write_files(root, {
                "two/two_test.go": 'package two\nimport "testing"\nfunc TestOther(t *testing.T) {}\n',
            })
            with self.assertRaisesRegex(verify.VerificationError, "example.org/proof/two: required tests did not pass"):
                verify.run_test_packages(root, env, packages, flags)

    def test_deletions_run_remaining_package_tests_and_ignore_removed_packages(self):
        with tempfile.TemporaryDirectory() as directory, redirect_stdout(io.StringIO()):
            root = Path(directory).resolve()
            value = self.baseline(root, {
                "core/remaining/obsolete.go": "package remaining\nvar Obsolete = 1\n",
                "core/remaining/remaining_test.go": "package remaining\nimport \"testing\"\nfunc TestRemaining(t *testing.T) {}\n",
                "core/removed/removed_test.go": "package removed\nimport \"testing\"\nfunc TestRemoved(t *testing.T) {}\n",
                "core/removed/README.md": "Former package\n",
            })
            (root / "core/remaining/obsolete.go").unlink()
            (root / "core/removed/removed_test.go").unlink()
            git(root, "add", ".")
            git(root, "commit", "--quiet", "-m", "Remove source")
            env, modules = verify.workspace(root)
            self.assertEqual(verify.changed_test_packages(root, value, env, modules), ["example.org/core/remaining"])

    def test_empty_discovery_or_empty_runtime_corpus_cannot_pass(self):
        with tempfile.TemporaryDirectory() as directory, redirect_stdout(io.StringIO()):
            root = Path(directory).resolve()
            value = self.baseline(root, {"core/source.go": "package core\nvar Value = 1\n"})
            self.write_files(root, {"core/source.go": "package core\nvar Value = 2\n"})
            env, modules = verify.workspace(root)
            with self.assertRaisesRegex(verify.VerificationError, "no changed local Go packages with tests"):
                verify.changed_test_packages(root, value, env, modules)
            report = verify.TestRun({"example.org/core": []})
            report.accept(json.dumps({"Package": "example.org/core", "Action": "pass"}))
            self.assertTrue(any("no tests executed" in error for error in report.errors(0)))


if __name__ == "__main__":
    unittest.main()
