"""Inspect the retained draft and corrected worker registration."""
import ast
from pathlib import Path


def auto_ack(path):
    tree = ast.parse(Path(path).read_text(encoding="utf-8"))
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            if node.func.attr == "basic_consume":
                for kw in node.keywords:
                    if kw.arg == "auto_ack" and isinstance(kw.value, ast.Constant):
                        return kw.value.value
    raise RuntimeError("No explicit auto_ack setting found")


if __name__ == "__main__":
    draft = auto_ack("audit/initial_worker.py.txt")
    final = auto_ack("src/worker.py")
    print("DRAFT AUDIT:", "FAIL - auto_ack=True acknowledges before work" if draft else "PASS")
    print("FINAL AUDIT:", "PASS - auto_ack=False" if final is False else "FAIL")
    assert draft is True and final is False
    print("See test_commit_before_ack_crash_does_not_repeat_effect for behavioral verification.")
