import os
import pytest

def test_no_funnel_references_in_app():
    """
    Guard test: Ensures that 'funnel' does not appear in any backend application code (.py files).
    This prevents regression where dead code might be re-introduced or lingering imports remain.
    """
    root_dir = os.path.join(os.getcwd(), "backend", "app")
    
    # We allow "funnel" in these specific contexts (e.g. documentation or this test itself)
    # but for app code, it should be strictly zero.
    
    found_references = []
    
    for root, _, files in os.walk(root_dir):
        for f in files:
            if f.endswith(".py"):
                path = os.path.join(root, f)
                with open(path, "r", encoding="utf-8") as file:
                    content = file.read().lower()
                    if "funnel" in content:
                        # Find the specific line for better error reporting
                        file.seek(0)
                        for line_no, line in enumerate(file, 1):
                            if "funnel" in line.lower():
                                found_references.append(f"{f}:{line_no} -> {line.strip()}")
    
    if found_references:
        pytest.fail(f"Found 'funnel' references in backend/app:\n" + "\n".join(found_references))

if __name__ == "__main__":
    test_no_funnel_references_in_app()
