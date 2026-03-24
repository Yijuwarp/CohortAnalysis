import os

EXCLUDE_DIRS = {"node_modules", ".git", "dist", "build", "__pycache__"}

def should_exclude(path):
    return any(part in EXCLUDE_DIRS for part in path.split(os.sep))

def get_md_files(root):
    md_files = []
    for dirpath, dirnames, filenames in os.walk(root):
        if should_exclude(dirpath):
            continue
        for file in filenames:
            if file.endswith(".md"):
                # Use relative path for deterministic reporting and cleaner output
                rel_path = os.path.relpath(os.path.join(dirpath, file), root)
                md_files.append(rel_path)
    return md_files

def sort_files(files):
    def priority(path):
        # Normalize path separators for consistent comparison
        normalized_path = path.replace(os.sep, "/")
        
        # Root docs (no slashes)
        if "/" not in normalized_path:
            return (0, normalized_path)
        # /docs/
        if normalized_path.startswith("docs/"):
            return (1, normalized_path)
        # /backend/docs/
        if normalized_path.startswith("backend/docs/"):
            return (2, normalized_path)
        # Others
        return (3, normalized_path)
    
    return sorted(files, key=priority)

def combine(files, root=".", output="combined_docs.md"):
    with open(output, "w", encoding="utf-8") as out:
        out.write("# COMBINED DOCUMENTATION\n\n")
        for file in files:
            out.write("\n---\n")
            out.write(f"\n## FILE: {file}\n\n")
            full_path = os.path.join(root, file)
            with open(full_path, "r", encoding="utf-8") as f:
                out.write(f.read())
            out.write("\n")

if __name__ == "__main__":
    root = "."
    files = get_md_files(root)
    # Filter out the output file itself if it exists
    files = [f for f in files if f != "combined_docs.md"]
    files = sort_files(files)
    combine(files, root=root)
    print(f"Generated combined_docs.md with {len(files)} files.")
