---
description: Branch, Push, and PR Workflow
---
// turbo-all
For every task that involves code or documentation changes:

1. **Create Branch**: Create a new descriptive branch from `main`.
   ```bash
   git checkout -b <branch-name>
   ```

2. **Commit Changes**: Add and commit all changes with a descriptive message.
   ```bash
   git add .
   git commit -m "docs/feat/fix: <description>"
   ```

3. **Push to Origin**: Push the new branch to the remote repository.
   ```bash
   git push origin <branch-name>
   ```

4. **Create PR**: Create a Pull Request using the GitHub CLI.
   ```bash
   gh pr create --title "<PR title>" --body "<PR body>"
   ```

5. **Wait for Merge**: Do not merge the PR yourself. Wait for the user to review and merge it.

6. **Cleanup**: After the PR is merged:
   ```bash
   git checkout main
   git pull origin main
   git branch -d <branch-name>
   ```
