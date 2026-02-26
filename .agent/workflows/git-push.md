---
description: Commit and push all changes to GitHub after every modification
---

# Git Commit & Push Workflow

Run this after every code change (even small ones) to keep GitHub in sync.

// turbo-all

1. Stage all changes:
```
git add -A
```

2. Commit with a descriptive message:
```
git commit --no-gpg-sign -m "<descriptive commit message>"
```

3. Push to origin:
```
git push origin master
```

> If push fails due to branch name, try `git push origin master` instead.
