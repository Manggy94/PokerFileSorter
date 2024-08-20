#!/bin/sh

# Suppress .env file from git

git filter-branch --force --index-filter \
'git rm --cached --ignore-unmatch .env' \
--prune-empty --tag-name-filter cat -- --all

# Remove the backup created by filter-branch
git reflog expire --expire=now --all
git gc --prune=now --aggressive

# Force push to update the history
git push origin --force --all
git push origin --force --tags


