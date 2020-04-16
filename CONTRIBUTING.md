# Contributing to Arcon

All contributions are appreciated! Whether if it is fixing a typo, refactoring existing code, or adding a new feature.

If you are unsure where to start, have a look at the open issues here on Github.

## Getting Started

Fork the arcon repository and create a new branch for the feature you aim to work on. Keep the master branch of the fork clean and have it follow arcon's master.

Create a new branch

```bash
git checkout -b my_feature_branch
```
Add remote upstream (SSH):

```bash
git remote add upstream git@github.com:cda-group/arcon.git
```
Add remote upstream (HTTPS):
  
```bash
git remote add upstream https://github.com/cda-group/arcon.git
```
Whenever you need to sync your fork:

```bash
git pull upstream master
```

## Pull Requests

Arcon uses a squash and merge strategy for all pull requests. This means that all commits of a pull request will be squashed into a single commit.

Some general tips for creating Pull Requests:

1. Provide a description of what your PR adds to Arcon.
2. Motivate your changes. If the PR now uses library X rather than Y to solve a problem, please motivate the change.
3. Keep the PR simple, that is, try not to add several features into a single PR.
4. Connect PR/commit to a github issue, e.g., "closes #4"


Before submitting a PR, make sure to run all related tests and verifications to catch potential errors locally rather than at the CI.
