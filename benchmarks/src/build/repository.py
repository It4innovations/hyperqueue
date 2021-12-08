import contextlib
import logging

from git import Repo
from git.repo.fun import rev_parse

from .. import ROOT_DIR

REPO = Repo(ROOT_DIR)

# This tag represents the active git workspace
TAG_WORKSPACE = "current"


def resolve_tag(tag: str) -> str:
    if tag == TAG_WORKSPACE:
        return tag
    return rev_parse(REPO, tag).hexsha


@contextlib.contextmanager
def checkout_tag(tag: str):
    if tag == TAG_WORKSPACE:
        yield
    else:
        active_branch = REPO.active_branch

        logging.info("Stashing repository")
        REPO.git.stash()
        try:
            aliases = REPO.git.name_rev(["--name-only", tag]).split()
            logging.info(f"Checking out {tag} ({', '.join(aliases)})")
            REPO.git.checkout(tag)
            yield
        finally:
            logging.info(f"Reverting to original state ({active_branch})")
            REPO.git.checkout(active_branch)
            REPO.git.stash("pop")
