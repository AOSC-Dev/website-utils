# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "toml",
#     "pyyaml",
# ]
# ///
"""
Usage:
uv run conver-wiki-md.py path/to/files

Only used for aoscc pages currently
"""

import argparse
import re
from pathlib import Path

import toml
import yaml

HEADING_LINE = re.compile(r"^(#{1,6})([ \t]+)", re.M)


def bump_headings(text: str) -> str:
    """Add one more # to every ATX heading (up to ######)."""

    def _repl(match):
        hashes, space = match.groups()
        return f"{hashes}#{space}"

    return HEADING_LINE.sub(_repl, text)


FRONT_TOML = re.compile(r"^\+\+\+\s*$(.*?)^\+\+\+\s*$", re.S | re.M)


def toml_to_yaml(toml_block: str) -> str:
    """Convert a TOML front-matter block (without +++) to YAML ***."""
    data = toml.loads(toml_block)

    if "taxonomies" in data:
        data.pop("taxonomies", None)

    yaml_block = yaml.safe_dump(
        data,
        sort_keys=False,
        default_flow_style=False,
        allow_unicode=True,
        width=80,
    )
    return f"---\n{yaml_block}---\n"


MD_LINK = re.compile(r"\(\.\.\/(.*)\)")
IMG_TAG = re.compile(r'<img([^>]*?)src="\.\./([^"]+)"([^>]*?)>', re.I)
IMG_PATH = "/assets/aoscc"


def replace_paths(text: str) -> str:
    """Handle ../ → ./ and special <img …> rewrite."""

    def _md_link_repl(match):
        match = match.group(1).rstrip("/")
        return f"(./{match})"

    text = MD_LINK.sub(_md_link_repl, text)
    text = IMG_TAG.sub(f'<img\\1src="{IMG_PATH}/\\2"\\3>', text)

    return text


TMPL_START_CARD = re.compile(r"{% card\(type=\"([^\"]*)\"\) %}")
TMPL_END = re.compile(r"{% end %}")


def convert_cards(text: str) -> str:
    text = TMPL_START_CARD.sub(r"::: \1", text)
    text = TMPL_END.sub(r":::", text)
    return text


def process_markdown(path: Path) -> None:
    original = path.read_text(encoding="utf-8")

    # front matter
    def _fm_repl(match):
        toml_block = match.group(1)
        return toml_to_yaml(toml_block)

    converted = FRONT_TOML.sub(_fm_repl, original)

    # others
    converted = bump_headings(converted)
    converted = replace_paths(converted)
    converted = convert_cards(converted)

    # backup & overwrite
    path.with_suffix(".md.bak").write_text(original, encoding="utf-8")
    path.write_text(converted, encoding="utf-8")
    print(f"{path.relative_to(start_dir)}")


parser = argparse.ArgumentParser(description="Bulk-convert Markdown files.")
parser.add_argument(
    "directory", default=".", help="Root directory containing .md files"
)
args = parser.parse_args()
start_dir = Path(args.directory).resolve()

for md_file in start_dir.rglob("*.md"):
    process_markdown(md_file)
