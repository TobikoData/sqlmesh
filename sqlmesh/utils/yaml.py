from __future__ import annotations

import io
from collections import OrderedDict

from ruamel.yaml import YAML, CommentedMap

yaml = YAML()


def dumps(value: CommentedMap | OrderedDict) -> str:
    """Dumps a ruamel.yaml loaded object and converts it into a string"""
    result = io.StringIO()
    yaml.dump(value, result)
    return result.getvalue()
