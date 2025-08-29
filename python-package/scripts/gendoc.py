import sys
from pathlib import Path
from typing import Optional, Union

import docspec
import docstring_to_markdown
from pydoc_markdown.contrib.loaders.python import PythonLoader
from pydoc_markdown.contrib.renderers.markdown import MarkdownRenderer
from pydoc_markdown.interfaces import Context


def name_is_public(name: str) -> bool:
    return name[0] != "_" and name[1] != "_"


def remove_private_method(class_: docspec.Class) -> docspec.Class:
    class_.members = [
        i
        for i in class_.members
        if i.name == "__init__" or name_is_public(i.name)
    ]
    return class_


def process_node(
    node: Union[docspec.Class, docspec.Function],
) -> Union[docspec.Class, docspec.Function]:
    if isinstance(node, docspec.Class):
        return remove_private_method(node)
    elif isinstance(node, docspec.Function):
        return node


def fmt_docstrings(
    docstring: Optional[docspec.Docstring],
) -> Optional[docspec.Docstring]:
    if docstring is None:
        return None

    markdown = docstring_to_markdown.convert(docstring.content)

    new_content = "\n".join(
        [line.replace("#### ", "") for line in markdown.split("\n")]
    )

    docstring.content = new_content

    return docstring


def remove_private_function_class(module: docspec.Module) -> docspec.Module:
    new_members: list[docspec._ModuleMemberType] = []

    for member in module.members:
        if isinstance(
            member, (docspec.Class, docspec.Function)
        ) and name_is_public(member.name):
            if isinstance(member, docspec.Class):
                class_ = remove_private_method(member)
                class_.docstring = fmt_docstrings(class_.docstring)

                class_methods = []
                for method in class_.members:
                    method.docstring = fmt_docstrings(method.docstring)
                    class_methods.append(method)

                class_.members = class_methods
                new_members.append(class_)

            if isinstance(member, docspec.Function) and name_is_public(
                member.name
            ):
                member.docstring = fmt_docstrings(member.docstring)

                new_members.append(member)

    module.members = new_members

    return module


def generate(save_dir: Path):
    context = Context(directory=".")
    loader = PythonLoader(
        search_path=["basedosdados"],
        packages=[
            "download.download",
            "download.metadata",
            "upload.dataset",
            "upload.table",
            "upload.storage",
        ],
    )
    renderer = MarkdownRenderer(
        render_module_header=False,
        # Remove anchor <a>
        insert_header_anchors=False,
        header_level_by_type={
            "Module": 2,
            "Class": 3,
            "Method": 4,
            "Function": 3,
            "Variable": 4,
        },
    )

    loader.init(context)
    renderer.init(context)

    modules = list(loader.load())

    pub_modules = [remove_private_function_class(m) for m in modules]

    content = renderer.render_to_string(pub_modules)

    header = """---
title: Python
category: APIs
order: 0
---

# Python
"""

    (save_dir / "api_reference_python.md").write_text(
        "\n".join([header, content]), encoding="utf-8"
    )


if __name__ == "__main__":
    save_path = "--save-path" in sys.argv
    if not save_path:
        raise Exception("Missing --save-path argument")

    dir = Path(sys.argv[-1])

    if not dir.is_dir():
        raise Exception("--save-path must be a directory")

    generate(dir)
