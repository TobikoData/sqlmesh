import typing as t

from rich.console import Console
from rich.progress import Column, ProgressColumn, Task, Text
from rich.theme import Theme

theme = Theme(
    {
        "added": "green",
        "removed": "red",
        "direct": "magenta",  # directly modified
        "indirect": "yellow",  # indirectly modified
        "metadata": "cyan",  # metadata updated
    }
)

console = Console(theme=theme)


class BatchColumn(ProgressColumn):
    """Renders completed count/total, "pending".

    Space pads the completed count so that progress length does not change as task progresses
    past powers of 10.

    Source: https://rich.readthedocs.io/en/stable/reference/progress.html#rich.progress.MofNCompleteColumn

    Args:
        separator: Text to separate completed and total values. Defaults to "/".
    """

    def __init__(self, separator: str = "/", table_column: t.Optional[Column] = None):
        self.separator = separator
        super().__init__(table_column=table_column)

    def render(self, task: Task) -> Text:
        """Show completed count/total, "pending"."""
        total = int(task.total) if task.total is not None else "?"
        completed = int(task.completed)

        if completed == 0 and task.total is not None and task.total > 0:
            return Text("pending", style="progress.download")

        total_width = len(str(total))
        return Text(
            f"{completed:{total_width}d}{self.separator}{total}",
            style="progress.download",
        )
