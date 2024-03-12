"""Helpers for building robust Slack messages"""

import json
import sys
import typing as t
from enum import Enum
from textwrap import dedent

if sys.version_info >= (3, 8):
    from typing import TypedDict
else:
    from typing_extensions import TypedDict


SLACK_MAX_TEXT_LENGTH = 3000
SLACK_MAX_ALERT_PREVIEW_BLOCKS = 5
SLACK_MAX_ATTACHMENTS_BLOCKS = 50
CONTINUATION_SYMBOL = "..."


TSlackBlock = t.Dict[str, t.Any]


class TSlackBlocks(TypedDict):
    blocks: t.List[TSlackBlock]


class TSlackMessage(TSlackBlocks):
    attachments: t.List[TSlackBlocks]


class SlackMessageComposer:
    """Builds Slack message with primary and secondary blocks"""

    def __init__(self, initial_message: t.Optional[TSlackMessage] = None) -> None:
        """Initialize the Slack message builder"""
        self.slack_message = initial_message or {"blocks": [], "attachments": [{"blocks": []}]}

    def add_primary_blocks(self, *blocks: TSlackBlock) -> "SlackMessageComposer":
        """Add blocks to the message. Blocks are always displayed"""
        self.slack_message["blocks"].extend(blocks)
        return self

    def add_secondary_blocks(self, *blocks: TSlackBlock) -> "SlackMessageComposer":
        """Add attachments to the message

        Attachments are hidden behind "show more" button. The first 5 attachments
        are always displayed. NOTICE: attachments blocks are deprecated by Slack
        """
        self.slack_message["attachments"][0]["blocks"].extend(blocks)
        if len(self.slack_message["attachments"][0]["blocks"]) >= SLACK_MAX_ATTACHMENTS_BLOCKS:
            raise ValueError("Too many attachments")
        return self

    def _introspect(self) -> "SlackMessageComposer":
        """Print the message to stdout

        This is a debugging method. Useful during composition of the message."""
        print(json.dumps(self.slack_message, indent=2))
        return self


def normalize_message(message: t.Union[str, t.List[str], t.Tuple[str], t.Set[str]]) -> str:
    """Normalize message to fit Slack's max text length"""
    if isinstance(message, (list, tuple, set)):
        message = "\n".join(message)
    dedented_message = dedent(message)
    if len(dedented_message) < SLACK_MAX_TEXT_LENGTH:
        return dedent(dedented_message)
    return dedent(
        dedented_message[: SLACK_MAX_TEXT_LENGTH - len(CONTINUATION_SYMBOL) - 3]
        + CONTINUATION_SYMBOL
        + dedented_message[-3:]
    )


def divider_block() -> dict:
    """Create a divider block"""
    return {"type": "divider"}


def fields_section_block(*messages: str) -> dict:
    """Create a section block with multiple markdown fields"""
    return {
        "type": "section",
        "fields": [
            {
                "type": "mrkdwn",
                "text": normalize_message(message),
            }
            for message in messages
        ],
    }


def text_section_block(message: str) -> dict:
    """Create a section block with text"""
    return {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": normalize_message(message),
        },
    }


def preformatted_rich_text_block(message: str) -> dict:
    """Create a "rich text" block with pre-formatted text (i.e.: a code block).

    Note that this can also be acheived with text_section_block and using markdown's
    triple backticks.
    This function avoids issues with the text containing such backticks and also does
    not require editing the message in order to insert such backticks.
    """
    return {
        "type": "rich_text",
        "elements": [
            {
                "type": "rich_text_preformatted",
                "elements": [
                    {
                        "type": "text",
                        "text": normalize_message(message),
                    },
                ],
            },
        ],
    }


def empty_section_block() -> dict:
    """Create an empty section block"""
    return {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": normalize_message("\t"),
        },
    }


def context_block(*messages: str) -> dict:
    """Create a context block with multiple fields"""
    return {
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": normalize_message(message),
            }
            for message in messages
        ],
    }


def header_block(message: str) -> dict:
    """Create a header block"""
    return {
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": message,
        },
    }


def button_action_block(text: str, url: str) -> dict:
    """Create a button action block"""
    return {
        "type": "actions",
        "elements": [
            {
                "type": "button",
                "text": {"type": "plain_text", "text": text, "emoji": True},
                "value": text,
                "url": url,
            }
        ],
    }


def compacted_sections_blocks(*messages: str) -> t.List[dict]:
    """Create a list of compacted sections blocks"""
    return [
        {
            "type": "section",
            "fields": [
                {
                    "type": "mrkdwn",
                    "text": normalize_message(message),
                }
                for message in messages[i : i + 2]
            ],
        }
        for i in range(0, len(messages), 2)
    ]


class SlackAlertIcon(str, Enum):
    """Enum for status of the alert"""

    # General statuses
    X = ":x:"
    OK = ":large_green_circle:"
    START = ":arrow_forward:"
    STOP = ":stop_button:"
    WARN = ":warning:"
    ALERT = ":rotating_light:"

    # Execution statuses
    FAILURE = ":rotating_light:"
    SUCCESS = ":white_check_mark:"
    WARNING = ":warning:"
    SKIPPED = ":fast_forward:"
    PASSED = ":white_check_mark:"

    # Log levels
    UNKNOWN = ":question:"
    INFO = ":information_source:"
    DEBUG = ":beetle:"
    CRITICAL = ":fire:"
    FATAL = ":skull_and_crossbones:"
    EXCEPTION = ":boom:"

    def __str__(self) -> str:
        return self.value


def stringify_list(list_variation: t.Union[t.List[str], str]) -> str:
    """Prettify and deduplicate list of strings"""
    if isinstance(list_variation, str):
        return list_variation
    if len(list_variation) == 1:
        return list_variation[0]
    return " ".join(list_variation)


message = SlackMessageComposer
