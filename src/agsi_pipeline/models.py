from __future__ import annotations


class AgsiPipelineError(Exception):
    """Base error for pipeline failures."""


class InvalidResponseError(AgsiPipelineError):
    """Raised when an API response fails validation."""


class DuplicateKeyError(AgsiPipelineError):
    """Raised when a snapshot produces duplicate country keys."""
