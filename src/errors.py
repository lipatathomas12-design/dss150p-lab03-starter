from __future__ import annotations

import logging

log = logging.getLogger("pipeline")


class PipelineStageError(Exception):
    def __init__(self, stage: str, message: str, context: dict | None = None):
        self.stage = stage
        self.context = context or {}
        super().__init__(f"[stage={stage}] {message} | context={self.context}")


class DataQualityError(Exception):
    """Raised by validate when a data contract assertion fails."""


def run_stage(stage: str, func, context: dict | None = None):
    log.info("stage=%s start context=%s", stage, context or {})
    try:
        result = func()
    except PipelineStageError:
        raise
    except Exception as exc:  # noqa: BLE001 - re-raised with stage context, never swallowed
        log.exception("stage=%s failed context=%s", stage, context or {})
        raise PipelineStageError(stage, f"{type(exc).__name__}: {exc}", context) from exc
    log.info("stage=%s done", stage)
    return result