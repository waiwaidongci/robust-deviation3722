from enum import StrEnum
class RunStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
NON_TERMINAL_STATUSES = (RunStatus.QUEUED, RunStatus.RUNNING)
