# Graceful Shutdown Coordinator for Agents

Coordinates graceful agent shutdown: drain signals, in-flight task completion deadlines, ordered resource teardown, and shutdown hooks.

**Stack:** Python 3.12 · stdlib only · unittest

## Problem statement

Killing agents mid-task loses work.  The coordinator runs a phased shutdown:

## Design decisions

- `drain` - stop accepting new work, signal in-flight tasks, 2.
- `await_completion` - wait up to a deadline for tasks to finish, 3.
- `teardown` - release resources in reverse-registration order, 4.
- `hooks` - run shutdown hooks (flush buffers, close connections).

## Usage example

```python
from src.shutdown_coordinator import ShutdownCoordinator

c = ShutdownCoordinator(completion_deadline=10.0)
c.add_resource("db-pool", lambda: print("db closed"))
task = c.submit("index-job")
c.task_done("index-job")
print(c.shutdown()["completed"])  # ['index-job']
```

## Layout

- `README.md`
- `src/shutdown_coordinator.py`
- `tests/test_shutdown_coordinator.py`
- `LICENSE`
- `.gitignore`

## Setup

No third-party dependencies — Python 3.11+ standard library only.

```bash
cd agentic-shutdown-coordinator
python -m unittest discover -s tests -v
```

## License

MIT — see [LICENSE](LICENSE).
