'''Graceful shutdown coordinator for agents.

Killing agents mid-task loses work.  The coordinator runs a phased
shutdown:

1. ``drain`` - stop accepting new work, signal in-flight tasks,
2. ``await_completion`` - wait up to a deadline for tasks to finish,
3. ``teardown`` - release resources in reverse-registration order,
4. ``hooks`` - run shutdown hooks (flush buffers, close connections).

``shutdown`` runs all phases and returns a report; tasks still running
past the deadline are listed as ``abandoned``.  ``pending_tasks`` and
``task_state`` inspect in-flight work before shutting down.
'''

from __future__ import annotations

import threading
import time


class ShutdownCoordinator:
    '''Phased graceful shutdown with deadlines and hooks.'''

    def __init__(self, completion_deadline=30.0):
        self.completion_deadline = completion_deadline
        self.accepting = True
        self.tasks = {}    # name -> {"done": Event, "drain": Event}
        self.resources = []  # (name, release_fn) in registration order
        self.hooks = []
        self.drained = False

    def submit(self, name):
        '''Register an in-flight task; fails once draining.'''
        if not self.accepting:
            raise RuntimeError("coordinator is draining: no new tasks")
        if name in self.tasks:
            raise ValueError(f"task already registered: {name}")
        self.tasks[name] = {"done": threading.Event(),
                            "drain": threading.Event()}
        return self.tasks[name]

    def task_done(self, name):
        self.tasks[name]["done"].set()

    def task_state(self, name):
        '''{"done": bool, "drain_signaled": bool} for a task.'''
        task = self.tasks[name]
        return {"done": task["done"].is_set(),
                "drain_signaled": task["drain"].is_set()}

    def pending_tasks(self):
        '''Names of tasks not yet marked done.'''
        return [n for n, t in self.tasks.items() if not t["done"].is_set()]

    def is_draining(self):
        return self.drained

    def cancel_task(self, name):
        '''Forget a task record; returns True when it existed.'''
        return self.tasks.pop(name, None) is not None

    def add_resource(self, name, release_fn):
        self.resources.append((name, release_fn))

    def resource_names(self):
        return [name for name, _ in self.resources]

    def drain_signal(self, name):
        '''Ask one task to wind down; returns True when found.'''
        task = self.tasks.get(name)
        if task is None:
            return False
        task["drain"].set()
        return True

    def task_count(self):
        '''Number of registered tasks.'''
        return len(self.tasks)

    def add_hook(self, fn):

        self.hooks.append(fn)

    def shutdown(self):
        '''Run drain -> await -> teardown -> hooks; returns a report.'''
        report = {"drained": [], "completed": [], "abandoned": [],
                  "torn_down": [], "hooks_run": 0, "errors": []}
        # phase 1: drain
        self.accepting = False
        for name, task in self.tasks.items():
            task["drain"].set()
            report["drained"].append(name)
        self.drained = True
        # phase 2: await completion up to the deadline
        deadline = time.monotonic() + self.completion_deadline
        for name, task in self.tasks.items():
            remaining = max(0.0, deadline - time.monotonic())
            if task["done"].wait(timeout=remaining):
                report["completed"].append(name)
            else:
                report["abandoned"].append(name)
        # phase 3: teardown in reverse order
        for name, release_fn in reversed(self.resources):
            try:
                release_fn()
                report["torn_down"].append(name)
            except Exception as exc:  # noqa: BLE001 - keep tearing down
                report["errors"].append(f"{name}: {exc}")
        # phase 4: hooks
        for hook in self.hooks:
            try:
                hook()
                report["hooks_run"] += 1
            except Exception as exc:  # noqa: BLE001 - report and continue
                report["errors"].append(f"hook: {exc}")
        return report

    def reset(self):
        '''Re-arm the coordinator for reuse.'''
        self.accepting = True
        self.drained = False
        self.tasks = {}
        self.resources = []
        self.hooks = []
