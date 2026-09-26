'''Tests for the shutdown coordinator.'''

import threading
import time
import unittest

from src.shutdown_coordinator import ShutdownCoordinator


class ShutdownCoordinatorTests(unittest.TestCase):
    def test_rejects_new_tasks_when_draining(self):
        c = ShutdownCoordinator()
        c.accepting = False
        with self.assertRaises(RuntimeError):
            c.submit("late")

    def test_clean_shutdown(self):
        c = ShutdownCoordinator()
        task = c.submit("t1")
        c.add_resource("db", lambda: None)
        order = []
        c.add_hook(lambda: order.append("hook"))
        threading.Timer(0.05, lambda: c.task_done("t1")).start()
        report = c.shutdown()
        self.assertEqual(report["completed"], ["t1"])

    def test_abandoned_on_deadline(self):
        c = ShutdownCoordinator(completion_deadline=0.05)
        c.submit("stuck")  # never completes
        report = c.shutdown()
        self.assertEqual(report["abandoned"], ["stuck"])

    def test_teardown_reverse_order(self):
        c = ShutdownCoordinator()
        order = []
        c.add_resource("first", lambda: order.append("first"))
        c.add_resource("second", lambda: order.append("second"))
        report = c.shutdown()
        self.assertEqual(order, ["second", "first"])

    def test_teardown_error_continues(self):
        c = ShutdownCoordinator()

        def bad():
            raise RuntimeError("release failed")

        c.add_resource("bad", bad)
        c.add_resource("good", lambda: None)
        report = c.shutdown()
        self.assertEqual(report["torn_down"], ["good"])
        self.assertEqual(len(report["errors"]), 1)

    def test_hook_error_reported(self):
        c = ShutdownCoordinator()

        def bad_hook():
            raise ValueError("hook boom")

        c.add_hook(bad_hook)
        report = c.shutdown()
        self.assertEqual(report["hooks_run"], 0)
        self.assertEqual(len(report["errors"]), 1)

    def test_drain_signals_tasks(self):
        c = ShutdownCoordinator()
        task = c.submit("t1")
        seen = []
        threading.Timer(0.02, lambda: seen.append(task["drain"].is_set())).start()
        threading.Timer(0.04, lambda: c.task_done("t1")).start()
        c.shutdown()
        self.assertEqual(seen, [True])

    def test_empty_shutdown(self):
        report = ShutdownCoordinator().shutdown()
        self.assertEqual(report["completed"], [])

from src.shutdown_coordinator import ShutdownCoordinator


class ShutdownCoordinatorExtraTests(unittest.TestCase):
    def test_pending_and_task_state(self):
        c = ShutdownCoordinator()
        c.submit("a")
        c.submit("b")
        c.task_done("a")
        self.assertEqual(c.pending_tasks(), ["b"])
        self.assertEqual(c.task_state("a"),
                         {"done": True, "drain_signaled": False})
        self.assertFalse(c.is_draining())

    def test_duplicate_submit_rejected(self):
        c = ShutdownCoordinator()
        c.submit("a")
        with self.assertRaises(ValueError):
            c.submit("a")



if __name__ == "__main__":
    unittest.main()
