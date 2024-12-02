from IPython.core.display_functions import display
from ipywidgets import widgets

from xcube_clms.constants import (
    LOG,
    FINISHED,
    QUEUE_WEIGHT,
    DOWNLOAD_WEIGHT,
    EXTRACTION_WEIGHT,
)
from xcube_clms.preloadtask import PreloadTask


class PreloadHandle:
    """
    Preload handle to let the user interactively view, cancel, close the
    progress of the preloading.
    """

    def __init__(self, tasks: list[PreloadTask]):
        self.tasks = tasks
        self.html_output = widgets.HTML("<p>Loading tasks...</p>")

    def update_html(self):
        task_html = "".join([task.html_output for task in self.tasks])

        self.html_output.value = f"""
                <div style="font-family: Arial, sans-serif; padding: 10px; max-width: 600px; margin: 0 auto;">
                    <h2>Task Manager</h2>
                    <table style="width: 100%; border-collapse: collapse;">
                        <thead style="background-color: #f4f4f4;">
                            <tr>
                                <th>Task Name</th>
                                <th>Queue Status</th>
                                <th>Download Status</th>
                                <th>Actions</th>
                            </tr>
                        </thead>
                        <tbody>
                            {task_html}
                        </tbody>
                    </table>
                </div>
                """

    def display(self):
        display(self.html_output)

    def cancel(self, task_id):
        """
        Cancels the task
        """
        requested_task = self._find_requested_task(task_id)
        if requested_task is None:
            LOG.warn(f"No task found in Task Manager for task_id: {task_id}")
            return
        requested_task.cancel()

    def cancel_all(self):
        for task in self.tasks:
            task.cancel()

    def _find_requested_task(self, task_id):
        requested_task = None
        for task in self.tasks:
            if task.task_id == task_id:
                requested_task = task
        return requested_task

    def is_cancelled(self, task_id):
        requested_task = self._find_requested_task(task_id)
        cancel_event = requested_task.get_events().get("cancel_event")
        if cancel_event.is_set():
            return True
        return False

    def progress(self, task_id):
        requested_task = self._find_requested_task(task_id)
        events = requested_task.get_events()
        total_progress = 0
        if events.get("queue_event").get_state() == FINISHED:
            total_progress += QUEUE_WEIGHT
        if events.get("download_event").get_state() == FINISHED:
            total_progress += DOWNLOAD_WEIGHT
        if events.get("extraction_event").get_state() == FINISHED:
            total_progress += EXTRACTION_WEIGHT
        return total_progress
