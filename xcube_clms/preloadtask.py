import time
from itertools import cycle

from IPython.core.display_functions import display
from ipywidgets import widgets

from xcube_clms.constants import ACCEPT_HEADER, CANCEL_ENDPOINT, JSON_TYPE, LOG
from xcube_clms.event import Event
from xcube_clms.utils import (
    get_authorization_header,
    build_api_url,
    make_api_request,
    get_response_of_type,
)


class PreloadTask:
    """
    Creates a task. The task can be cancelled. It provides a progress method
    to check on the progress of the task.
    """

    def __init__(self, task_id, data_id, url, api_token):
        self.html_output = widgets.HTML()
        self.task_id = task_id
        self.data_id = data_id
        self.url = url
        self.api_token = api_token
        self._queue_status = "Not started"
        self._download_status = "Not started"
        self.events = PreloadTask.get_events()

    @staticmethod
    def get_events():
        return {
            # For cancelling this task. If set, the task will be cancelled.
            "cancel_event": Event(),
            # For checking if the task is still in queue. If set, it means
            # that the task is in queue. If it is cleared, it is not in queue.
            "queue_event": Event(),
            # For checking if the downloaded has completed. If set,
            # the download is in progress.
            "download_event": Event(),
            # For checking if the downloaded has been unzipped and extracted
            # to a folder. If set, the extraction is in progress. If cleared,
            # the extraction is complete.
            "extraction_event": Event(),
        }

    # Currently this method will not work as expected because the CLMS API's
    # Delete endpoint does not work as expected. I have created an issue with
    # them, waiting for further communication from them.
    def cancel(self):
        self.events.get("cancel_event").set()
        headers = ACCEPT_HEADER.copy()
        headers.update(get_authorization_header(self.api_token))

        url = build_api_url(self.url, CANCEL_ENDPOINT, datasets_request=False)
        json = {"TaskID": self.task_id}
        response_data = make_api_request(
            url=url, headers=headers, json=json, method="DELETE"
        )
        response = get_response_of_type(response_data, JSON_TYPE)

        if not response.ok:
            LOG.warn(f"Cancel request not successful. {response.content}")
        LOG.warn(f"Cancel request successful. {response.content}")

    def spinner(self, queue_event, task_id, cancel_event):
        """
        Displays a spinner with elapsed time for a single task until the event is set.
        """
        spinner = cycle(["◐", "◓", "◑", "◒"])
        start_time = time.time()

        while queue_event.is_set():
            elapsed = int(time.time() - start_time)
            print(
                f"\rTask {task_id}: {next(spinner)} Elapsed time: {elapsed}s",
                end="",
                flush=True,
            )
            time.sleep(0.3)
        if cancel_event.is_set():
            print(f"\rTask {task_id}: Task cancellation requested!{' ' * 20}")
            self.cancel()

        else:
            print(f"\rTask {task_id}: Done!{' ' * 20}")

    def display(self):
        display(self.html_output)

    @property
    def queue_status(self):
        return self._queue_status

    @queue_status.setter
    def queue_status(self, status):
        self._queue_status = status
        self.update_html()

    @property
    def download_status(self):
        return self._download_status

    @download_status.setter
    def download_status(self, status):
        self._download_status = status
        self.update_html()

    def update_html(self):
        status_colors = {
            "Success": "background-color: green; color: white;",
            "Failed": "background-color: red; color: white;",
            "Cancelled": "background-color: yellow; color: black;",
            "Pending": "background-color: lightorange; color: black;",
            "Not started": "background-color: lightgray; color: black;",
        }

        queue_status_style = status_colors.get(
            self.queue_status, "background-color: lightgray; color: black;"
        )
        download_status_style = status_colors.get(
            self.download_status, "background-color: lightgray; color: " "black;"
        )

        self.html_output = f"""
                <tr style="border-bottom: 1px solid #ddd;">
                    <td>{self.data_id}</td>
                    <td>{self.task_id}</td>
                    <td style="{queue_status_style}">{self.queue_status}</td>
                    <td style="{download_status_style}"
                    >{self.download_status}</td>
                    <td>
                        <button style="
                        padding: 5px 10px; 
                        background-color: #ff4d4d; 
                        color: white; 
                        border: none; 
                        border-radius: 5px; 
                        cursor: pointer;" 
                        onclick="cancel_task('{self.cancel}')">Cancel</button>
                    </td>
                </tr>
                """
