class PreloadHandle:
    """
    Preload handle to let the user interactively view, cancel, close the
    progress of the preloading.
    """

    def cancel(self):
        """
        Cancels the task
        """


# def spinner(self, status_event, task_id, cancel_event):
#        """
#        Displays a spinner with elapsed time for a single task until the event is set.
#        """
#        spinner = cycle(["◐", "◓", "◑", "◒"])
#        start_time = time.time()
#
#        while status_event.is_set():
#            elapsed = int(time.time() - start_time)
#            print(
#                f"\rTask {task_id}: {next(spinner)} Elapsed time: {elapsed}s",
#                end="",
#                flush=True,
#            )
#            time.sleep(0.3)
#        if cancel_event.is_set():
#            print(f"\rTask {task_id}: Task cancellation requested!{' ' * 20}")
#            headers = ACCEPT_HEADER.copy()
#            headers.update(get_authorization_header(self._api_token))
#
#            url = build_api_url(self._url, CANCEL_ENDPOINT, datasets_request=False)
#            json = {"TaskID": task_id}
#            response_data = make_api_request(
#                url=url, headers=headers, json=json, method="DELETE"
#            )
#            response = get_response_of_type(response_data, JSON_TYPE)
#
#            if not response.ok:
#                LOG.warn(f"Cancel request not successful. {response.content}")
#
#        else:
#            print(f"\rTask {task_id}: Done!{' ' * 20}")
