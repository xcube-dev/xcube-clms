class PreloadHandle:
    """
    Preload handle to let the user interactively view, cancel, close the
    progress of the preloading.
    """

    def _repr_html_(self):
        return

    def cancel(self):
        """
        Cancels the task
        """
