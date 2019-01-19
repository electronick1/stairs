

class SignalsMixin:
    signals_on_app_created = []

    def on_app_created(self):
        def _wrap_handler(handler):
            self.signals_on_app_created.append(handler)

        return _wrap_handler

    def send_signal_on_app_created(self):
        for handler in self.signals_on_app_created:
            handler(self)
