from importlib.metadata import version

from .app import WebSocketProviderExtension


__version__ = version(__package__)


def _jupyter_labextension_paths():
    return [{"src": "labextension", "dest": "jupyter-websocket-provider"}]


def _jupyter_server_extension_points():
    return [{"module": "jupyter_websocket_provider", "app": WebSocketProviderExtension}]
