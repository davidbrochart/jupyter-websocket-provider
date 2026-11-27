import json
from inspect import unwrap
from types import SimpleNamespace
from urllib.parse import quote
from uuid import UUID

import pytest
from anyio import CancelScope, Event, create_task_group, fail_after, sleep
from jupyter_server.services.contents.filemanager import AsyncFileContentsManager
from tornado.httpclient import AsyncHTTPClient
from tornado.httpserver import HTTPServer
from tornado.netutil import bind_sockets
from tornado.web import Application
from tornado.websocket import websocket_connect
from watchfiles import awatch

from jupyter_websocket_provider import WebSocketProviderExtension
from jupyter_websocket_provider.handlers import (
    DOCUMENT_CHANGED_CODE,
    DOCUMENT_DELETED_CODE,
    EMPTY_SYNC_REPLY,
    CollaborationWebSocketHandler,
    DocumentSaveHandler,
    DocumentSessionHandler,
)


class LocalRequest:
    """Skip Jupyter's authentication setup in the localhost test app."""

    def prepare(self):
        pass


class Session(LocalRequest, DocumentSessionHandler):
    put = unwrap(DocumentSessionHandler.put)


class Save(LocalRequest, DocumentSaveHandler):
    put = unwrap(DocumentSaveHandler.put)


class Socket(LocalRequest, CollaborationWebSocketHandler):
    get = unwrap(CollaborationWebSocketHandler.get)


class CollaborationServer:
    path = "folder/a b.txt"

    def __init__(self, root):
        self.root = root
        self.file = self.root / self.path
        self.file.parent.mkdir()
        self.file.write_text("hello", encoding="utf-8")
        self.sockets = []
        self.watcher_ready = Event()
        self.watcher_batch_processed = Event()

        manager = AsyncFileContentsManager(root_dir=str(self.root))
        app = Application(contents_manager=manager, ws_ping_interval=0)
        self.serverapp = SimpleNamespace(web_app=app, contents_manager=manager)
        self.extension = WebSocketProviderExtension(serverapp=self.serverapp)
        self.extension.initialize_handlers()

        handlers = {
            DocumentSessionHandler: Session,
            DocumentSaveHandler: Save,
            CollaborationWebSocketHandler: Socket,
        }
        app.add_handlers(r".*", [
            (route, handlers[handler], options)
            for route, handler, options in self.extension.handlers
        ])
        self.http_server = HTTPServer(app)
        sockets = bind_sockets(0, address="127.0.0.1")
        self.http_server.add_sockets(sockets)
        self.base_url = f"http://127.0.0.1:{sockets[0].getsockname()[1]}"
        self.http_client = AsyncHTTPClient(force_instance=True)

    async def watch(self, root, **options):
        options.update(yield_on_timeout=True, rust_timeout=50)
        async for changes in awatch(root, **options):
            self.watcher_ready.set()
            if changes:
                yield changes
                self.watcher_batch_processed.set()

    async def close(self):
        for socket in self.sockets:
            socket.close()
        self.http_client.close()
        self.http_server.stop()
        await self.extension.stop_extension()
        await self.http_server.close_all_connections()

    async def session(self, path=None):
        response = await self.http_client.fetch(
            f"{self.base_url}/api/collaboration/session/{quote(path or self.path, safe='')}",
            method="PUT",
            body=json.dumps({"type": "file", "format": "text"}),
        )
        return json.loads(response.body)["fileId"]

    async def connect(self, file_id):
        url = f"{self.base_url}/api/collaboration/room/text:file:{file_id}"
        socket = await websocket_connect(url.replace("http://", "ws://", 1))
        self.sockets.append(socket)
        return socket

    async def initialize_room(self):
        file_id = await self.session()
        socket = await self.connect(file_id)
        message = json.loads(await socket.read_message())
        assert message["type"] == "initialize"
        assert message["content"] == "hello"
        await self.acknowledge_initialization(socket)
        return file_id, socket

    async def acknowledge_initialization(self, socket):
        await socket.write_message("initialized")
        assert await socket.read_message() == EMPTY_SYNC_REPLY

    async def save(self, file_id, path=None):
        room = quote(f"text:file:{file_id}", safe="")
        return await self.http_client.fetch(
            f"{self.base_url}/api/collaboration/save/{room}",
            method="PUT",
            body=json.dumps({"path": path or self.path, "content": "edited"}),
            raise_error=False,
        )


pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
async def collaboration(tmp_path, monkeypatch):
    server = CollaborationServer(tmp_path)
    monkeypatch.setattr("jupyter_websocket_provider.app.awatch", server.watch)
    async with create_task_group() as tasks:
        tasks.start_soon(server.extension._start_jupyter_server_extension, server.serverapp)
        try:
            with fail_after(10):
                yield server
        finally:
            with CancelScope(shield=True):
                await server.close()


async def test_room_id_expires_after_last_client_leaves(collaboration):
    file_id, first = await collaboration.initialize_room()
    assert UUID(file_id).hex == file_id
    assert await collaboration.session() == file_id
    second = await collaboration.connect(file_id)
    await second.write_message(b"peer sync", binary=True)
    assert await first.read_message() == b"peer sync"
    first.close()
    assert await collaboration.session() == file_id
    second.close()
    while collaboration.extension.rooms:
        await sleep(0.01)

    assert await collaboration.session() != file_id
    stale = await collaboration.connect(file_id)
    assert await stale.read_message() is None
    assert stale.close_code == 1003
    assert (await collaboration.save(file_id)).code == 409


async def test_waiting_client_takes_over_after_initializer_timeout(collaboration, monkeypatch):
    monkeypatch.setattr("jupyter_websocket_provider.handlers.INITIALIZATION_TIMEOUT", 0.5)
    file_id = await collaboration.session()
    first = await collaboration.connect(file_id)
    assert json.loads(await first.read_message())["type"] == "initialize"
    second = await collaboration.connect(file_id)
    assert await first.read_message() is None
    assert first.close_code == 4500
    assert json.loads(await second.read_message())["type"] == "initialize"
    await collaboration.acknowledge_initialization(second)
    assert await collaboration.session() == file_id


async def test_terminal_rename_keeps_room_and_updates_both_clients(collaboration):
    file_id, first = await collaboration.initialize_room()
    second = await collaboration.connect(file_id)
    await second.write_message(b"peer sync", binary=True)
    assert await first.read_message() == b"peer sync"
    await collaboration.watcher_ready.wait()
    destination = collaboration.root / "renamed.txt"
    collaboration.file.rename(destination)
    notices = [json.loads(await peer.read_message()) for peer in (first, second)]

    for notice in notices:
        assert (notice["type"], notice["path"]) == ("rename", "renamed.txt")
    assert sum(notice.get("publish", False) for notice in notices) == 1
    assert await collaboration.session("renamed.txt") == file_id
    assert (await collaboration.save(file_id)).code == 409
    assert (await collaboration.save(file_id, "renamed.txt")).code == 204
    assert destination.read_text(encoding="utf-8") == "edited"
    assert not collaboration.file.exists()


async def test_disk_content_change_expires_id_but_collaborative_save_keeps_it(collaboration):
    file_id, socket = await collaboration.initialize_room()
    await collaboration.watcher_ready.wait()
    assert (await collaboration.save(file_id)).code == 204
    await collaboration.watcher_batch_processed.wait()
    assert await collaboration.session() == file_id
    assert collaboration.file.read_text(encoding="utf-8") == "edited"

    replacement = collaboration.file.with_suffix(".tmp")
    replacement.write_text("external edit", encoding="utf-8")
    replacement.replace(collaboration.file)
    assert await socket.read_message() is None
    assert socket.close_code == DOCUMENT_CHANGED_CODE
    assert (await collaboration.save(file_id)).code == 409
    fresh_id = await collaboration.session()
    assert fresh_id != file_id
    fresh = await collaboration.connect(fresh_id)
    assert json.loads(await fresh.read_message())["content"] == "external edit"


async def test_terminal_delete_retires_room_and_blocks_old_saves(collaboration):
    file_id, socket = await collaboration.initialize_room()
    await collaboration.watcher_ready.wait()
    collaboration.file.unlink()
    assert await socket.read_message() is None
    assert socket.close_code == DOCUMENT_DELETED_CODE
    assert (await collaboration.save(file_id)).code == 409
    assert not collaboration.file.exists()

    collaboration.file.write_text("replacement", encoding="utf-8")
    assert await collaboration.session() != file_id
    assert (await collaboration.save(file_id)).code == 409
    assert collaboration.file.read_text(encoding="utf-8") == "replacement"
