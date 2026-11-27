from __future__ import annotations

import json
from dataclasses import dataclass, field
from uuid import uuid4
from weakref import WeakValueDictionary

from anyio import CancelScope, Event, Lock, create_task_group, sleep
from anyio.abc import TaskGroup
from jupyter_core.utils import ensure_async
from jupyter_server.auth import authorized
from jupyter_server.auth.decorator import ws_authenticated
from jupyter_server.base.handlers import APIHandler, JupyterHandler
from jupyter_server.base.websocket import WebSocketMixin
from jupyter_server.services.contents.filemanager import FileContentsManager
from tornado.iostream import StreamClosedError
from tornado.web import HTTPError, authenticated
from tornado.websocket import WebSocketClosedError, WebSocketHandler


# Yjs sync step 2 with an empty update. The initializer already has the data.
EMPTY_SYNC_REPLY = b"\x00\x01\x02\x00\x00"
INITIALIZATION_TIMEOUT = 15
SESSION_RESERVATION_TIMEOUT = 60
DOCUMENT_FORMATS = (("json", "notebook"), ("text", "file"), ("base64", "file"))
GLOBAL_AWARENESS_ROOM = "JupyterLab:globalAwareness"
DOCUMENT_DELETED_CODE = 4409
DOCUMENT_CHANGED_CODE = 4410


def find_room_key(
    rooms: dict[tuple[str, ...], Room], document_format: tuple[str, ...], path: str
) -> tuple[str, ...] | None:
    return next(
        (key for key, room in rooms.items() if key[:2] == document_format and room.path == path),
        None,
    )


async def rename_rooms(rooms: dict[tuple[str, ...], Room], old_path: str, new_path: str):
    async with create_task_group() as tasks:
        for key, room in list(rooms.items()):
            if room.path != old_path:
                continue
            displaced_key = find_room_key(rooms, key[:2], new_path)
            if displaced_key is not None:
                displaced = rooms.pop(displaced_key)
                for client in displaced.clients:
                    client._close_stale_room()
            room.path = new_path
            for client in room.clients:
                tasks.start_soon(client._send_rename, new_path)


def delete_rooms(rooms: dict[tuple[str, ...], Room], path: str):
    prefix = f"{path}/"
    for key, room in list(rooms.items()):
        if room.path is None or (room.path != path and not room.path.startswith(prefix)):
            continue
        retire_room(rooms, key, DOCUMENT_DELETED_CODE, "This file was deleted.")


def retire_room(rooms: dict[tuple[str, ...], Room], key: tuple[str, ...], code: int, reason: str):
    room = rooms.pop(key)
    if room.initializer is not None:
        if code == DOCUMENT_DELETED_CODE:
            room.initializer._close_on_load_error(404)
        else:
            room.initializer._close_stale_room()
        room.initializer._finish_initialization()
    for client in tuple(room.clients):
        client.close(code, reason)


def disk_lock(settings: dict, path: str) -> Lock:
    locks = settings.setdefault("websocket_provider_disk_locks", WeakValueDictionary())
    lock = locks.get(path)
    if lock is None:
        lock = locks[path] = Lock()
    return lock


async def read_disk_hash(manager: FileContentsManager, path: str) -> str | None:
    model = await ensure_async(manager.get(path, content=False, require_hash=True))
    return model.get("hash")


def retire_changed_rooms(rooms: dict[tuple[str, ...], Room], path: str, disk_hash: str | None):
    if disk_hash is not None:
        for key, room in list(rooms.items()):
            if room.path == path and room.disk_hash is not None and room.disk_hash != disk_hash:
                retire_room(rooms, key, DOCUMENT_CHANGED_CODE, "This file changed on disk.")


async def get_task_group(handler: JupyterHandler) -> TaskGroup:
    await handler.settings["websocket_provider_tasks_ready"].wait()
    group = handler.settings.get("websocket_provider_task_group")
    if group is None or group.cancel_scope.cancel_called:
        raise HTTPError(503, "WebSocket provider is shutting down")
    return group


async def check_path(handler: JupyterHandler, path: str) -> str:
    path = path.strip("/")
    if any(part in {"", ".", ".."} for part in path.split("/")):
        raise HTTPError(400, "Expected a Jupyter Contents API file path")
    manager = handler.contents_manager
    if not manager.allow_hidden and await ensure_async(manager.is_hidden(path)):
        raise HTTPError(404)
    return path


class DocumentSessionHandler(APIHandler):
    auth_resource = "contents"

    def initialize(self, rooms: dict[tuple[str, ...], Room]):
        self.rooms = rooms

    @authenticated
    @authorized(action="read")
    async def put(self, path: str):
        path = await check_path(self, path)
        body = self.get_json_body()
        if not isinstance(body, dict):
            raise HTTPError(400, "Expected a document type and format")
        file_format, file_type = body.get("format"), body.get("type")
        if (file_format, file_type) not in DOCUMENT_FORMATS:
            raise HTTPError(400, "Unsupported document type or format")
        model = await ensure_async(
            self.contents_manager.get(path, content=False, type=file_type)
        )
        if model["type"] != file_type:
            raise HTTPError(400, "Unexpected document type")
        group = await get_task_group(self)
        key = find_room_key(self.rooms, (file_format, file_type), path)
        if key is None:
            key = (file_format, file_type, uuid4().hex)
            room = self.rooms[key] = Room(path=path)
            group.start_soon(self._expire_reservation, key, room)
        self.finish({
            "format": file_format,
            "type": file_type,
            "fileId": key[2],
            "sessionId": self.settings["websocket_provider_session"],
        })

    async def _expire_reservation(self, key: tuple[str, ...], room: Room):
        await sleep(SESSION_RESERVATION_TIMEOUT)
        if (
            self.rooms.get(key) is room
            and not room.clients
            and not room.joining
            and room.initialization_complete is None
        ):
            del self.rooms[key]


@dataclass
class Room:
    clients: set[CollaborationWebSocketHandler] = field(default_factory=set)
    join_lock: Lock = field(default_factory=Lock)
    joining: int = 0
    initialization_complete: Event | None = None
    initializer: CollaborationWebSocketHandler | None = None
    path: str | None = None
    disk_hash: str | None = None
    started: bool = False


class DocumentSaveHandler(APIHandler):
    auth_resource = "contents"

    def initialize(self, rooms: dict[tuple[str, ...], Room]):
        self.rooms = rooms

    @authenticated
    @authorized(action="write")
    async def put(self, room_id: str):
        key = tuple(room_id.split(":", 2))
        if len(key) != 3 or key[:2] not in DOCUMENT_FORMATS:
            raise HTTPError(400, "Invalid collaboration room ID")
        body = self.get_json_body()
        if not isinstance(body, dict) or not isinstance(body.get("path"), str) or "content" not in body:
            raise HTTPError(400, "Expected a document path and content")

        path = await check_path(self, body["path"])
        manager = self.contents_manager
        async with disk_lock(self.settings, path):
            self._check_room(key, path)
            exists = await ensure_async(manager.file_exists(path))
            self._check_room(key, path)
            if not exists:
                raise HTTPError(409, "The file no longer exists at this path.")

            if isinstance(manager, FileContentsManager):
                current_hash = await read_disk_hash(manager, path)
                retire_changed_rooms(self.rooms, path, current_hash)
                self._check_room(key, path)

            await ensure_async(manager.save({
                "format": key[0],
                "type": key[1],
                "content": body["content"],
            }, path))
            if isinstance(manager, FileContentsManager):
                try:
                    saved_hash = await read_disk_hash(manager, path)
                except Exception:
                    self.log.warning("Could not record the disk hash for %r", path, exc_info=True)
                else:
                    for room in self.rooms.values():
                        if room.path == path:
                            room.disk_hash = saved_hash
        self.set_status(204)
        self.finish()

    def _check_room(self, key: tuple[str, ...], path: str):
        room = self.rooms.get(key)
        if room is None or not room.started or not room.clients:
            raise HTTPError(409, "This document session ended. Reopen the document before saving.")
        if room.path != path:
            raise HTTPError(409, "The file has moved. Refresh the document path and retry the save.")


class CollaborationWebSocketHandler(WebSocketMixin, WebSocketHandler, JupyterHandler):
    auth_resource = "contents"

    def initialize(self, rooms: dict[tuple[str, ...], Room]):
        self.rooms = rooms
        self.room: Room | None = None
        self.closed = False
        self.initializing: tuple[Room, Event] | None = None
        self.initialization_timeout: CancelScope | None = None
        self.file_type = self.file_format = None
        self.room_key: tuple[str, ...] | None = None
        self.initial_path: str | None = None

    @ws_authenticated
    @authorized
    async def get(self, room_id: str):
        if room_id == GLOBAL_AWARENESS_ROOM:
            self.room_key = (room_id,)
            return await WebSocketHandler.get(self, room_id)
        parts = room_id.split(":", 2)
        if len(parts) != 3 or tuple(parts[:2]) not in DOCUMENT_FORMATS:
            raise HTTPError(400, "Invalid collaboration room ID")
        self.file_format, self.file_type, file_id = parts
        self.room_key = (self.file_format, self.file_type, file_id)
        room = self.rooms.get(self.room_key)
        if room is None:
            return await WebSocketHandler.get(self, room_id)
        path = await check_path(self, room.path)
        await WebSocketHandler.get(self, path)

    async def open(self, path: str):
        super().open(path)
        key = self.room_key
        if key is None:
            self._close_stale_room()
            return
        if key == (GLOBAL_AWARENESS_ROOM,):
            room = self.rooms.setdefault(key, Room())
        else:
            room = self.rooms.get(key)
            if room is None:
                self._close_stale_room()
                return
        room.joining += 1
        try:
            async with room.join_lock:
                while room.initialization_complete is not None and not self.closed:
                    await room.initialization_complete.wait()
                if self.closed:
                    return
                if self.rooms.get(key) is not room:
                    self._close_stale_room()
                    return
                if self.room_key == (GLOBAL_AWARENESS_ROOM,):
                    await self.write_message(EMPTY_SYNC_REPLY, binary=True)
                elif not room.clients:
                    self.initial_path = room.path
                    message = await self._initial_message(room, self.initial_path)
                    group = await get_task_group(self)
                    if self.closed:
                        return
                    if self.rooms.get(key) is not room:
                        self._close_stale_room()
                        return
                    initialized = room.initialization_complete = Event()
                    room.initializer = self
                    self.initializing = (room, initialized)
                    self.initialization_timeout = CancelScope()
                    group.start_soon(
                        self._on_initialization_timeout, self.initialization_timeout
                    )
                    await self.write_message(message)
                    return
                if not self.closed:
                    room.clients.add(self)
                    self.room = room
        except (WebSocketClosedError, StreamClosedError):
            self.close()
            self._finish_initialization()
        except HTTPError as error:
            self._close_on_load_error(error.status_code)
        except Exception:
            self.log.exception("Failed to initialize shared document %r", path)
            self._close_on_load_error(500)
            self._finish_initialization()
        finally:
            room.joining -= 1
            self._remove_empty_room(room)

    async def _initial_message(self, room: Room, path: str) -> str:
        manager = self.contents_manager
        async with disk_lock(self.settings, path):
            model = await ensure_async(
                manager.get(
                    path, content=True, type=self.file_type, format=self.file_format,
                    require_hash=isinstance(manager, FileContentsManager),
                )
            )
            room.disk_hash = model.get("hash")
        if (model["format"], model["type"]) not in DOCUMENT_FORMATS:
            raise HTTPError(400, "Unsupported document type")
        return json.dumps({
            "type": "initialize",
            "path": path,
            "fileType": model["type"],
            "format": model["format"],
            "content": model["content"],
        })

    def _finish_initialization(self):
        if self.initializing is None:
            return
        room, initialized = self.initializing
        self.initializing = None
        self._cancel_initialization_timeout()
        room.initialization_complete = None
        room.initializer = None
        initialized.set()
        self._remove_empty_room(room)

    async def _on_initialization_timeout(self, scope: CancelScope):
        with scope:
            await sleep(INITIALIZATION_TIMEOUT)
            self._close_on_load_error(500)
            self._finish_initialization()

    def _cancel_initialization_timeout(self):
        if self.initialization_timeout is not None:
            self.initialization_timeout.cancel()
            self.initialization_timeout = None

    def _close_stale_room(self):
        self.close(
            1003,
            json.dumps({
                "reason": "unknown_session",
                "reloadable": True,
                "errorReason": "This document session ended. Reopen to sync.",
            }, separators=(",", ":")),
        )

    async def on_message(self, message: str | bytes):
        if self.initializing is not None:
            if message == "initialized":
                self._cancel_initialization_timeout()
                room, _ = self.initializing
                if self.rooms.get(self.room_key) is not room:
                    self._close_stale_room()
                    self._finish_initialization()
                    return
                room.started = True
                room.clients.add(self)
                self.room = room
                try:
                    await self.write_message(EMPTY_SYNC_REPLY, binary=True)
                    if room.path != self.initial_path:
                        await self._send_rename(room.path)
                except (WebSocketClosedError, StreamClosedError):
                    self.close()
                    room.clients.discard(self)
                    self.room = None
                finally:
                    self._finish_initialization()
            return
        if (
            self.room is None
            or self.rooms.get(self.room_key) is not self.room
            or isinstance(message, str)
        ):
            return
        async with create_task_group() as tasks:
            for client in self.room.clients:
                if client is not self:
                    tasks.start_soon(client._relay, message)

    async def _relay(self, message: str | bytes):
        try:
            await self.write_message(message, binary=isinstance(message, bytes))
        except (WebSocketClosedError, StreamClosedError):
            self.close()

    async def _send_rename(self, path: str):
        await self._relay(json.dumps({"type": "rename", "path": path}))

    def on_close(self):
        self.closed = True
        self._finish_initialization()
        if self.ping_callback is not None:
            self.ping_callback.stop()
        if self.room is not None:
            self.room.clients.discard(self)
            self._remove_empty_room(self.room)
            self.room = None

    def _remove_empty_room(self, room: Room):
        key = self.room_key
        if key is None or self.rooms.get(key) is not room or room.clients:
            return
        if room.started or (not room.joining and room.initialization_complete is None):
            del self.rooms[key]

    @property
    def ping_interval(self):
        interval = super().ping_interval
        return min(interval, 15000) if interval > 0 else interval

    async def on_pong(self, data: bytes):
        super().on_pong(data)
        # y-websocket needs a data frame to keep an idle connection open.
        # An awareness message with zero entries leaves client state intact.
        if self.room is not None:
            await self._relay(b"\x01\x01\x00")

    def _close_on_load_error(self, status: int):
        code = {400: 4400, 404: 4404}.get(status, 4500)
        self.close(code, "Cannot load document")
