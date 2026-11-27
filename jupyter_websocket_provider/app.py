from pathlib import Path
from uuid import uuid4

from anyio import Event, create_task_group, sleep_forever
from jupyter_core.utils import ensure_async
from jupyter_server.extension.application import ExtensionApp
from jupyter_server.services.contents.filemanager import FileContentsManager
from watchfiles import Change, awatch

from .handlers import (
    CollaborationWebSocketHandler,
    DocumentSaveHandler,
    DocumentSessionHandler,
    delete_rooms,
    disk_lock,
    read_disk_hash,
    read_file_identity,
    rename_rooms,
    retire_changed_rooms,
)


class WebSocketProviderExtension(ExtensionApp):
    name = "jupyter_websocket_provider"
    app_name = "Jupyter WebSocket Provider"

    def initialize_handlers(self):
        self.rooms = {}
        options = {"rooms": self.rooms}
        self.tasks_stopped = Event()
        settings = self.serverapp.web_app.settings
        settings["websocket_provider_tasks_ready"] = Event()
        settings["websocket_provider_session"] = uuid4().hex
        self.handlers.extend([
            (r"/api/collaboration/session/(.+)", DocumentSessionHandler, options),
            (r"/api/collaboration/room/(.+)", CollaborationWebSocketHandler, options),
            (r"/api/collaboration/save/(.+)", DocumentSaveHandler, options),
        ])

    async def _start_jupyter_server_extension(self, serverapp):
        settings = serverapp.web_app.settings
        manager = serverapp.contents_manager
        logger = manager.event_logger
        logger.add_listener(
            schema_id=manager.event_schema_id, listener=self._on_contents_event, modified=False
        )
        try:
            async with create_task_group() as group:
                settings["websocket_provider_task_group"] = group
                settings["websocket_provider_tasks_ready"].set()
                if isinstance(manager, FileContentsManager):
                    group.start_soon(self._watch_local_changes, manager)
                await sleep_forever()
        finally:
            logger.remove_listener(schema_id=manager.event_schema_id, listener=self._on_contents_event)
            settings.pop("websocket_provider_task_group", None)
            self.tasks_stopped.set()

    async def _on_contents_event(self, logger, schema_id, data):
        if data["action"] == "delete":
            delete_rooms(self.rooms, data["path"])
        elif data["action"] == "rename" and data["source_path"] != data["path"]:
            await rename_rooms(
                self.rooms, data["source_path"], data["path"],
            )

    async def _watch_local_changes(self, manager: FileContentsManager):
        try:
            root = Path(manager.root_dir).resolve()
            async for changes in awatch(
                root, watch_filter=None, ignore_permission_denied=True, rust_timeout=1000
            ):
                changed = set()
                added_or_removed = set()
                added = set()
                for change, path in changes:
                    try:
                        relative_path = Path(path).relative_to(root).as_posix()
                    except ValueError:
                        continue
                    changed.add(relative_path)
                    if change in {Change.added, Change.deleted}:
                        added_or_removed.add(relative_path)
                    if change == Change.added:
                        added.add(relative_path)
                if not changed:
                    continue

                paths = {
                    room.path for room in self.rooms.values()
                    if room.path is not None
                    and (room.path in changed or any(
                        parent.as_posix() in added_or_removed for parent in Path(room.path).parents
                    ))
                }
                for path in paths:
                    try:
                        async with disk_lock(self.serverapp.web_app.settings, path):
                            if await ensure_async(manager.file_exists(path)):
                                current_hash = await read_disk_hash(manager, path)
                                retire_changed_rooms(self.rooms, path, current_hash)
                                identity = await read_file_identity(root, path)
                                for room in self.rooms.values():
                                    if room.path == path:
                                        room.file_identity = identity
                            else:
                                destination = await self._find_external_move(manager, root, path, added)
                                if destination is None:
                                    delete_rooms(self.rooms, path)
                                else:
                                    current_hash = await read_disk_hash(manager, destination)
                                    await rename_rooms(self.rooms, path, destination, external=True)
                                    retire_changed_rooms(self.rooms, destination, current_hash)
                    except Exception:
                        self.log.warning("Could not check disk changes for %r", path, exc_info=True)
        except Exception:
            self.log.warning("Could not watch the local contents directory", exc_info=True)

    async def _find_external_move(
        self, manager: FileContentsManager, root: Path, path: str, added: set[str]
    ) -> str | None:
        identities = {
            room.file_identity for room in self.rooms.values()
            if room.path == path and room.file_identity is not None
        }
        if len(identities) != 1:
            return None
        identity = next(iter(identities))
        destinations = []
        for candidate in added - {path}:
            if await read_file_identity(root, candidate) != identity:
                continue
            if not manager.allow_hidden and await ensure_async(manager.is_hidden(candidate)):
                continue
            if await ensure_async(manager.file_exists(candidate)):
                destinations.append(candidate)
        return destinations[0] if len(destinations) == 1 else None

    async def stop_extension(self):
        group = self.serverapp.web_app.settings.get("websocket_provider_task_group")
        if group is not None:
            group.cancel_scope.cancel()
            await self.tasks_stopped.wait()
