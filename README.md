# Jupyter WebSocket Provider

A Jupyter Server extension that sends the initial file contents to the browser
and relays Yjs messages between clients over WebSocket. The browser creates the
shared document.

## Install from source

You need to have `python` and `npm` installed, then enter in a virtual environment:

```shell
python -m pip install 'jupyter-builder>=1.2' build
npm install --no-package-lock
npm run build
python -m build
```

Installation enables the server extension and installs a prebuilt JupyterLab
extension. The JupyterLab extension uses `jupyter-docprovider` to open shared
documents through `/api/collaboration/session/<path>` and
`/api/collaboration/room/<room-id>`.
Run this extension in place of `jupyter-server-ydoc`, which serves the same routes.

JupyterLab's session endpoint issues an opaque UUID as the file ID for each room
lifetime. Clients opening the same path, type, and format share that ID while
the room is active.

- The first connection to an empty room receives a JSON text frame with
  `type: "initialize"`, `path`, `fileType`, `format`, and `content`. The content
  is notebook JSON, text, or a base64 string, matching the Jupyter Contents API.
  Hold the content and send the text frame `initialized`. The server confirms
  with an empty Yjs sync step 2 and admits waiting clients. Apply the content
  before processing that sync frame. If the socket closes before confirmation,
  discard the pending content so another client can initialize.
- While the room has clients, the server forwards Yjs messages to
  every other connection in the same room. A joining client sends its Yjs sync
  step 1, and connected clients answer with their current state. Clients also
  handle awareness messages.
- JupyterLab sends the current contents and path to
  `/api/collaboration/save/<room-id>`. The server writes a full snapshot through
  the Contents Manager and returns HTTP 409 if the room has expired or the
  requested path no longer matches. If several clients save concurrently, the
  last write determines the disk contents.
- Deleting a file through Jupyter Server retires its room and shows an
  external-change notice in connected tabs. Use **Save to original file** to
  restore it, **Save As…** to choose a destination, or **Close tab** to close
  the deleted document. Saves from the retired session are rejected. With a
  local file Contents Manager, deletions made from the terminal trigger the
  same notice. When the watcher can identify the same file at a new path
  inside the Jupyter root, the room follows the move and keeps its file ID.
- With a local file Contents Manager, the server also compares disk content
  hashes on filesystem changes and before collaborative saves. An external
  edit retires the room if the hash differs. Use **Load from disk** to open
  a fresh room, **Save to original file** to replace the disk contents with
  your edits, or **Save As…** to keep your edits separately.
- When the last client leaves an initialized JupyterLab room, its file ID
  expires. The next open gets a new ID and starts from disk. A JupyterLab tab
  reconnecting with the expired ID gets a session-expired dialog and must
  reopen the document.

The relay also provides the global awareness room for collaboration presence.
To rebuild the prebuilt frontend after editing its source, install the Python
`jupyter-builder` package, run `npm install && npm run build`, then reinstall
the Python package.

## Tests

With pip 25.1 or newer, install the test dependencies and run pytest from the
repository root.

```sh
python -m pip install --group test -e .
python -m pytest tests
```

The integration tests use temporary files and localhost WebSocket connections.
