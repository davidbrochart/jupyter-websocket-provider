const {
  ExternalChangeHandler,
  ExternalChangeNoticePresenter,
  IDocumentProviderFactory,
  WebSocketProvider
} = require('@jupyter/docprovider');
const { PathExt } = require('@jupyterlab/coreutils');
const { IDocumentManager } = require('@jupyterlab/docmanager');
const { ServerConnection } = require('@jupyterlab/services');

const EMPTY_SYNC_REPLY = [0, 1, 2, 0, 0];
const DOCUMENT_DELETED_CODE = 4409;
const DOCUMENT_CHANGED_CODE = 4410;
const DOCUMENT_DELETED_MESSAGE = 'This file was deleted. Use Save As to recover your changes.';
const DOCUMENT_CHANGED_MESSAGE = 'This file changed on disk. Use the notice to load it or choose where to save your changes.';

function withDocumentInitialization(model, Socket, onExpired, onRename, onExternalChange) {
  let hasDocument = false;
  let roomId;

  class InitializingSocket extends Socket {
    constructor(url, protocols) {
      super(url, protocols);
      roomId ??= decodeURIComponent(new URL(url).pathname.split('/').pop());
      let pendingInitialization;
      this.addEventListener('close', event => {
        pendingInitialization = undefined;
        if (event.code === 1003) {
          // Let docprovider show its expiration dialog before stopping retries.
          queueMicrotask(onExpired);
        } else if (event.code === DOCUMENT_DELETED_CODE) {
          onExternalChange(true);
        } else if (event.code === DOCUMENT_CHANGED_CODE) {
          onExternalChange(false);
        }
      });
      // Apply the source before y-websocket processes the confirmation.
      this.addEventListener('message', event => {
        try {
          if (typeof event.data === 'string') {
            event.stopImmediatePropagation();
            const message = JSON.parse(event.data);
            if (message.type === 'initialize') {
              pendingInitialization = message;
              this.send('initialized');
            } else if (message.type === 'rename') {
              onRename(message.path);
            }
            return;
          }
          if (!pendingInitialization) {
            return;
          }
          const reply = new Uint8Array(event.data);
          if (reply.length !== EMPTY_SYNC_REPLY.length ||
              !EMPTY_SYNC_REPLY.every((byte, index) => reply[index] === byte)) {
            return;
          }
          const { path, fileType, content } = pendingInitialization;
          pendingInitialization = undefined;
          if (!hasDocument) {
            model.transact(() => {
              model.ydoc.getMap('state').set('path', path);
              // JupyterLab leaves the first cell of a collaborative notebook
              // to the initializer so peers don't each create their own.
              const source = fileType === 'notebook' && content.cells.length === 0
                ? {
                    ...content,
                    cells: [{
                      cell_type: 'code',
                      source: '',
                      metadata: { trusted: true },
                      execution_count: null,
                      outputs: []
                    }]
                  }
                : content;
              model.setSource(source);
            }, false);
            hasDocument = true;
          }
        } catch (error) {
          event.stopImmediatePropagation();
          console.error('Failed to initialize the shared document', error);
          this.close(4500, 'Cannot load document');
        }
      });
    }
  }

  return {
    Socket: InitializingSocket,
    getRoomId: () => roomId,
    markSynced: () => {
      hasDocument = true;
    }
  };
}

class ContentsSavingProvider extends WebSocketProvider {
  constructor(options, shell, docManager) {
    const serverSettings = options.serverSettings ?? ServerConnection.makeSettings();
    const { Socket, getRoomId, markSynced } = withDocumentInitialization(
      options.model,
      serverSettings.WebSocket,
      () => this.dispose(),
      path => this.onRoomRenamed(path),
      deleted => this.onExternalChange(deleted)
    );
    super({
      ...options,
      serverSettings: { ...serverSettings, WebSocket: Socket }
    });
    this.ready.then(markSynced, () => {});
    this.options = options;
    this.getRoomId = getRoomId;
    this.serverSettings = serverSettings;
    this.savePath = options.path;
    this.pendingSave = Promise.resolve();
    this.externalChangeStatus = null;
    this.state = options.model.ydoc.getMap('state');
    this.onSharedPathChanged = event => {
      if (event.keysChanged.has('path')) {
        const path = this.state.get('path');
        if (typeof path === 'string') {
          this.savePath = path;
        }
      }
    };
    this.state.observe(this.onSharedPathChanged);
    options.drive.fileChanged.connect(this.onFileChanged, this);
    const views = () => Array.from(shell.widgets('main')).filter(view =>
      docManager.contextForWidget(view)?.model.sharedModel === options.model
    );
    this.externalChange = new ExternalChangeHandler({
      ready: this.ready,
      translator: options.translator,
      contents: options.drive,
      actions: {
        saveAs: (path, originalPath) => this.saveExternalChangeAs(path, originalPath),
        overwrite: path => this.saveSnapshot(path)
      },
      showNotice: (notice, beforeSave, createNotice) =>
        ExternalChangeNoticePresenter.forDocument({
          notice, beforeSave, createNotice, shell, docManager, model: options.model
        }),
      onCloseDocument: () => {
        const openViews = views();
        const view = openViews.find(widget => widget === shell.currentWidget) ?? openViews[0];
        view?.dispose();
      },
      onSwitchDocument: async path => {
        const openViews = views();
        if (PathExt.normalize(path) === PathExt.normalize(this.savePath)) {
          // Release old contexts so the document manager opens a fresh model.
          const contexts = new Set(openViews.map(view => docManager.contextForWidget(view)));
          contexts.forEach(context => context?.dispose());
          openViews.forEach(view => view.dispose());
          docManager.open(path);
        } else if (docManager.open(path)) {
          openViews.forEach(view => view.dispose());
        }
      }
    });
  }

  onFileChanged(_sender, change) {
    const oldPath = change.oldValue?.path;
    const newPath = change.newValue?.path;
    if (change.type !== 'rename' || !oldPath || !newPath) {
      return;
    }
    const sharedPath = this.state.get('path');
    // A rename echoed by JupyterLab after peer sync already has the new path.
    // A server notification can update savePath before the local rename arrives.
    if (sharedPath === oldPath && sharedPath !== newPath) {
      this.options.model.transact(() => this.state.set('path', newPath), false);
    } else if (this.savePath === oldPath) {
      this.savePath = newPath;
    }
  }

  onRoomRenamed(path) {
    this.savePath = path;
  }

  onExternalChange(deleted) {
    if (this.externalChangeStatus) {
      return;
    }
    const change = { originalPath: this.savePath };
    if (deleted) {
      change.reason = 'deleted';
    }
    this.externalChangeStatus = change;
    this.externalChange.updateStatus(change);
    // Stop reconnecting while keeping the notice and shared model alive.
    queueMicrotask(() => super.dispose());
  }

  async saveExternalChangeAs(path, originalPath) {
    if (!path || (
      this.externalChangeStatus?.reason !== 'deleted' &&
      PathExt.normalize(path) === PathExt.normalize(originalPath)
    )) {
      throw new Error('Choose a different filename.');
    }
    try {
      await this.options.drive.get(path, { content: false });
    } catch (error) {
      if (error instanceof ServerConnection.ResponseError && error.response.status === 404) {
        await this.saveSnapshot(path);
        return;
      }
      throw error;
    }
    throw new Error('That filename already exists. Choose a new filename.');
  }

  async saveSnapshot(path) {
    const { drive, contentType, format, model } = this.options;
    await drive.save(path, { type: contentType, format, content: model.getSource() });
  }

  checkExternalChange() {
    if (this.externalChangeStatus) {
      throw new Error(this.externalChangeStatus.reason === 'deleted'
        ? DOCUMENT_DELETED_MESSAGE
        : DOCUMENT_CHANGED_MESSAGE);
    }
  }

  save() {
    const save = this.pendingSave.then(async () => {
      this.checkExternalChange();
      await this.ready;
      this.checkExternalChange();
      if (this.isDisposed) {
        throw new Error('Reopen the document before saving.');
      }
      const roomId = this.getRoomId();
      if (!roomId) {
        throw new Error('Reopen the document before saving.');
      }
      const baseUrl = this.serverSettings.baseUrl.replace(/\/+$/, '');
      const url = `${baseUrl}/api/collaboration/save/${encodeURIComponent(roomId)}`;
      const response = await ServerConnection.makeRequest(url, {
        method: 'PUT',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          path: this.savePath,
          content: this.options.model.getSource()
        })
      }, this.serverSettings);
      if (!response.ok) {
        throw await ServerConnection.ResponseError.create(response);
      }
    });
    // A failed save must not prevent subsequent attempts.
    this.pendingSave = save.catch(() => {});
    return save;
  }

  dispose() {
    if (this.externalChange.isDisposed) {
      return;
    }
    this.externalChange.dispose();
    this.state.unobserve(this.onSharedPathChanged);
    this.options.drive.fileChanged.disconnect(this.onFileChanged, this);
    super.dispose();
  }
}

module.exports = {
  id: 'jupyter-websocket-provider:document-factory',
  autoStart: true,
  provides: IDocumentProviderFactory,
  requires: [IDocumentManager],
  activate: (app, docManager) => ({
    create: options => new ContentsSavingProvider(options, app.shell, docManager)
  })
};
