/*
 * The recording plugin of tools/record-link-fixtures.sh (OBS-024 task 8.1). Plain CommonJS, no
 * build step.
 *
 * It runs inside the real Obsidian app, in an isolated profile whose only vault is an empty
 * recording vault. For each fixture under services/notes/tests/fixtures/link-rewrite/ it:
 *
 * 1. removes every file of the vault, and sets `alwaysUpdateLinks`, `newLinkFormat` (from
 *    case.json) and `useMarkdownLinks: false`;
 * 2. creates the files of before/ through the vault API, byte for byte, and waits until the
 *    metadata cache holds every note;
 * 3. renames `from` to `to` with `app.fileManager.renameFile`, the call behind `obsidian rename`
 *    and `obsidian move`, so Obsidian updates the links by its own rules;
 * 4. writes every file of the vault to after/, and the Obsidian version and the settings to
 *    recording.json.
 *
 * The script reads job.json next to this file and writes DONE when every fixture ended.
 */
'use strict';

const obsidian = require('obsidian');
const fs = require('fs');
const path = require('path');

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/** How long the vault must have no change before a step counts as settled. */
const QUIET_MS = 1500;
/** How long a step may take to settle. */
const SETTLE_LIMIT_MS = 20000;
/** The folders of a vault that are not notes. */
const SKIPPED = new Set(['.obsidian', '.trash']);

/** Every file under `root`, relative to it with `/`, sorted; the folders in SKIPPED are left out. */
function walk(root) {
  const found = [];
  const visit = (folder, prefix) => {
    for (const entry of fs.readdirSync(folder, { withFileTypes: true })) {
      if (prefix === '' && SKIPPED.has(entry.name)) continue;
      const relative = prefix + entry.name;
      if (entry.isDirectory()) visit(path.join(folder, entry.name), relative + '/');
      else if (entry.isFile()) found.push(relative);
    }
  };
  visit(root, '');
  return found.sort();
}

function parentOf(relative) {
  const at = relative.lastIndexOf('/');
  return at === -1 ? '' : relative.slice(0, at);
}

class LinkRecorder extends obsidian.Plugin {
  onload() {
    this.lastChange = Date.now();
    for (const name of ['create', 'modify', 'delete', 'rename']) {
      this.registerEvent(this.app.vault.on(name, () => { this.lastChange = Date.now(); }));
    }
    this.app.workspace.onLayoutReady(() => {
      this.run().catch((error) => this.finish({ error: String((error && error.stack) || error) }));
    });
  }

  base() {
    return this.app.vault.adapter.getBasePath();
  }

  finish(status) {
    fs.writeFileSync(this.job.done, JSON.stringify(status, null, 2) + '\n');
  }

  async run() {
    this.job = JSON.parse(fs.readFileSync(path.join(this.base(), this.manifest.dir, 'job.json'), 'utf8'));
    const only = this.job.only || [];
    const names = fs.readdirSync(this.job.fixtures)
      .filter((name) => fs.existsSync(path.join(this.job.fixtures, name, 'case.json')))
      .filter((name) => only.length === 0 || only.includes(name))
      .sort();
    const status = {};
    for (const name of names) {
      try {
        await this.record(name);
        status[name] = 'ok';
      } catch (error) {
        status[name] = 'error: ' + String((error && error.message) || error);
      }
    }
    this.finish({ status });
  }

  async record(name) {
    const vault = this.app.vault;
    const folder = path.join(this.job.fixtures, name);
    const spec = JSON.parse(fs.readFileSync(path.join(folder, 'case.json'), 'utf8'));
    await this.clear();
    vault.setConfig('alwaysUpdateLinks', true);
    vault.setConfig('newLinkFormat', spec.settings.newLinkFormat);
    vault.setConfig('useMarkdownLinks', false);
    const before = path.join(folder, 'before');
    for (const relative of walk(before)) {
      const bytes = fs.readFileSync(path.join(before, relative));
      await this.ensureFolder(parentOf(relative));
      await vault.createBinary(relative, bytes.buffer.slice(bytes.byteOffset, bytes.byteOffset + bytes.byteLength));
    }
    await this.settle();
    const file = vault.getAbstractFileByPath(spec.from);
    if (!(file instanceof obsidian.TFile)) throw new Error('the vault has no file ' + spec.from);
    await this.ensureFolder(parentOf(spec.to));
    await this.app.fileManager.renameFile(file, spec.to);
    await this.settle();

    const after = path.join(folder, 'after');
    fs.rmSync(after, { recursive: true, force: true });
    for (const relative of walk(this.base())) {
      const target = path.join(after, relative);
      fs.mkdirSync(path.dirname(target), { recursive: true });
      fs.copyFileSync(path.join(this.base(), relative), target);
    }
    const installer = (navigator.userAgent.match(/obsidian\/(\S+)/) || [])[1] || null;
    const recording = {
      obsidian: obsidian.apiVersion,
      installer,
      settings: {
        alwaysUpdateLinks: vault.getConfig('alwaysUpdateLinks'),
        newLinkFormat: vault.getConfig('newLinkFormat'),
        useMarkdownLinks: vault.getConfig('useMarkdownLinks'),
      },
    };
    fs.writeFileSync(path.join(folder, 'recording.json'), JSON.stringify(recording, null, 2) + '\n');
  }

  /** Remove every file and folder of the vault except the configuration. */
  async clear() {
    for (const child of [...this.app.vault.getRoot().children]) {
      await this.app.vault.delete(child, true);
    }
    await this.settle();
  }

  async ensureFolder(folder) {
    if (folder === '') return;
    let current = '';
    for (const part of folder.split('/')) {
      current = current === '' ? part : current + '/' + part;
      if (!this.app.vault.getAbstractFileByPath(current)) await this.app.vault.createFolder(current);
    }
  }

  /** Wait until the metadata cache holds every note and the vault had no change for QUIET_MS. */
  async settle() {
    const start = Date.now();
    for (;;) {
      await sleep(100);
      const cached = this.app.vault.getMarkdownFiles().every((file) => this.app.metadataCache.getFileCache(file));
      const now = Date.now();
      if (cached && now - this.lastChange > QUIET_MS && now - start > QUIET_MS) return;
      if (now - start > SETTLE_LIMIT_MS) throw new Error('the vault did not settle');
    }
  }
}

module.exports = LinkRecorder;
