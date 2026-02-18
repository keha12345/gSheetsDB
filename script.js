/**
 * gSheetsDB v1.0.0
 * Serverless NoSQL Database Engine for Google Sheets
 * * GitHub: https://github.com/keha12345/gSheetsDB
 */

const ss = SpreadsheetApp.getActiveSpreadsheet();

function doGet(e) {
  const serviceUrl = ScriptApp.getService().getUrl();
  const sdk = `
export class SheetDB {
  constructor(url = "${serviceUrl}") { this.url = url; }

  // Возвращает статус всех коллекций и их полей
  async getSchema() {
    return this._request('getSchema');
  }

  collection(name) {
    const db = this;
    const req = (action, payload) => this._request(action, { collection: name, ...payload });

    return {
      find: async (query = {}, options = {}) => {
        // Если переданы функции sort или where, конвертируем их в строки
        if (typeof query === 'function') query = { $where: query.toString() };
        if (query.$where && typeof query.$where === 'function') query.$where = query.$where.toString();
        if (options.sort && typeof options.sort === 'function') options.sort = options.sort.toString();

        const docs = await req('find', { query, options });
        return docs.map(d => new Document(d, name, db));
      },
      findOne: async (query = {}) => {
        const docs = await req('find', { query, options: { limit: 1 } });
        return docs.length ? new Document(docs[0], name, db) : null;
      },
      insertOne: async (data) => {
        const d = await req('insertOne', { data });
        return new Document(d, name, db);
      },
      updateOne: (query, data) => req('updateOne', { query, data }),
      deleteMany: (query) => req('deleteMany', { query })
    };
  }

  async _request(action, body = {}) {
    const res = await fetch(this.url, { method: 'POST', body: JSON.stringify({ action, ...body }) });
    const r = await res.json();
    if (r.status === 'error') throw new Error(r.message);
    return r.data;
  }
}

class Document {
  constructor(data, collection, db) {
    Object.assign(this, data);
    Object.defineProperty(this, '_meta', { value: { collection, db }, enumerable: false });
  }
  async save() {
    const { collection, db } = this._meta;
    const updateData = { ...this };
    delete updateData._id; 
    delete updateData._row;
    return db.collection(collection).updateOne({ _id: this._id }, updateData);
  }
}`;
  return ContentService.createTextOutput(sdk).setMimeType(ContentService.MimeType.JAVASCRIPT);
}

function doPost(e) {
  try {
    const req = JSON.parse(e.postData.contents);
    if (req.action === 'getSchema') return response({ status: 'success', data: getSchema() });

    const sheet = getOrCreateSheet(req.collection);
    const engine = new DatabaseEngine(sheet);
    let result;

    switch (req.action) {
      case 'find': result = engine.find(req.query, req.options || {}); break;
      case 'insertOne': result = engine.insertOne(req.data); break;
      case 'updateOne': result = engine.updateOne(req.query, req.data); break;
      case 'deleteMany': result = engine.deleteMany(req.query); break;
      default: throw "Unknown action";
    }
    return response({ status: 'success', data: result });
  } catch (err) {
    return response({ status: 'error', message: err.toString() });
  }
}

function getSchema() {
  return ss.getSheets().map(s => ({
    collection: s.getName(),
    count: s.getLastRow() - 1,
    fields: s.getRange(1, 1, 1, s.getLastColumn()).getDisplayValues()[0].filter(Boolean)
  }));
}

class DatabaseEngine {
  constructor(sheet) { this.sheet = sheet; }

  getDocs() {
    const vals = this.sheet.getDataRange().getDisplayValues();
    const head = vals.shift() || [];
    return vals.map((row, i) => {
      const d = { _row: i + 2 };
      head.forEach((h, j) => d[h] = row[j]);
      return d;
    });
  }

  find(query, options) {
    let docs = this.getDocs();

    // 1. Фильтрация
    if (query.$where) {
      const filterFn = new Function('doc', 'return (' + query.$where + ')(doc)');
      docs = docs.filter(doc => filterFn(doc));
    } else {
      docs = docs.filter(doc => this.match(doc, query));
    }

    // 2. Сортировка
    if (options.sort) {
      const sortFn = new Function('a', 'b', 'return (' + options.sort + ')(a, b)');
      docs.sort((a, b) => sortFn(a, b));
    }

    // 3. Лимит
    if (options.limit) docs = docs.slice(0, options.limit);

    return docs;
  }

  match(doc, query) {
    return Object.keys(query).every(key => {
      const val = query[key];
      const docVal = doc[key];
      if (val && typeof val === 'object') {
        if (val.$gt !== undefined) return docVal > val.$gt;
        if (val.$lt !== undefined) return docVal < val.$lt;
        if (val.$regex !== undefined) return new RegExp(val.$regex, val.$options || '').test(docVal);
        if (val.$startsWith !== undefined) return String(docVal).startsWith(val.$startsWith);
        if (val.$endsWith !== undefined) return String(docVal).endsWith(val.$endsWith);
      }
      return docVal == val;
    });
  }

  insertOne(data) {
    const head = this.sheet.getRange(1, 1, 1, Math.max(this.sheet.getLastColumn(), 1)).getDisplayValues()[0];
    data._id = data._id || 'id_' + Math.random().toString(36).substr(2, 9);
    data.createdAt = data.createdAt || new Date();
    Object.keys(data).forEach(k => {
      if (head.indexOf(k) === -1) {
        this.sheet.getRange(1, head.length + 1).setValue(k).setFontWeight("bold");
        head.push(k);
      }
    });
    this.sheet.appendRow(head.map(h => data[h] !== undefined ? data[h] : ""));
    return data;
  }

  updateOne(query, update) {
    const targets = this.find(query, {});
    const head = this.sheet.getRange(1, 1, 1, this.sheet.getLastColumn()).getDisplayValues()[0];
    targets.forEach(doc => {
      Object.keys(update).forEach(k => {
        let col = head.indexOf(k);
        if (col === -1) {
          this.sheet.getRange(1, head.length + 1).setValue(k).setFontWeight("bold");
          head.push(k); col = head.length - 1;
        }
        this.sheet.getRange(doc._row, col + 1).setValue(update[k]);
      });
    });
    return { modified: targets.length };
  }
}

function getOrCreateSheet(name) {
  let s = ss.getSheetByName(name);
  if (!s) { s = ss.insertSheet(name); s.appendRow(['_id', 'createdAt']).setFontWeight("bold"); }
  return s;
}

function response(obj) {
  return ContentService.createTextOutput(JSON.stringify(obj)).setMimeType(ContentService.MimeType.JSON);
}
