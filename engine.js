/**
 * gSheetsDB v1.0.3
 * Serverless NoSQL Database Engine for Google Sheets
 * GitHub: https://github.com/keha12345/gSheetsDB
 */

const ss = SpreadsheetApp.getActiveSpreadsheet();

/**
 * GET: Раздает SDK драйвер с динамическим URL
 */
function doGet(e) {
  const serviceUrl = ScriptApp.getService().getUrl();
  const sdk = `
export class SheetDB {
  constructor(url = "${serviceUrl}") { this.url = url; }

  async getSchema() {
    return this._request('getSchema');
  }

  collection(name) {
    const db = this;
    const req = (action, payload) => this._request(action, { collection: name, ...payload });

    return {
      find: async (query = {}, options = {}) => {
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

constructor(data, collection, db) {
    Object.assign(this, data);
    // Приватные мета-данные, не участвуют в циклах и сериализации
    Object.defineProperty(this, '_meta', { value: { collection, db }, enumerable: false });
  }

  // Возвращает чистый объект без служебных полей
  toObject() {
    const obj = { ...this };
    delete obj._row; 
    return obj;
  }

  // Для корректного JSON.stringify()
  toJSON() {
    return this.toObject();
  }

  // Обновление текущего документа
  async save() {
    const { collection, db } = this._meta;
    const updateData = this.toObject();
    delete updateData._id; // ID используем только для поиска
    return db.collection(collection).updateOne({ _id: this._id }, updateData);
  }

  // Удаление текущего документа
  async delete() {
    const { collection, db } = this._meta;
    return db.collection(collection).deleteMany({ _id: this._id });
  }
}`;
  return ContentService.createTextOutput(sdk).setMimeType(ContentService.MimeType.JAVASCRIPT);
}

/**
 * POST: Обработка NoSQL операций
 */
function doPost(e) {
  try {
    const req = JSON.parse(e.postData.contents);
    
    // Метод интроспекции базы
    if (req.action === 'getSchema') {
      return response({ status: 'success', data: getSchema() });
    }

    if (!req.collection) throw "Collection name is required";

    const sheet = getOrCreateSheet(req.collection);
    const engine = new DatabaseEngine(sheet);
    let result;

    switch (req.action) {
      case 'find': 
        result = engine.find(req.query || {}, req.options || {}); 
        break;
      case 'insertOne': 
        result = engine.insertOne(req.data || {}); 
        break;
      case 'updateOne': 
        result = engine.updateOne(req.query || {}, req.data || {}); 
        break;
      case 'deleteMany': 
        result = engine.deleteMany(req.query || {}); 
        break;
      default: 
        throw "Unknown action: " + req.action;
    }
    return response({ status: 'success', data: result });

  } catch (err) {
    return response({ status: 'error', message: err.toString() });
  }
}

// --- СИСТЕМНЫЕ ФУНКЦИИ ---

function getSchema() {
  return ss.getSheets().map(s => ({
    collection: s.getName(),
    count: Math.max(0, s.getLastRow() - 1),
    fields: s.getRange(1, 1, 1, Math.max(1, s.getLastColumn())).getDisplayValues()[0].filter(Boolean)
  }));
}

function getOrCreateSheet(name) {
  let s = ss.getSheetByName(name);
  if (!s) {
    s = ss.insertSheet(name);
    s.appendRow(['_id', 'createdAt']);
    s.getRange(1, 1, 1, 2).setFontWeight("bold").setBackground("#f3f3f3");
  }
  return s;
}

function response(obj) {
  return ContentService.createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}

/**
 * ЯДРО ДВИЖКА
 */
class DatabaseEngine {
  constructor(sheet) {
    this.sheet = sheet;
  }

  // Считываем всё как строки (DisplayValues)
  getDocs() {
    const range = this.sheet.getDataRange();
    const vals = range.getDisplayValues();
    const head = vals.shift() || [];
    return vals.map((row, i) => {
      const d = { _row: i + 2 };
      head.forEach((h, j) => d[h] = row[j]);
      return d;
    });
  }

  // Каст только для логики сравнения
  autoCast(val) {
    const num = parseFloat(val);
    return (!isNaN(num) && isFinite(val)) ? num : val;
  }

  match(doc, query) {
    return Object.keys(query).every(key => {
      try {
        const val = query[key];
        const docRaw = doc[key];
        
        const dVal = this.autoCast(docRaw);
        
        // Если в запросе оператор (объект)
        if (val && typeof val === 'object' && !Array.isArray(val)) {
          if (val.$gt !== undefined) return dVal > val.$gt;
          if (val.$lt !== undefined) return dVal < val.$lt;
          if (val.$gte !== undefined) return dVal >= val.$gte;
          if (val.$lte !== undefined) return dVal <= val.$lte;
          if (val.$ne !== undefined) return dVal != val.$ne;
          
          if (val.$regex !== undefined) return new RegExp(val.$regex, val.$options || '').test(docRaw);
          if (val.$startsWith !== undefined) return String(docRaw).startsWith(val.$startsWith);
          if (val.$endsWith !== undefined) return String(docRaw).endsWith(val.$endsWith);
          
          return true;
        }
        
        // Обычное сравнение
        return dVal == this.autoCast(val);
      } catch (e) {
        return false;
      }
    });
  }

  find(query, options) {
    let docs = this.getDocs();

    // 1. Фильтрация ($where или объект)
    if (query.$where) {
      const filterFn = new Function('doc', 'return (' + query.$where + ')(doc)');
      docs = docs.filter(doc => {
        try { return filterFn(doc); } catch(e) { return false; }
      });
    } else {
      docs = docs.filter(doc => this.match(doc, query));
    }

    // 2. Сортировка
    if (options.sort) {
      const sortFn = new Function('a', 'b', 'return (' + options.sort + ')(a, b)');
      docs.sort((a, b) => {
        try { return sortFn(a, b); } catch(e) { return 0; }
      });
    }

    if (options.limit) docs = docs.slice(0, options.limit);
    return docs;
  }

  insertOne(data) {
    const headRange = this.sheet.getRange(1, 1, 1, Math.max(1, this.sheet.getLastColumn()));
    const head = headRange.getValues()[0].map(String);
    
    data._id = data._id || 'id_' + Math.random().toString(36).substr(2, 9);
    data.createdAt = data.createdAt || new Date().toISOString();

    Object.keys(data).forEach(k => {
      if (head.indexOf(k) === -1) {
        this.sheet.getRange(1, head.length + 1).setValue(k)
          .setFontWeight("bold").setBackground("#f3f3f3");
        head.push(k);
      }
    });

    const row = head.map(h => data[h] !== undefined ? data[h] : "");
    this.sheet.appendRow(row);
    return data;
  }

  updateOne(query, update) {
    const targets = this.find(query, {});
    const head = this.sheet.getRange(1, 1, 1, Math.max(1, this.sheet.getLastColumn())).getValues()[0].map(String);
    
    targets.forEach(doc => {
      Object.keys(update).forEach(k => {
        let col = head.indexOf(k);
        if (col === -1) {
          this.sheet.getRange(1, head.length + 1).setValue(k)
            .setFontWeight("bold").setBackground("#f3f3f3");
          head.push(k); 
          col = head.length - 1;
        }
        this.sheet.getRange(doc._row, col + 1).setValue(update[k]);
      });
    });
    return { modifiedCount: targets.length };
  }

  deleteMany(query) {
    const targets = this.find(query, {});
    targets.sort((a, b) => b._row - a._row).forEach(doc => {
      this.sheet.deleteRow(doc._row);
    });
    return { deletedCount: targets.length };
  }
}
