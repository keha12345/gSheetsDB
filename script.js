/**
 * gSheetsDB v1.0.0
 * Serverless NoSQL Database Engine for Google Sheets
 * * GitHub: https://github.com/keha12345/gSheetsDB
 */

const ss = SpreadsheetApp.getActiveSpreadsheet();

/**
 * GET: Раздает SDK драйвер с прошитым URL
 */
function doGet(e) {
  const serviceUrl = ScriptApp.getService().getUrl();
  
  const sdk = `
/**
 * gSheetsDB Client SDK
 */
export class SheetDB {
  constructor(url = "${serviceUrl}") {
    this.url = url;
  }

  collection(name) {
    const request = async (action, payload = {}) => {
      const response = await fetch(this.url, {
        method: 'POST',
        body: JSON.stringify({ collection: name, action, ...payload })
      });
      const result = await response.json();
      if (result.status === 'error') throw new Error(result.message);
      return result.data;
    };

    return {
      find: (query = {}) => request('find', { query }),
      findOne: (query = {}) => request('findOne', { query }),
      insertOne: (data) => request('insertOne', { data }),
      updateOne: (query, data) => request('updateOne', { query, data }),
      deleteMany: (query = {}) => request('deleteMany', { query })
    };
  }
}
  `;

  return ContentService.createTextOutput(sdk)
    .setMimeType(ContentService.MimeType.JAVASCRIPT);
}

/**
 * POST: Обработка NoSQL операций
 */
function doPost(e) {
  try {
    const request = JSON.parse(e.postData.contents);
    const { action, collection, query, data } = request;
    
    if (!collection) throw new Error("Collection name is required");
    
    const sheet = getOrCreateSheet(collection);
    const engine = new DatabaseEngine(sheet);
    let result;

    switch (action) {
      case 'find': result = engine.find(query); break;
      case 'findOne': result = engine.find(query)[0] || null; break;
      case 'insertOne': result = engine.insertOne(data); break;
      case 'updateOne': result = engine.updateOne(query, data); break;
      case 'deleteMany': result = engine.deleteMany(query); break;
      default: throw new Error("Action " + action + " not supported");
    }

    return renderJson({ status: 'success', data: result });
  } catch (err) {
    return renderJson({ status: 'error', message: err.toString() });
  }
}

// --- ВНУТРЕННИЕ ФУНКЦИИ ---

function getOrCreateSheet(name) {
  let sheet = ss.getSheetByName(name);
  if (!sheet) {
    sheet = ss.insertSheet(name);
    // Инициализируем базовыми системными колонками
    sheet.appendRow(['_id', 'createdAt']);
    sheet.getRange(1, 1, 1, 2).setFontWeight("bold").setBackground("#f3f3f3");
  }
  return sheet;
}

function renderJson(obj) {
  return ContentService.createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}

/**
 * Ядро обработки данных
 */
class DatabaseEngine {
  constructor(sheet) {
    this.sheet = sheet;
  }

  // Считывает таблицу и превращает её в массив объектов
  getDocuments() {
    const range = this.sheet.getDataRange();
    const values = range.getValues();
    const headers = values.shift() || [];
    
    return values.map((row, index) => {
      const doc = { _row: index + 2 }; // Сохраняем номер строки для обновлений
      headers.forEach((header, i) => {
        doc[header] = row[i];
      });
      return doc;
    });
  }

  // Поиск по объекту-фильтру
  find(query) {
    const docs = this.getDocuments();
    return docs.filter(doc => 
      Object.keys(query).every(key => doc[key] == query[key])
    );
  }

  // Вставка документа с авто-созданием колонок
  insertOne(data) {
    const headers = this.sheet.getRange(1, 1, 1, Math.max(this.sheet.getLastColumn(), 1)).getValues()[0];
    
    // Генерируем ID и время, если их нет
    data._id = data._id || 'id_' + Math.random().toString(36).substr(2, 9);
    data.createdAt = data.createdAt || new Date();

    // Проверяем новые поля и расширяем таблицу
    Object.keys(data).forEach(key => {
      if (headers.indexOf(key) === -1) {
        this.sheet.getRange(1, headers.length + 1).setValue(key)
            .setFontWeight("bold").setBackground("#f3f3f3");
        headers.push(key);
      }
    });

    // Формируем строку по порядку заголовков
    const row = headers.map(h => data[h] !== undefined ? data[h] : "");
    this.sheet.appendRow(row);
    return data;
  }

  // Обновление всех найденных по запросу документов
  updateOne(query, updateData) {
    const docs = this.find(query);
    if (docs.length === 0) return { modifiedCount: 0 };

    const headers = this.sheet.getRange(1, 1, 1, this.sheet.getLastColumn()).getValues()[0];

    docs.forEach(doc => {
      Object.keys(updateData).forEach(key => {
        let colIdx = headers.indexOf(key);
        // Если в обновлении новое поле — создаем колонку
        if (colIdx === -1) {
          this.sheet.getRange(1, headers.length + 1).setValue(key)
              .setFontWeight("bold").setBackground("#f3f3f3");
          headers.push(key);
          colIdx = headers.length - 1;
        }
        this.sheet.getRange(doc._row, colIdx + 1).setValue(updateData[key]);
      });
    });

    return { modifiedCount: docs.length };
  }

  // Удаление строк
  deleteMany(query) {
    const docs = this.find(query);
    // Удаляем с конца, чтобы не сбивать индексы строк
    docs.sort((a, b) => b._row - a._row).forEach(doc => {
      this.sheet.deleteRow(doc._row);
    });
    return { deletedCount: docs.length };
  }
}
