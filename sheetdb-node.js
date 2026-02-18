class SheetDB {
  constructor(url) { this.url = url; }

  async getSchema() { return this._request('getSchema'); }

  collection(name) {
    const db = this;
    const req = (action, payload) => this._request(action, { collection: name, ...payload });

    return {
      find: async (query = {}, options = {}) => {
        if (typeof query === 'function') query = { $where: query.toString() };
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
  toObject() {
    const obj = { ...this };
    delete obj._row; 
    return obj;
  }
  toJSON() { return this.toObject(); }
  async save() {
    const { collection, db } = this._meta;
    const updateData = this.toObject();
    delete updateData._id;
    return db.collection(collection).updateOne({ _id: this._id }, updateData);
  }
}

module.exports = { SheetDB };
