# 🚀 gSheetsDB v1.0.3

**gSheetsDB** — это легковесный NoSQL движок на базе Google Apps Script, который превращает обычную Google Таблицу в полноценную документоориентированную базу данных с API, вдохновленным **Mongoose**.

Главная фишка: **Zero-Installation**. Вам не нужно устанавливать пакеты через npm. Вы импортируете клиентский драйвер напрямую из вашего развернутого API.

---

## 🔥 Почему это круто?

* **📦 SDK на лету**: Просто добавьте `import` ссылки вашего скрипта. SDK сам знает свой URL.
* **🧠 Умный поиск**: Поддержка MongoDB-like операторов (`$gt`, `$regex`, `$startsWith`) и даже передача **чистых JS-функций** для фильтрации и сортировки прямо на сервере.
* **🏗 Auto-Schema**: База сама создает листы (коллекции) и колонки (поля) на основе ваших JSON-объектов.
* **📄 Active Record**: Полученные документы — это объекты с методами `.save()`, `.delete()` и `.toObject()`.
* **☁️ Serverless & Free**: Работает полностью на инфраструктуре Google.

---

## 🛠 Установка и Деплой (за 2 минуты)

1. **Создайте таблицу**: Создайте новую [Google Таблицу](https://sheets.new).
2. **Откройте редактор скриптов**: `Расширения` -> `Apps Script`.
3. **Вставьте код**: Скопируйте содержимое файла `engine.js` из этого репозитория.
4. **Разверните API**:
* Нажмите **Начать развертывание** (Deploy) -> **Новое развертывание**.
* Тип: **Веб-приложение** (Web App).
* Запуск от имени: **Вас** (Me).
* Доступ: **Все** (Anyone).


5. **Скопируйте URL**: Это ваш ключ к базе.

---

### 1. 💻 Установка и подключение (API Reference)

### Node.js (Серверная среда)

Установите пакет напрямую из этого репозитория GitHub:

```bash
npm install github:keha12345/gSheetsDB

```

#### **CommonJS (Если используете `require`)**

Подходит для большинства текущих Node.js проектов и скриптов:

```javascript
const { SheetDB } = require('gsheetdb');

const db = new SheetDB('ВАШ_URL_APPS_SCRIPT');

```

#### **ES Modules (Если используете `import`)**

Для современных проектов (React, Vite, проекты с `"type": "module"`):

```javascript
import { SheetDB } from 'gsheetdb';

const db = new SheetDB('ВАШ_URL_APPS_SCRIPT');

```

#### Браузер (Прямое подключение)

Если вам нужно использовать БД прямо на фронтенде без сборки:

```javascript
import { SheetDB } from 'ВАШ_URL_APPS_SCRIPT';

const db = new SheetDB();

```







---

### 2. Работа с коллекциями (Листами)

Метод `collection(name)` переключает контекст на конкретный лист. Если листа нет, он будет создан при первой записи.

```javascript
const Players = db.collection('players');

```

---

### 3. Создание данных (Insert)

Метод `insertOne(data)` принимает объект.

* Автоматически добавляет колонки, если ключи объекта отсутствуют в таблице.
* Автоматически генерирует `_id` и `createdAt`.

```javascript
const user = await Players.insertOne({
  name: "Ivan",
  age: 25,
  skills: ["JS", "React"],
  stats: { power: 80, agility: 90 } // Вложится как строка "[object Object]" или JSON
});

```

---

### 4. Поиск данных (Find)

#### А) Простой поиск (Equality)

```javascript
const users = await Players.find({ name: "Ivan", age: 25 });

```

#### Б) Операторы сравнения (Comparison)

Поддерживаются числа и строки (через автоматическое приведение типов).

```javascript
const results = await Players.find({
  age: { $gt: 18 },       // Больше (Greater Than)
  level: { $gte: 10 },    // Больше или равно
  rank: { $lt: 5 },       // Меньше
  score: { $lte: 100 },   // Меньше или равно
  status: { $ne: "banned" } // Не равно (Not Equal)
});

```

#### В) Строковые операторы и Regex

```javascript
const results = await Players.find({
  email: { $endsWith: "@gmail.com" },
  city: { $startsWith: "Mos" },
  bio: { $regex: "frontend", $options: "i" } // Поиск без учета регистра
});

```

#### Г) Функциональный фильтр (на стороне сервера)

Вы можете передать анонимную функцию. Она выполнится в контексте Google Apps Script.

```javascript
const activeAdmins = await Players.find(doc => {
  return doc.role === 'admin' && doc.lastLogin > '2025-01-01';
});

```

---

### 5. Сортировка, Лимиты и Выборка

#### Поиск одного документа

```javascript
const user = await Players.findOne({ _id: "id_xyz123" });

```

#### Сортировка и Лимит

```javascript
const topPlayers = await Players.find({}, {
  sort: (a, b) => b.score - a.score, // Сортировка от большего к меньшему
  limit: 10                         // Взять первые 10 записей
});


// Найти самую последнюю созданную запись 
// _row создается автоматически и соответствует порядковому номеру записи что идиально для поиска последнего добавленного
const lastRecord = await Players.find({}, {
  sort: (a, b) => b._row - a._row,
  limit: 1
});
```

---

### 6. Обновление и Модель Документа (Active Record)

#### Массовое обновление

```javascript
await Players.update(
  { status: "pending" }, // Фильтр
  { status: "active", confirmedAt: new Date().toISOString() } // Новые данные
);

```

#### Работа с экземпляром (Document)

Объекты, возвращаемые `find` и `insertOne`, являются экземплярами класса `Document`.

```javascript
const user = await Players.findOne({ name: "Ivan" });

user.score += 10;            // Меняем поле
user.newField = "Secret";    // Добавляем новое (создаст колонку в таблице)
await user.save();           // Сохраняем изменения в таблицу

await user.delete();         // Удаляем эту конкретную запись из таблицы

```

---

### 7. Трансформация данных (Чистый JSON)

Если вам нужно передать данные в стейт-менеджер (Redux, Vuex) без методов `save()` и служебного поля `_row`.

```javascript
const rawObject = user.toObject(); 
// или просто:
const json = JSON.stringify(user);

```

---

### 8. Интроспекция базы (Schema)

Позволяет узнать структуру всех листов, не открывая Google Таблицу.

```javascript
const schema = await db.getSchema();
/*
Возвращает массив:
[{
  collection: "players",
  count: 145,
  fields: ["_id", "createdAt", "name", "age", "score"]
}, ...]
*/

```

---

### 9. Массовое удаление

```javascript
// Удалить всех забаненных
await Players.deleteMany({ status: "banned" });

// Очистить всю коллекцию (будьте осторожны!)
await Players.deleteMany({}); 

```

---

### 🚀 Нюансы фильтрации (Type Casting)

Поскольку Google Таблицы — это визуальный инструмент, **gSheetsDB** считывает данные методом `getDisplayValues()` (как строки).

1. При выполнении `find` с операторами `$gt`, `$lt` и т.д., движок пробует превратить значение ячейки в число.
2. Если в ячейке текст "100", он будет сравниваться как число `100`.
3. Если в ячейке текст "VIP", сравнение с числом (например, `> 50`) всегда вернет `false`, не вызывая ошибки.


---

## 📊 Как хранятся данные

**gSheetsDB** использует концепцию **"What You See Is What You Get"**:

* Данные читаются через `getDisplayValues()`. Это значит, вы получаете их ровно в том формате, в котором они видны в таблице (с учетом форматирования дат и чисел).
* **Auto-casting**: При выполнении запросов движок автоматически пробует привести строки к числам для корректной математической обработки (`$gt`, `$lt`).

---

## 💡 Полезные нюансы

### Работа с чистыми данными

Если вам нужно передать данные документа в State-менеджер (Redux/Vuex) или в `localStorage`, используйте метод `.toObject()` или просто `JSON.stringify()`:

```javascript
const raw = user.toObject(); // Чистый JS объект без методов драйвера

```

### Безопасность

Функциональные запросы (`$where`, `sort`) используют `new Function()`. Это дает огромную гибкость, но означает, что любой человек, знающий URL вашего API, может выполнить произвольный JS-код в контексте вашего Google-аккаунта.
**Рекомендация:** используйте gSheetsDB для внутренних проектов, админ-панелей или защищайте URL через прокси-сервер.

---

## ⚠️ Лимиты и Квоты

* **Запросы**: ~20,000 в день (лимит Google Apps Script).
* **Время**: Один запрос не может длиться дольше 360 секунд.
* **Объем**: Google Таблицы ограничены 10,000,000 ячеек.

---

**Лицензия:** MIT. Создано для быстрой разработки и прототипирования. 🚀

---

