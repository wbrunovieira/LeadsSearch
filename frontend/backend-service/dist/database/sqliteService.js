import Database from 'better-sqlite3';
const db = new Database('/app/logs.db', { verbose: console.log });
db.prepare(`
  CREATE TABLE IF NOT EXISTS logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT,
    message TEXT
  )
`).run();
export const insertLog = (message) => {
    const stmt = db.prepare('INSERT INTO logs (timestamp, message) VALUES (?, ?)');
    stmt.run(new Date().toISOString(), message);
    console.log(`Log inserido: ${message}`);
};
// Função para consultar logs
export const getLogs = () => {
    const stmt = db.prepare('SELECT * FROM logs ORDER BY id DESC');
    return stmt.all();
};
