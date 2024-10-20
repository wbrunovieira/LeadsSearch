import initSqlJs, { Database } from 'sql.js';


export const initDatabase = async (): Promise<Database> => {
  const SQL = await initSqlJs({
    locateFile: file => `https://cdnjs.cloudflare.com/ajax/libs/sql.js/1.6.1/sql-wasm.wasm`
  });

  const db = new SQL.Database();


  db.run(`
    CREATE TABLE IF NOT EXISTS logs (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      timestamp TEXT,
      message TEXT
    )
  `);

  console.log("Banco de dados SQLite inicializado.");
  return db;
};


export const insertLog = (db: Database, message: string) => {
  db.run(`INSERT INTO logs (timestamp, message) VALUES (?, ?)`, [
    new Date().toISOString(),
    message
  ]);
};
