import { exec } from 'child_process';
import Database from 'better-sqlite3';
// Inicializando o banco de dados
const db = new Database('logs.db', { verbose: console.log });
// Criando a tabela de logs (se não existir)
db.prepare(`
  CREATE TABLE IF NOT EXISTS logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT,
    message TEXT
  )
`).run();
// Função para inserir log no banco de dados
const insertLog = (message) => {
    const stmt = db.prepare('INSERT INTO logs (timestamp, message) VALUES (?, ?)');
    stmt.run(new Date().toISOString(), message);
    console.log(`Log inserido: ${message}`);
};
// Função para capturar logs do Docker e armazená-los
export const captureDockerLogs = () => {
    return new Promise((resolve, reject) => {
        const command = "echo 'Teste de execução de comando'";
        console.log('Iniciando execução do comando:', command);
        const childProcess = exec(command, (error, stdout, stderr) => {
            if (error) {
                console.error(`Erro ao executar o comando: ${error.message}`);
                insertLog(`ERROR: ${error.message}`);
                reject(error);
                return;
            }
            if (stderr) {
                console.error(`Erro no comando: ${stderr}`);
                insertLog(`ERROR: ${stderr}`);
                reject(new Error(stderr));
                return;
            }
        });
        console.log('captureDockerLogs command initiated');
        if (!childProcess.stdout) {
            const error = new Error('Erro: Não foi possível capturar stdout do processo filho.');
            console.error(error.message);
            insertLog(error.message);
            reject(error);
            return;
        }
        childProcess.stdout.on('data', (data) => {
            console.log(`Log capturado: ${data}`);
            insertLog(data);
        });
        childProcess.stderr?.on('data', (data) => {
            console.log(`Erro capturado: ${data}`);
            insertLog(`ERROR: ${data}`);
        });
        childProcess.on('close', (code) => {
            console.log(`Processo de logs finalizado com código ${code}`);
            resolve();
        });
    });
};
