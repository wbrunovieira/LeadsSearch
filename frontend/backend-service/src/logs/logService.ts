import { exec } from 'child_process';
import { Database } from 'sql.js';
import { insertLog } from '../database/sqliteService';


export const captureDockerLogs = (db: Database) => {
  const command = 'docker-compose logs -f leads_search-lead-search-1';
  const childProcess = exec(command);

  childProcess.stdout?.on('data', (data: string) => {
    console.log(`Log capturado: ${data}`);
    insertLog(db, data);
  });

  childProcess.stderr?.on('data', (data: string) => {
    console.log(`Erro capturado: ${data}`);
    insertLog(db, `ERROR: ${data}`);
  });

  childProcess.on('close', (code: number) => {
    console.log(`Processo de logs finalizado com código ${code}`);
  });
};
