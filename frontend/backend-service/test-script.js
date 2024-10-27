import { initDatabase } from './src/database/sqliteService';
import { captureDockerLogs } from './src/logs/logService';

(async () => {
    try {
        console.log("Iniciando teste de initDatabase...");
        const db = await initDatabase();
        console.log("Banco de dados inicializado com sucesso.");

        console.log("Iniciando teste de captureDockerLogs...");
        await captureDockerLogs(db);
        console.log("Captura de logs do Docker finalizada com sucesso.");
    } catch (error) {
        console.error("Erro durante o teste:", error);
    }
})();

