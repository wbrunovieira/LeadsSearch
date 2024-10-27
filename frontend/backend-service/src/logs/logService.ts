import { exec } from "child_process";
import { Database } from "sql.js";
import { insertLog } from "../database/sqliteService";

export const captureDockerLogs = (db: Database): Promise<void> => {
    return new Promise((resolve, reject) => {
        const command = "echo 'Teste de execução de comando'";

        console.log("Iniciando execução do comando:", command);

        const childProcess = exec(command, (error, stdout, stderr) => {
            if (error) {
                console.error(`Erro ao executar o comando: ${error.message}`);
                reject(error);
                return;
            }

            if (stderr) {
                console.error(`Erro no comando: ${stderr}`);
                reject(new Error(stderr));
                return;
            }
        });

        console.log("captureDockerLogs command initiated");

        if (!childProcess.stdout) {
            const error = new Error(
                "Erro: Não foi possível capturar stdout do processo filho."
            );
            console.error(error.message);
            reject(error);
            return;
        }

        childProcess.stdout.on("data", (data) => {
            console.log(`Log capturado: ${data}`);
            insertLog(db, data);
        });

        childProcess.stderr?.on("data", (data) => {
            console.log(`Erro capturado: ${data}`);
            insertLog(db, `ERROR: ${data}`);
        });

        childProcess.on("close", (code) => {
            console.log(`Processo de logs finalizado com código ${code}`);
            resolve();
        });
    });
};
