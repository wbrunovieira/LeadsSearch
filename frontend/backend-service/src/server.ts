import Fastify from "fastify";
import cors from "@fastify/cors";
import { initDatabase } from "./database/sqliteService";
import { captureDockerLogs } from "./logs/logService";
import { Database } from "sql.js";

const server = Fastify({ logger: true });
let db: Database | null = null;

server.register(cors, {
    origin: "*",
    methods: ["GET", "POST"],
    allowedHeaders: ["Content-Type"],
});

server.get("/api/logs", async (request, reply) => {
    console.log("Rota /api/logs acessada");
    const logs = db ? db.exec("SELECT * FROM logs ORDER BY id DESC") : null;

    if (!db) {
        reply.status(500).send({ error: "Banco de dados não inicializado" });
        return;
    }
    if (!logs || logs.length === 0) {
        reply
            .status(500)
            .send({
                error: "Banco de dados não inicializado ou falha na consulta",
            });
        return;
    }

    reply.send(logs[0]?.values || []);
});

const startServer = async () => {
    try {
        db = await initDatabase();

        console.log("db inicializado com sucesso.");
        try {
            await captureDockerLogs(db);
            console.log("CaptureDockerLogs finalizado com sucesso.");
        } catch (error) {
            console.error("Erro durante a execução de captureDockerLogs:", error);
        }

        await server.listen({ port: 3333 });
        console.log("Servidor rodando na porta 3333");
    } catch (err) {
        server.log.error(err);
        process.exit(1);
    }
};


startServer();
