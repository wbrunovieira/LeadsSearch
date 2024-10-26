import Fastify from "fastify";
import cors from "@fastify/cors";
import { initDatabase} from "./database/sqliteService";
import { captureDockerLogs } from "./logs/logService";
import { Database } from "sql.js";

const server = Fastify({ logger: true });
let db: Database | null = null;

server.register(cors, {
    origin: "*",
});

server.get("/api/logs", async (request, reply) => {
    if (!db) {
        reply.status(500).send({ error: "Banco de dados não inicializado" });
        return;
    }

    const logs = db.exec("SELECT * FROM logs ORDER BY id DESC") || [];
    reply.send(logs[0]?.values || []);
});

const startServer = async () => {
    try {
        db = await initDatabase();

        captureDockerLogs(db);

        await server.listen({ port: 3333 });
        console.log("Servidor rodando na porta 3333");
    } catch (err) {
        server.log.error(err);
        process.exit(1);
    }
};

startServer();
