import Fastify from "fastify";
import cors from "@fastify/cors";
import { captureDockerLogs } from "./logs/logService.js";
import { getLogs } from "./database/sqliteService.js";
const server = Fastify({ logger: true });
server.register(cors, { origin: "*", methods: ["GET", "POST"] });
server.get('/api/logs', async (request, reply) => {
    try {
        const logs = getLogs();
        reply.send(logs);
    }
    catch (error) {
        console.error('Erro ao buscar logs:', error);
        reply.status(500).send({ error: 'Erro ao buscar logs.' });
    }
});
const startServer = async () => {
    try {
        console.log("Iniciando servidor...");
        try {
            await captureDockerLogs();
            console.log("Logs do Docker capturados com sucesso.");
        }
        catch (error) {
            console.error("Erro ao capturar logs do Docker:", error);
        }
        await server.listen({ port: 3333 });
        console.log("Servidor rodando na porta 3333");
    }
    catch (err) {
        server.log.error(err);
        process.exit(1);
    }
};
startServer();
