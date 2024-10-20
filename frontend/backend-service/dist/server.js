import Fastify from "fastify";
const fastify = Fastify({ logger: true });
fastify.get("/", async (request, reply) => {
    return { hello: "world" };
});
const start = async () => {
    try {
        await fastify.listen({ port: 3333 });
        console.log("Servidor rodando na porta 3333");
    }
    catch (err) {
        fastify.log.error(err);
        process.exit(1);
    }
};
start();
