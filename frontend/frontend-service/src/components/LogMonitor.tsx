import React, { useEffect, useState } from "react";

import { Database } from "sql.js";
import { initDatabase } from "../database/sqliteService";
import { captureDockerLogs } from "../logs/logService";

const LogMonitor: React.FC = () => {
    const [logs, setLogs] = useState<string[]>([]);
    const [db, setDb] = useState<Database | null>(null);

    useEffect(() => {
        const initialize = async () => {
            const dbInstance = await initDatabase();
            setDb(dbInstance);
            captureDockerLogs(dbInstance);
        };
        initialize();
    }, []);

    useEffect(() => {
        if (!db) return;

        setLogs((prevLogs) => [
            ...prevLogs,
            "Exemplo de log 1",
            "Exemplo de log 2",
        ]);
    }, [db]);

    return (
        <div className="bg-[#350545] p-6 rounded-xl shadow-md text-white">
            <h2 className="text-2xl font-bold mb-4">Logs Recentes</h2>{" "}
            <h2 className="text-2xl font-bold mb-4">Logs Recentes</h2>
            <div
                style={{
                    maxHeight: "300px",
                    overflowY: "scroll",
                    border: "1px solid black space-y-2",
                }}
            >
                {logs.map((log, index) => (
                    <p key={index}>{log}</p>
                ))}
            </div>
        </div>
    );
};

export default LogMonitor;
