import React, { useEffect, useState } from "react";

const LogMonitor: React.FC = () => {
    const [logs, setLogs] = useState<string[]>([]);

    useEffect(() => {
        const initialize = async () => {
            fetchLogs();
        };
        initialize();
    }, []);

    const fetchLogs = async () => {
        try {
            const response = await fetch("http://localhost:3333/api/logs");
            console.log("fetchLogs response",response)
            const data = await response.json();
            setLogs(data.map((log: any) => `${log[1]}: ${log[2]}`));
        } catch (error) {
            console.error("Erro ao buscar logs:", error);
        }
    };

    return (
        <div className="bg-[#350545] p-6 rounded-xl shadow-md text-white-100">
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
