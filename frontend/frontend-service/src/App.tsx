import "./App.css";
import LogMonitor from "./components/LogMonitor";
import ServiceCard from "./components/ServiceCard";

function App() {
    return (
        <>
          <div className="min-h-screen bg-gradient-to-r from-[#350545] to-[#792990] flex flex-col items-center justify-start p-6">
            <header className="text-center mb-12">
                <h1 className="text-4xl font-bold text-[#ffb947] mb-4">Leads Search Dashboard</h1>
                <p className="text-[#ffb947] text-lg">Monitoramento em tempo real dos serviços</p>
            </header>

            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-8 w-full max-w-6xl">
                
                <ServiceCard serviceName="Lead Search" />
                <ServiceCard serviceName="API Gateway" />
                <ServiceCard serviceName="Scrapper" />
                <ServiceCard serviceName="Validator" />
                <ServiceCard serviceName="Website Fetcher" />
                <ServiceCard serviceName="Link Fetcher" />
            </div>

            <div className="mt-12 w-full max-w-6xl">
                <LogMonitor />
            </div>
        </div>
        </>
    );
}

export default App;
