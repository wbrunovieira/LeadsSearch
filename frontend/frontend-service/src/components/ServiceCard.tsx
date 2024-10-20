import React from 'react';

interface ServiceCardProps {
    serviceName: string;
}

const ServiceCard: React.FC<ServiceCardProps> = ({ serviceName }) => {
    return (
        <div className="bg-gradient-to-r from-[#350545] to-[#792990] p-6 rounded-xl shadow-md hover:shadow-lg transition-shadow duration-300 text-white flex flex-col items-center justify-center h-48">
            <h2 className="text-xl font-bold mb-2">{serviceName}</h2>
            <p>Status: <span className="text-green-400">Ativo</span></p>
        </div>
    );
}

export default ServiceCard;
