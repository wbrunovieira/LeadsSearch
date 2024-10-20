import { create } from 'zustand';



interface Service {
  name: string;
  status: string;
  lastUpdated: string;
}

interface StoreState {
  services: Service[];
  updateServices: (newServices: Service[]) => void;
}

const useStore = create<StoreState>((set) => ({
  services: [], 
  updateServices: (newServices) => set({ services: newServices }), 
}));

export default useStore;
