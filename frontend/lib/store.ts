import { create } from "zustand";

interface User {
    id: string;
    email: string;
    plan: string;
}

interface AuthState {
    user: User | null;
    accessToken: string | null;
    isAuthenticated: boolean;
    login: (accessToken: string, refreshToken: string, user: User) => void;
    logout: () => void;
    setUser: (user: User) => void;
}

export const useAuthStore = create<AuthState>((set) => ({
    user: null,
    accessToken: typeof window !== "undefined" ? localStorage.getItem("access_token") : null,
    isAuthenticated: typeof window !== "undefined" ? !!localStorage.getItem("access_token") : false,

    login: (accessToken: string, refreshToken: string, user: User) => {
        localStorage.setItem("access_token", accessToken);
        localStorage.setItem("refresh_token", refreshToken);
        set({ user, accessToken, isAuthenticated: true });
    },

    logout: () => {
        localStorage.removeItem("access_token");
        localStorage.removeItem("refresh_token");
        set({ user: null, accessToken: null, isAuthenticated: false });
    },

    setUser: (user: User) => set({ user }),
}));
