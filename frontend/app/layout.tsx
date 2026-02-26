import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "./globals.css";
import { Providers } from "@/lib/providers";
import { Sidebar } from "@/components/sidebar";

const inter = Inter({ subsets: ["latin"] });

export const metadata: Metadata = {
    title: "DataForge — AI Data Preparation Platform",
    description:
        "Open-source AI-agent-driven data preparation platform for fine-tuning LLMs, RAG pipelines, and ML model training.",
};

export default function RootLayout({
    children,
}: {
    children: React.ReactNode;
}) {
    return (
        <html lang="en" className="dark">
            <body className={inter.className}>
                <Providers>
                    <div className="flex h-screen overflow-hidden">
                        <Sidebar />
                        <main className="flex-1 overflow-y-auto bg-background">
                            <div className="mx-auto max-w-7xl px-6 py-8">{children}</div>
                        </main>
                    </div>
                </Providers>
            </body>
        </html>
    );
}
