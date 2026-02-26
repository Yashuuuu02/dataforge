"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { cn } from "@/lib/utils";
import {
    LayoutDashboard,
    Database,
    PlayCircle,
    Bot,
    GitBranch,
    Settings,
    Anvil,
} from "lucide-react";

const navigation = [
    { name: "Dashboard", href: "/dashboard", icon: LayoutDashboard },
    { name: "Datasets", href: "/datasets", icon: Database },
    { name: "Jobs", href: "/jobs", icon: PlayCircle },
    { name: "Agent", href: "/agent", icon: Bot },
    { name: "Workflows", href: "/workflows", icon: GitBranch },
    { name: "Fine-Tune", href: "/finetune", icon: Anvil },
    { name: "Settings", href: "/settings", icon: Settings },
];

export function Sidebar() {
    const pathname = usePathname();

    return (
        <aside className="flex h-screen w-64 flex-col border-r border-border bg-card">
            {/* Logo */}
            <div className="flex h-16 items-center gap-2 border-b border-border px-6">
                <div className="flex h-8 w-8 items-center justify-center rounded-lg bg-primary">
                    <Anvil className="h-5 w-5 text-primary-foreground" />
                </div>
                <span className="text-xl font-bold tracking-tight">
                    Data<span className="text-primary">Forge</span>
                </span>
            </div>

            {/* Navigation */}
            <nav className="flex-1 space-y-1 px-3 py-4">
                {navigation.map((item) => {
                    const isActive = pathname === item.href || pathname?.startsWith(item.href + "/");
                    return (
                        <Link
                            key={item.name}
                            href={item.href}
                            className={cn(
                                "flex items-center gap-3 rounded-lg px-3 py-2.5 text-sm font-medium transition-all duration-200",
                                isActive
                                    ? "bg-primary/10 text-primary shadow-sm"
                                    : "text-muted-foreground hover:bg-accent hover:text-foreground"
                            )}
                        >
                            <item.icon className={cn("h-5 w-5 shrink-0", isActive && "text-primary")} />
                            {item.name}
                            {isActive && (
                                <div className="ml-auto h-1.5 w-1.5 rounded-full bg-primary" />
                            )}
                        </Link>
                    );
                })}
            </nav>

            {/* Footer */}
            <div className="border-t border-border p-4">
                <div className="flex items-center gap-3 rounded-lg bg-accent/50 px-3 py-2">
                    <div className="flex h-8 w-8 items-center justify-center rounded-full bg-primary/20 text-xs font-semibold text-primary">
                        DF
                    </div>
                    <div className="flex-1 truncate">
                        <p className="text-sm font-medium">DataForge</p>
                        <p className="text-xs text-muted-foreground">v0.1.0 · Free Plan</p>
                    </div>
                </div>
            </div>
        </aside>
    );
}
