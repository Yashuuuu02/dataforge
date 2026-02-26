"use client";

import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Database, PlayCircle, GitBranch, TrendingUp, Activity, Zap } from "lucide-react";

const stats = [
    {
        title: "Total Datasets",
        value: "0",
        description: "Uploaded & processed",
        icon: Database,
        color: "text-blue-500",
        bgColor: "bg-blue-500/10",
    },
    {
        title: "Jobs Run",
        value: "0",
        description: "Processing tasks completed",
        icon: PlayCircle,
        color: "text-emerald-500",
        bgColor: "bg-emerald-500/10",
    },
    {
        title: "Workflows Saved",
        value: "0",
        description: "Reusable pipelines",
        icon: GitBranch,
        color: "text-purple-500",
        bgColor: "bg-purple-500/10",
    },
    {
        title: "Data Processed",
        value: "0 MB",
        description: "Total data throughput",
        icon: TrendingUp,
        color: "text-amber-500",
        bgColor: "bg-amber-500/10",
    },
];

export default function DashboardPage() {
    return (
        <div className="space-y-8">
            {/* Header */}
            <div>
                <h1 className="text-3xl font-bold tracking-tight">Dashboard</h1>
                <p className="mt-2 text-muted-foreground">
                    Overview of your data preparation workspace
                </p>
            </div>

            {/* Stats Grid */}
            <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-4">
                {stats.map((stat) => (
                    <Card key={stat.title} className="relative overflow-hidden">
                        <div className="absolute right-0 top-0 h-24 w-24 translate-x-8 -translate-y-4 rounded-full bg-gradient-to-br from-primary/5 to-primary/10" />
                        <CardHeader className="flex flex-row items-center justify-between pb-2">
                            <CardTitle className="text-sm font-medium text-muted-foreground">
                                {stat.title}
                            </CardTitle>
                            <div className={`rounded-lg p-2 ${stat.bgColor}`}>
                                <stat.icon className={`h-4 w-4 ${stat.color}`} />
                            </div>
                        </CardHeader>
                        <CardContent>
                            <div className="text-2xl font-bold">{stat.value}</div>
                            <p className="mt-1 text-xs text-muted-foreground">{stat.description}</p>
                        </CardContent>
                    </Card>
                ))}
            </div>

            {/* Charts placeholder */}
            <div className="grid gap-4 md:grid-cols-2">
                <Card>
                    <CardHeader>
                        <CardTitle className="flex items-center gap-2 text-lg">
                            <Activity className="h-5 w-5 text-primary" />
                            Processing Activity
                        </CardTitle>
                    </CardHeader>
                    <CardContent>
                        <div className="flex h-48 items-center justify-center rounded-lg border border-dashed border-border">
                            <div className="text-center">
                                <Activity className="mx-auto h-10 w-10 text-muted-foreground/40" />
                                <p className="mt-2 text-sm text-muted-foreground">
                                    Processing activity will appear here
                                </p>
                            </div>
                        </div>
                    </CardContent>
                </Card>

                <Card>
                    <CardHeader>
                        <CardTitle className="flex items-center gap-2 text-lg">
                            <Zap className="h-5 w-5 text-amber-500" />
                            Recent Jobs
                        </CardTitle>
                    </CardHeader>
                    <CardContent>
                        <div className="flex h-48 items-center justify-center rounded-lg border border-dashed border-border">
                            <div className="text-center">
                                <Zap className="mx-auto h-10 w-10 text-muted-foreground/40" />
                                <p className="mt-2 text-sm text-muted-foreground">
                                    No jobs have been run yet
                                </p>
                            </div>
                        </div>
                    </CardContent>
                </Card>
            </div>
        </div>
    );
}
