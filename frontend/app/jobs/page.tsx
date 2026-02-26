"use client";

import { Card, CardContent } from "@/components/ui/card";
import { PlayCircle, Clock, CheckCircle2, XCircle } from "lucide-react";

export default function JobsPage() {
    return (
        <div className="space-y-8">
            {/* Header */}
            <div>
                <h1 className="text-3xl font-bold tracking-tight">Jobs</h1>
                <p className="mt-2 text-muted-foreground">
                    Monitor your data processing and transformation jobs
                </p>
            </div>

            {/* Stats row */}
            <div className="grid grid-cols-4 gap-4">
                {[
                    { label: "Queued", value: 0, icon: Clock, color: "text-amber-500" },
                    { label: "Running", value: 0, icon: PlayCircle, color: "text-blue-500" },
                    { label: "Completed", value: 0, icon: CheckCircle2, color: "text-emerald-500" },
                    { label: "Failed", value: 0, icon: XCircle, color: "text-red-500" },
                ].map((stat) => (
                    <Card key={stat.label}>
                        <CardContent className="flex items-center gap-3 p-4">
                            <stat.icon className={`h-5 w-5 ${stat.color}`} />
                            <div>
                                <p className="text-2xl font-bold">{stat.value}</p>
                                <p className="text-xs text-muted-foreground">{stat.label}</p>
                            </div>
                        </CardContent>
                    </Card>
                ))}
            </div>

            {/* Empty state */}
            <Card className="border-dashed">
                <CardContent className="flex flex-col items-center justify-center py-16">
                    <div className="rounded-full bg-primary/10 p-4">
                        <PlayCircle className="h-10 w-10 text-primary" />
                    </div>
                    <h3 className="mt-4 text-lg font-semibold">No jobs yet</h3>
                    <p className="mt-2 max-w-sm text-center text-sm text-muted-foreground">
                        Jobs are created when you process datasets. Upload a dataset and start a processing pipeline to see jobs here.
                    </p>
                </CardContent>
            </Card>
        </div>
    );
}
