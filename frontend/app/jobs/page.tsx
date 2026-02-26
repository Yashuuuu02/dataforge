"use client";

import { useState, useEffect, useCallback } from "react";
import { useRouter } from "next/navigation";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
    Loader2,
    CheckCircle2,
    XCircle,
    Clock,
    Play,
    ChevronRight,
    Zap,
} from "lucide-react";
import api from "@/lib/api";

type JobItem = {
    id: string;
    dataset_id: string;
    mode: string;
    status: "queued" | "running" | "completed" | "failed";
    progress: number;
    started_at: string | null;
    completed_at: string | null;
    error_message: string | null;
    config: Record<string, any> | null;
    workflow_steps: any[] | null;
};

const STATUS_BADGE: Record<string, { className: string; icon: React.ReactNode; label: string }> = {
    queued: { className: "bg-zinc-700/60 text-zinc-300", icon: <Clock className="h-3 w-3" />, label: "Queued" },
    running: { className: "bg-blue-900/50 text-blue-400", icon: <Loader2 className="h-3 w-3 animate-spin" />, label: "Running" },
    completed: { className: "bg-emerald-900/50 text-emerald-400", icon: <CheckCircle2 className="h-3 w-3" />, label: "Completed" },
    failed: { className: "bg-red-900/50 text-red-400", icon: <XCircle className="h-3 w-3" />, label: "Failed" },
};

function formatDuration(start: string | null, end: string | null): string {
    if (!start) return "—";
    const s = new Date(start).getTime();
    const e = end ? new Date(end).getTime() : Date.now();
    const secs = Math.floor((e - s) / 1000);
    if (secs < 60) return `${secs}s`;
    if (secs < 3600) return `${Math.floor(secs / 60)}m ${secs % 60}s`;
    return `${Math.floor(secs / 3600)}h ${Math.floor((secs % 3600) / 60)}m`;
}

export default function JobsPage() {
    const router = useRouter();
    const [jobs, setJobs] = useState<JobItem[]>([]);
    const [loading, setLoading] = useState(true);

    const fetchJobs = useCallback(async () => {
        try {
            const res = await api.get("/api/jobs/");
            setJobs(res.data.jobs || []);
        } catch {
        } finally {
            setLoading(false);
        }
    }, []);

    useEffect(() => {
        fetchJobs();
        const interval = setInterval(fetchJobs, 3000);
        return () => clearInterval(interval);
    }, [fetchJobs]);

    return (
        <div className="space-y-8">
            <div>
                <h1 className="text-3xl font-bold tracking-tight">Jobs</h1>
                <p className="mt-2 text-muted-foreground">Monitor and manage your processing pipelines</p>
            </div>

            <Card className="border-border/50 bg-card/50 backdrop-blur">
                <CardHeader>
                    <CardTitle className="text-lg">Processing Jobs</CardTitle>
                </CardHeader>
                <CardContent>
                    {loading ? (
                        <div className="flex items-center justify-center py-12">
                            <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
                        </div>
                    ) : jobs.length === 0 ? (
                        <div className="flex flex-col items-center justify-center py-12">
                            <div className="rounded-full bg-primary/10 p-4">
                                <Zap className="h-10 w-10 text-primary" />
                            </div>
                            <h3 className="mt-4 text-lg font-semibold">No jobs yet</h3>
                            <p className="mt-2 max-w-sm text-center text-sm text-muted-foreground">
                                Go to a dataset and click &ldquo;Start Processing&rdquo; to run the common pipeline.
                            </p>
                        </div>
                    ) : (
                        <div className="overflow-x-auto">
                            <table className="w-full text-sm">
                                <thead>
                                    <tr className="border-b border-border/50 text-left text-muted-foreground">
                                        <th className="pb-3 pr-4 font-medium">Job ID</th>
                                        <th className="pb-3 pr-4 font-medium">Mode</th>
                                        <th className="pb-3 pr-4 font-medium">Status</th>
                                        <th className="pb-3 pr-4 font-medium">Progress</th>
                                        <th className="pb-3 pr-4 font-medium">Duration</th>
                                        <th className="pb-3 pr-4 font-medium">Steps</th>
                                        <th className="pb-3 font-medium"></th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {jobs.map((job) => {
                                        const badge = STATUS_BADGE[job.status] || STATUS_BADGE.queued;
                                        const stepsCount = job.workflow_steps?.length || 0;
                                        return (
                                            <tr
                                                key={job.id}
                                                onClick={() => router.push(`/jobs/${job.id}`)}
                                                className="cursor-pointer border-b border-border/30 transition-colors hover:bg-zinc-900/30"
                                            >
                                                <td className="py-3 pr-4 font-mono text-xs">{job.id.slice(0, 8)}…</td>
                                                <td className="py-3 pr-4 capitalize text-muted-foreground">{job.mode}</td>
                                                <td className="py-3 pr-4">
                                                    <span className={`inline-flex items-center gap-1.5 rounded-full px-2.5 py-0.5 text-xs font-medium ${badge.className}`}>
                                                        {badge.icon}
                                                        {badge.label}
                                                    </span>
                                                </td>
                                                <td className="py-3 pr-4">
                                                    <div className="flex items-center gap-2">
                                                        <div className="h-1.5 w-24 rounded-full bg-zinc-800 overflow-hidden">
                                                            <div
                                                                className="h-full rounded-full bg-primary transition-all duration-500"
                                                                style={{ width: `${job.progress}%` }}
                                                            />
                                                        </div>
                                                        <span className="text-xs text-muted-foreground tabular-nums">{job.progress}%</span>
                                                    </div>
                                                </td>
                                                <td className="py-3 pr-4 text-muted-foreground text-xs">
                                                    {formatDuration(job.started_at, job.completed_at)}
                                                </td>
                                                <td className="py-3 pr-4 text-muted-foreground tabular-nums">{stepsCount}</td>
                                                <td className="py-3">
                                                    <ChevronRight className="h-4 w-4 text-muted-foreground" />
                                                </td>
                                            </tr>
                                        );
                                    })}
                                </tbody>
                            </table>
                        </div>
                    )}
                </CardContent>
            </Card>
        </div>
    );
}
