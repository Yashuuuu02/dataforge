"use client";

import { useState, useEffect, useCallback } from "react";
import { useParams, useRouter } from "next/navigation";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
    ArrowLeft,
    Loader2,
    CheckCircle2,
    XCircle,
    Clock,
    AlertCircle,
    Download,
    BarChart3,
    ChevronDown,
    ChevronUp,
    SkipForward,
} from "lucide-react";
import api from "@/lib/api";

type JobDetail = {
    id: string;
    dataset_id: string;
    mode: string;
    status: string;
    progress: number;
    started_at: string | null;
    completed_at: string | null;
    error_message: string | null;
    config: Record<string, any> | null;
    workflow_steps: any[] | null;
};

type StepInfo = {
    step: string;
    rows_before: number;
    rows_after: number;
    rows_removed: number;
    metadata: Record<string, any>;
    warnings: string[];
};

type JobResult = {
    total_rows_before: number;
    total_rows_after: number;
    total_rows_removed: number;
    duration_seconds: number;
    steps: StepInfo[];
    warnings: string[];
    download_url: string | null;
};

const STEP_NAMES: Record<string, string> = {
    deduplication: "Deduplication",
    pii_scrubbing: "PII Scrubbing",
    noise_removal: "Noise Removal",
    language_filter: "Language Filter",
    quality_scorer: "Quality Scorer",
};

const STEP_ICONS: Record<string, string> = {
    deduplication: "🔁",
    pii_scrubbing: "🔒",
    noise_removal: "🧹",
    language_filter: "🌐",
    quality_scorer: "⭐",
};

export default function JobDetailPage() {
    const params = useParams();
    const router = useRouter();
    const jobId = params.id as string;

    const [job, setJob] = useState<JobDetail | null>(null);
    const [result, setResult] = useState<JobResult | null>(null);
    const [loading, setLoading] = useState(true);
    const [expandedSteps, setExpandedSteps] = useState<Set<number>>(new Set());
    const [wsStep, setWsStep] = useState<string>("");
    const [wsMessage, setWsMessage] = useState<string>("");
    const [wsProgress, setWsProgress] = useState<number>(0);

    const fetchData = useCallback(async () => {
        try {
            const jobRes = await api.get(`/api/jobs/${jobId}`);
            setJob(jobRes.data);
            if (jobRes.data.status === "completed" || jobRes.data.status === "failed") {
                try {
                    const resultRes = await api.get(`/api/jobs/${jobId}/result`);
                    setResult(resultRes.data);
                } catch { }
            }
        } catch {
        } finally {
            setLoading(false);
        }
    }, [jobId]);

    useEffect(() => { fetchData(); }, [fetchData]);

    // WebSocket for live progress
    useEffect(() => {
        if (!job || (job.status !== "running" && job.status !== "queued")) return;
        const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
        const ws = new WebSocket(`${protocol}//${window.location.host}/api/ws/job/${jobId}`);
        ws.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                setWsStep(data.step || "");
                setWsMessage(data.message || "");
                setWsProgress(data.progress || 0);
                if (data.status === "completed" || data.status === "failed") {
                    fetchData();
                }
            } catch { }
        };
        return () => ws.close();
    }, [job?.status, jobId, fetchData]);

    const toggleStep = (idx: number) => {
        setExpandedSteps((prev) => {
            const next = new Set(prev);
            next.has(idx) ? next.delete(idx) : next.add(idx);
            return next;
        });
    };

    if (loading) return <div className="flex items-center justify-center py-20"><Loader2 className="h-8 w-8 animate-spin text-muted-foreground" /></div>;
    if (!job) return <div className="text-center py-20"><AlertCircle className="h-10 w-10 text-red-400 mx-auto" /><p className="mt-4 text-muted-foreground">Job not found</p></div>;

    const pipelineResult = job.config?.pipeline_result || result;
    const steps: StepInfo[] = pipelineResult?.steps || [];

    return (
        <div className="space-y-6">
            {/* Header */}
            <div className="flex items-center gap-3">
                <Button variant="ghost" size="icon" onClick={() => router.push("/jobs")}>
                    <ArrowLeft className="h-5 w-5" />
                </Button>
                <div>
                    <h1 className="text-2xl font-bold tracking-tight">Job {job.id.slice(0, 8)}…</h1>
                    <p className="text-sm text-muted-foreground capitalize">{job.mode} pipeline · {job.status}</p>
                </div>
            </div>

            {/* Live progress */}
            {(job.status === "running" || job.status === "queued") && (
                <Card className="border-blue-800/50 bg-blue-950/20">
                    <CardContent className="py-4">
                        <div className="flex items-center gap-3">
                            <Loader2 className="h-5 w-5 animate-spin text-blue-400 shrink-0" />
                            <div className="flex-1 min-w-0">
                                <p className="font-medium text-blue-300 truncate">
                                    {wsStep ? `Running: ${STEP_NAMES[wsStep] || wsStep}` : (job.status === "queued" ? "Queued..." : "Processing...")}
                                </p>
                                {wsMessage && <p className="text-sm text-blue-400/80 truncate">{wsMessage}</p>}
                            </div>
                            <div className="w-32 shrink-0">
                                <div className="h-2 rounded-full bg-blue-900/50 overflow-hidden">
                                    <div className="h-full rounded-full bg-blue-500 transition-all duration-500" style={{ width: `${wsProgress || job.progress}%` }} />
                                </div>
                                <p className="mt-1 text-xs text-blue-400 text-right tabular-nums">{wsProgress || job.progress}%</p>
                            </div>
                        </div>
                    </CardContent>
                </Card>
            )}

            {/* Error */}
            {job.status === "failed" && (
                <Card className="border-red-800/50 bg-red-950/20">
                    <CardContent className="py-4 flex items-start gap-3">
                        <AlertCircle className="h-5 w-5 text-red-400 mt-0.5" />
                        <div>
                            <p className="font-medium text-red-300">Pipeline Failed</p>
                            <p className="text-sm text-red-400/80">{job.error_message || "Unknown error"}</p>
                        </div>
                    </CardContent>
                </Card>
            )}

            {/* Result summary cards */}
            {job.status === "completed" && pipelineResult && (
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                    <Card className="bg-card/50 border-border/50">
                        <CardContent className="py-4">
                            <p className="text-xs text-muted-foreground uppercase tracking-wider flex items-center gap-1"><BarChart3 className="h-3 w-3" /> Before</p>
                            <p className="mt-1 text-2xl font-bold tabular-nums">{pipelineResult.total_rows_before?.toLocaleString()}</p>
                        </CardContent>
                    </Card>
                    <Card className="bg-card/50 border-border/50">
                        <CardContent className="py-4">
                            <p className="text-xs text-muted-foreground uppercase tracking-wider">After</p>
                            <p className="mt-1 text-2xl font-bold tabular-nums text-emerald-400">{pipelineResult.total_rows_after?.toLocaleString()}</p>
                        </CardContent>
                    </Card>
                    <Card className="bg-card/50 border-border/50">
                        <CardContent className="py-4">
                            <p className="text-xs text-muted-foreground uppercase tracking-wider">Removed</p>
                            <p className="mt-1 text-2xl font-bold tabular-nums text-red-400">{pipelineResult.total_rows_removed?.toLocaleString()}</p>
                        </CardContent>
                    </Card>
                    <Card className="bg-card/50 border-border/50">
                        <CardContent className="py-4">
                            <p className="text-xs text-muted-foreground uppercase tracking-wider">Duration</p>
                            <p className="mt-1 text-2xl font-bold">{pipelineResult.duration_seconds?.toFixed(1)}s</p>
                        </CardContent>
                    </Card>
                </div>
            )}

            {/* Download button */}
            {job.status === "completed" && result?.download_url && (
                <div className="flex gap-3">
                    <a href={result.download_url} target="_blank" rel="noopener noreferrer">
                        <Button className="gap-2"><Download className="h-4 w-4" /> Download Processed Dataset</Button>
                    </a>
                    <Button variant="outline" onClick={() => router.push(`/datasets/${job.dataset_id}`)}>View Original Dataset</Button>
                </div>
            )}

            {/* Step timeline */}
            {steps.length > 0 && (
                <Card className="border-border/50 bg-card/50">
                    <CardHeader><CardTitle className="text-lg">Pipeline Steps</CardTitle></CardHeader>
                    <CardContent className="space-y-3">
                        {steps.map((step, idx) => {
                            const isSkipped = step.metadata?.skipped;
                            const isExpanded = expandedSteps.has(idx);
                            return (
                                <div key={idx} className="rounded-lg border border-border/40 bg-zinc-900/30 overflow-hidden">
                                    <button
                                        onClick={() => toggleStep(idx)}
                                        className="w-full flex items-center gap-3 px-4 py-3 text-left hover:bg-zinc-800/30 transition-colors"
                                    >
                                        <span className="text-lg">{STEP_ICONS[step.step] || "⚙️"}</span>
                                        <div className="flex-1 min-w-0">
                                            <p className="font-medium">{STEP_NAMES[step.step] || step.step}</p>
                                            <p className="text-xs text-muted-foreground">
                                                {isSkipped ? "Skipped" : `${step.rows_before.toLocaleString()} → ${step.rows_after.toLocaleString()} rows (−${step.rows_removed.toLocaleString()})`}
                                            </p>
                                        </div>
                                        {isSkipped ? (
                                            <SkipForward className="h-4 w-4 text-yellow-500" />
                                        ) : (
                                            <CheckCircle2 className="h-4 w-4 text-emerald-400" />
                                        )}
                                        {isExpanded ? <ChevronUp className="h-4 w-4 text-muted-foreground" /> : <ChevronDown className="h-4 w-4 text-muted-foreground" />}
                                    </button>
                                    {isExpanded && (
                                        <div className="px-4 pb-3 border-t border-border/30 pt-3">
                                            {step.warnings.length > 0 && (
                                                <div className="mb-2 rounded border border-yellow-800/50 bg-yellow-950/20 p-2 text-xs text-yellow-400">
                                                    {step.warnings.map((w, i) => <p key={i}>{w}</p>)}
                                                </div>
                                            )}
                                            <div className="grid grid-cols-2 md:grid-cols-3 gap-3 text-sm">
                                                {Object.entries(step.metadata).filter(([k]) => k !== "skipped" && k !== "reason").map(([key, value]) => (
                                                    <div key={key}>
                                                        <p className="text-xs text-muted-foreground">{key.replace(/_/g, " ")}</p>
                                                        <p className="font-medium text-xs">
                                                            {typeof value === "object" ? JSON.stringify(value) : String(value)}
                                                        </p>
                                                    </div>
                                                ))}
                                            </div>
                                        </div>
                                    )}
                                </div>
                            );
                        })}
                    </CardContent>
                </Card>
            )}

            {/* Warnings */}
            {pipelineResult?.warnings?.length > 0 && (
                <Card className="border-yellow-800/50 bg-yellow-950/20">
                    <CardContent className="py-4">
                        <p className="text-sm font-medium text-yellow-300 mb-2">Warnings</p>
                        {pipelineResult.warnings.map((w: string, i: number) => (
                            <p key={i} className="text-xs text-yellow-400/80">{w}</p>
                        ))}
                    </CardContent>
                </Card>
            )}
        </div>
    );
}
