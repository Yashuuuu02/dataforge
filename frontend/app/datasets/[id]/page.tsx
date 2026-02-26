"use client";

import { useState, useEffect, useCallback } from "react";
import { useParams, useRouter } from "next/navigation";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
    ArrowLeft,
    Loader2,
    BarChart3,
    Columns3,
    Hash,
    FileText,
    Globe2,
    AlertCircle,
    CheckCircle2,
    Play,
} from "lucide-react";
import api from "@/lib/api";

type DatasetDetail = {
    id: string;
    name: string;
    detected_format: string | null;
    row_count: number | null;
    column_count: number | null;
    size_bytes: number | null;
    status: string;
    created_at: string;
    error_message: string | null;
    source_type: string | null;
};

type ColumnStat = {
    name: string;
    dtype: string;
    null_count: number;
    null_percentage: number;
    unique_count: number;
    sample_values: any[];
    min?: number | null;
    max?: number | null;
    mean?: number | null;
    std?: number | null;
};

type StatsData = {
    dataset_id: string;
    row_count: number;
    column_count: number;
    size_bytes: number;
    estimated_tokens: number | null;
    detected_language: string | null;
    columns: ColumnStat[];
};

type PreviewData = {
    columns: string[];
    dtypes: Record<string, string>;
    row_count: number;
    rows: Record<string, any>[];
};

function formatBytes(bytes: number | null): string {
    if (!bytes) return "—";
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1048576) return `${(bytes / 1024).toFixed(1)} KB`;
    if (bytes < 1073741824) return `${(bytes / 1048576).toFixed(1)} MB`;
    return `${(bytes / 1073741824).toFixed(2)} GB`;
}

export default function DatasetDetailPage() {
    const params = useParams();
    const router = useRouter();
    const datasetId = params.id as string;

    const [dataset, setDataset] = useState<DatasetDetail | null>(null);
    const [stats, setStats] = useState<StatsData | null>(null);
    const [preview, setPreview] = useState<PreviewData | null>(null);
    const [loading, setLoading] = useState(true);
    const [activeView, setActiveView] = useState<"preview" | "stats">("preview");
    const [wsStatus, setWsStatus] = useState<string | null>(null);
    const [wsProgress, setWsProgress] = useState<number | null>(null);

    const fetchData = useCallback(async () => {
        try {
            const dsRes = await api.get(`/api/datasets/${datasetId}`);
            setDataset(dsRes.data);

            if (dsRes.data.status === "ready") {
                const [previewRes, statsRes] = await Promise.all([
                    api.get(`/api/ingestion/datasets/${datasetId}/preview`),
                    api.get(`/api/ingestion/datasets/${datasetId}/stats`),
                ]);
                setPreview(previewRes.data);
                setStats(statsRes.data);
            }
        } catch {
            // handle error
        } finally {
            setLoading(false);
        }
    }, [datasetId]);

    useEffect(() => {
        fetchData();
    }, [fetchData]);

    // WebSocket for processing progress
    useEffect(() => {
        if (!dataset || dataset.status !== "processing") return;

        const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
        const wsUrl = `${protocol}//${window.location.host}/api/ws/ingestion/${datasetId}`;
        const ws = new WebSocket(wsUrl);

        ws.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                setWsProgress(data.progress);
                setWsStatus(data.message);
                if (data.status === "ready" || data.status === "failed") {
                    fetchData();
                }
            } catch { }
        };

        return () => ws.close();
    }, [dataset?.status, datasetId, fetchData]);

    if (loading) {
        return (
            <div className="flex items-center justify-center py-20">
                <Loader2 className="h-8 w-8 animate-spin text-muted-foreground" />
            </div>
        );
    }

    if (!dataset) {
        return (
            <div className="text-center py-20">
                <AlertCircle className="h-10 w-10 text-red-400 mx-auto" />
                <p className="mt-4 text-lg text-muted-foreground">Dataset not found</p>
                <Button variant="outline" className="mt-4" onClick={() => router.push("/datasets")}>
                    <ArrowLeft className="mr-2 h-4 w-4" /> Back to Datasets
                </Button>
            </div>
        );
    }

    return (
        <div className="space-y-6">
            {/* Header */}
            <div className="flex items-center gap-3">
                <Button variant="ghost" size="icon" onClick={() => router.push("/datasets")}>
                    <ArrowLeft className="h-5 w-5" />
                </Button>
                <div>
                    <h1 className="text-2xl font-bold tracking-tight">{dataset.name}</h1>
                    <p className="text-sm text-muted-foreground">
                        {dataset.detected_format?.toUpperCase()} · {formatBytes(dataset.size_bytes)} · {dataset.source_type || "Upload"}
                    </p>
                </div>
            </div>

            {/* Processing indicator */}
            {(dataset.status === "processing" || dataset.status === "pending") && (
                <Card className="border-blue-800/50 bg-blue-950/20">
                    <CardContent className="py-4">
                        <div className="flex items-center gap-3">
                            <Loader2 className="h-5 w-5 animate-spin text-blue-400" />
                            <div className="flex-1">
                                <p className="font-medium text-blue-300">
                                    {dataset.status === "pending" ? "Queued for processing..." : "Processing..."}
                                </p>
                                {wsStatus && <p className="text-sm text-blue-400/80">{wsStatus}</p>}
                            </div>
                            {wsProgress !== null && (
                                <div className="w-32">
                                    <div className="h-2 rounded-full bg-blue-900/50 overflow-hidden">
                                        <div
                                            className="h-full rounded-full bg-blue-500 transition-all duration-500"
                                            style={{ width: `${wsProgress}%` }}
                                        />
                                    </div>
                                    <p className="mt-1 text-xs text-blue-400 text-right">{wsProgress}%</p>
                                </div>
                            )}
                        </div>
                    </CardContent>
                </Card>
            )}

            {/* Error state */}
            {dataset.status === "failed" && (
                <Card className="border-red-800/50 bg-red-950/20">
                    <CardContent className="py-4 flex items-start gap-3">
                        <AlertCircle className="h-5 w-5 text-red-400 mt-0.5" />
                        <div>
                            <p className="font-medium text-red-300">Ingestion Failed</p>
                            <p className="text-sm text-red-400/80">{dataset.error_message || "Unknown error"}</p>
                        </div>
                    </CardContent>
                </Card>
            )}

            {/* Stats Cards */}
            {dataset.status === "ready" && (
                <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
                    <Card className="bg-card/50 border-border/50">
                        <CardContent className="py-4">
                            <div className="flex items-center gap-2 text-muted-foreground text-xs font-medium uppercase tracking-wider">
                                <BarChart3 className="h-3.5 w-3.5" />
                                Rows
                            </div>
                            <p className="mt-1 text-2xl font-bold tabular-nums">{dataset.row_count?.toLocaleString() || "—"}</p>
                        </CardContent>
                    </Card>
                    <Card className="bg-card/50 border-border/50">
                        <CardContent className="py-4">
                            <div className="flex items-center gap-2 text-muted-foreground text-xs font-medium uppercase tracking-wider">
                                <Columns3 className="h-3.5 w-3.5" />
                                Columns
                            </div>
                            <p className="mt-1 text-2xl font-bold tabular-nums">{dataset.column_count || "—"}</p>
                        </CardContent>
                    </Card>
                    <Card className="bg-card/50 border-border/50">
                        <CardContent className="py-4">
                            <div className="flex items-center gap-2 text-muted-foreground text-xs font-medium uppercase tracking-wider">
                                <Hash className="h-3.5 w-3.5" />
                                Tokens
                            </div>
                            <p className="mt-1 text-2xl font-bold tabular-nums">{stats?.estimated_tokens?.toLocaleString() || "—"}</p>
                        </CardContent>
                    </Card>
                    <Card className="bg-card/50 border-border/50">
                        <CardContent className="py-4">
                            <div className="flex items-center gap-2 text-muted-foreground text-xs font-medium uppercase tracking-wider">
                                <Globe2 className="h-3.5 w-3.5" />
                                Language
                            </div>
                            <p className="mt-1 text-2xl font-bold">{stats?.detected_language?.toUpperCase() || "—"}</p>
                        </CardContent>
                    </Card>
                    <Card className="bg-card/50 border-border/50">
                        <CardContent className="py-4">
                            <div className="flex items-center gap-2 text-muted-foreground text-xs font-medium uppercase tracking-wider">
                                <FileText className="h-3.5 w-3.5" />
                                Size
                            </div>
                            <p className="mt-1 text-2xl font-bold">{formatBytes(dataset.size_bytes)}</p>
                        </CardContent>
                    </Card>
                </div>
            )}

            {/* View Tabs + Action Button */}
            {dataset.status === "ready" && (
                <div className="flex items-center justify-between">
                    <div className="flex gap-1 rounded-lg bg-zinc-900/50 p-1">
                        <button
                            onClick={() => setActiveView("preview")}
                            className={`px-3 py-1.5 rounded-md text-sm font-medium transition-colors ${activeView === "preview" ? "bg-primary text-primary-foreground" : "text-muted-foreground hover:text-foreground"
                                }`}
                        >
                            Preview
                        </button>
                        <button
                            onClick={() => setActiveView("stats")}
                            className={`px-3 py-1.5 rounded-md text-sm font-medium transition-colors ${activeView === "stats" ? "bg-primary text-primary-foreground" : "text-muted-foreground hover:text-foreground"
                                }`}
                        >
                            Column Stats
                        </button>
                    </div>
                    <Button className="gap-2" onClick={() => router.push("/jobs")}>
                        <Play className="h-4 w-4" />
                        Start Processing
                    </Button>
                </div>
            )}

            {/* Data Preview Table */}
            {dataset.status === "ready" && activeView === "preview" && preview && (
                <Card className="border-border/50 bg-card/50 overflow-hidden">
                    <CardContent className="p-0">
                        <div className="overflow-x-auto">
                            <table className="w-full text-sm">
                                <thead>
                                    <tr className="border-b border-border/50 bg-zinc-900/30">
                                        {preview.columns.map((col) => (
                                            <th key={col} className="px-4 py-2.5 text-left font-medium text-muted-foreground whitespace-nowrap">
                                                <div>{col}</div>
                                                <div className="text-[10px] text-muted-foreground/60 font-normal">{preview.dtypes[col]}</div>
                                            </th>
                                        ))}
                                    </tr>
                                </thead>
                                <tbody>
                                    {preview.rows.map((row, i) => (
                                        <tr key={i} className="border-b border-border/20 hover:bg-zinc-900/20">
                                            {preview.columns.map((col) => (
                                                <td key={col} className="px-4 py-2 max-w-[300px] truncate text-zinc-300">
                                                    {row[col] !== null && row[col] !== undefined ? String(row[col]) : <span className="text-zinc-600 italic">null</span>}
                                                </td>
                                            ))}
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                        <div className="px-4 py-2 border-t border-border/30 text-xs text-muted-foreground">
                            Showing {preview.row_count} of {dataset.row_count?.toLocaleString()} rows
                        </div>
                    </CardContent>
                </Card>
            )}

            {/* Column Stats */}
            {dataset.status === "ready" && activeView === "stats" && stats && (
                <div className="grid gap-4 md:grid-cols-2">
                    {stats.columns.map((col) => (
                        <Card key={col.name} className="border-border/50 bg-card/50">
                            <CardContent className="py-4">
                                <div className="flex items-center justify-between mb-3">
                                    <h4 className="font-semibold">{col.name}</h4>
                                    <span className="text-xs bg-zinc-800 px-2 py-0.5 rounded text-muted-foreground">{col.dtype}</span>
                                </div>
                                <div className="grid grid-cols-3 gap-3 text-sm">
                                    <div>
                                        <p className="text-xs text-muted-foreground">Nulls</p>
                                        <p className="font-medium">{col.null_count} <span className="text-muted-foreground text-xs">({col.null_percentage}%)</span></p>
                                    </div>
                                    <div>
                                        <p className="text-xs text-muted-foreground">Unique</p>
                                        <p className="font-medium">{col.unique_count.toLocaleString()}</p>
                                    </div>
                                    {col.min !== null && col.min !== undefined && (
                                        <div>
                                            <p className="text-xs text-muted-foreground">Range</p>
                                            <p className="font-medium text-xs">{col.min?.toFixed(2)} — {col.max?.toFixed(2)}</p>
                                        </div>
                                    )}
                                    {col.mean !== null && col.mean !== undefined && (
                                        <div>
                                            <p className="text-xs text-muted-foreground">Mean</p>
                                            <p className="font-medium">{col.mean?.toFixed(2)}</p>
                                        </div>
                                    )}
                                    {col.std !== null && col.std !== undefined && (
                                        <div>
                                            <p className="text-xs text-muted-foreground">Std</p>
                                            <p className="font-medium">{col.std?.toFixed(2)}</p>
                                        </div>
                                    )}
                                </div>
                                {col.sample_values.length > 0 && (
                                    <div className="mt-3 border-t border-border/30 pt-2">
                                        <p className="text-xs text-muted-foreground mb-1">Sample values</p>
                                        <div className="flex flex-wrap gap-1">
                                            {col.sample_values.slice(0, 5).map((v, i) => (
                                                <span key={i} className="text-xs bg-zinc-900 border border-border/40 px-1.5 py-0.5 rounded max-w-[150px] truncate inline-block">
                                                    {String(v)}
                                                </span>
                                            ))}
                                        </div>
                                    </div>
                                )}
                            </CardContent>
                        </Card>
                    ))}
                </div>
            )}
        </div>
    );
}
