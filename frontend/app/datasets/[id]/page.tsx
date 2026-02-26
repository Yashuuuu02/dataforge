"use client";

import { useState, useEffect, useCallback, useMemo } from "react";
import { useParams, useRouter } from "next/navigation";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import {
    ArrowLeft, Loader2, BarChart3, Columns3, Hash, FileText, Globe2,
    AlertCircle, CheckCircle2, Play, X, GripVertical, ChevronDown, ChevronUp, Settings2,
} from "lucide-react";
import { DndContext, closestCenter, KeyboardSensor, PointerSensor, useSensor, useSensors, DragEndEvent } from "@dnd-kit/core";
import { arrayMove, SortableContext, sortableKeyboardCoordinates, useSortable, verticalListSortingStrategy } from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import api from "@/lib/api";

// ── Types ──────────────────────────────────────────────
type DatasetDetail = { id: string; name: string; detected_format: string | null; row_count: number | null; column_count: number | null; size_bytes: number | null; status: string; created_at: string; error_message: string | null; source_type: string | null; };
type ColumnStat = { name: string; dtype: string; null_count: number; null_percentage: number; unique_count: number; sample_values: any[]; min?: number | null; max?: number | null; mean?: number | null; std?: number | null; };
type StatsData = { dataset_id: string; row_count: number; column_count: number; size_bytes: number; estimated_tokens: number | null; detected_language: string | null; columns: ColumnStat[]; };
type PreviewData = { columns: string[]; dtypes: Record<string, string>; row_count: number; rows: Record<string, any>[]; };

type PipelineStepDef = {
    id: string;
    step: string;
    label: string;
    icon: string;
    description: string;
    enabled: boolean;
    config: Record<string, any>;
    configFields: ConfigField[];
};

type ConfigField = {
    key: string;
    label: string;
    type: "text" | "select" | "number" | "boolean" | "multitext";
    options?: { value: string; label: string }[];
    default: any;
};

function formatBytes(bytes: number | null): string {
    if (!bytes) return "—";
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1048576) return `${(bytes / 1024).toFixed(1)} KB`;
    if (bytes < 1073741824) return `${(bytes / 1048576).toFixed(1)} MB`;
    return `${(bytes / 1073741824).toFixed(2)} GB`;
}

// ── Default pipeline steps ──────────────────────────────
const DEFAULT_STEPS: PipelineStepDef[] = [
    {
        id: "dedup", step: "deduplication", label: "Deduplication", icon: "🔁",
        description: "Remove duplicate rows using exact hash or semantic similarity",
        enabled: true,
        config: { method: "exact", columns: "all", keep: "first", semantic_threshold: 0.95 },
        configFields: [
            { key: "method", label: "Method", type: "select", options: [{ value: "exact", label: "Exact (SHA256)" }, { value: "semantic", label: "Semantic" }, { value: "both", label: "Both" }], default: "exact" },
            { key: "columns", label: "Columns", type: "text", default: "all" },
            { key: "keep", label: "Keep", type: "select", options: [{ value: "first", label: "First" }, { value: "last", label: "Last" }], default: "first" },
            { key: "semantic_threshold", label: "Similarity Threshold", type: "number", default: 0.95 },
        ],
    },
    {
        id: "noise", step: "noise_removal", label: "Noise Removal", icon: "🧹",
        description: "Fix encoding, strip HTML, normalize whitespace and unicode",
        enabled: true,
        config: { fix_encoding: true, strip_html: true, normalize_whitespace: true, remove_control_chars: true, normalize_unicode: true, strip_urls: false, min_text_length: 0, max_text_length: 0 },
        configFields: [
            { key: "fix_encoding", label: "Fix Encoding (ftfy)", type: "boolean", default: true },
            { key: "strip_html", label: "Strip HTML", type: "boolean", default: true },
            { key: "normalize_whitespace", label: "Normalize Whitespace", type: "boolean", default: true },
            { key: "normalize_unicode", label: "Normalize Unicode", type: "boolean", default: true },
            { key: "strip_urls", label: "Strip URLs", type: "boolean", default: false },
            { key: "min_text_length", label: "Min Text Length", type: "number", default: 0 },
            { key: "max_text_length", label: "Max Text Length (0=none)", type: "number", default: 0 },
        ],
    },
    {
        id: "pii", step: "pii_scrubbing", label: "PII Scrubbing", icon: "🔒",
        description: "Detect and redact personally identifiable information",
        enabled: true,
        config: { action: "redact", entities: ["ALL"], redact_with: "[REDACTED]", columns: "all_text" },
        configFields: [
            { key: "action", label: "Action", type: "select", options: [{ value: "redact", label: "Redact" }, { value: "remove_row", label: "Remove Row" }, { value: "flag", label: "Flag Only" }], default: "redact" },
            { key: "redact_with", label: "Replace With", type: "text", default: "[REDACTED]" },
            { key: "columns", label: "Columns", type: "text", default: "all_text" },
        ],
    },
    {
        id: "lang", step: "language_filter", label: "Language Filter", icon: "🌐",
        description: "Detect language and filter/tag rows",
        enabled: false,
        config: { action: "tag_only", languages: ["en"], min_confidence: 0.8 },
        configFields: [
            { key: "action", label: "Action", type: "select", options: [{ value: "tag_only", label: "Tag Only" }, { value: "filter_keep", label: "Keep Languages" }, { value: "filter_remove", label: "Remove Languages" }], default: "tag_only" },
            { key: "languages", label: "Languages (comma-sep)", type: "text", default: "en" },
            { key: "min_confidence", label: "Min Confidence", type: "number", default: 0.8 },
        ],
    },
    {
        id: "quality", step: "quality_scorer", label: "Quality Scorer", icon: "⭐",
        description: "Score each row 0-10 for quality using heuristics or AI",
        enabled: true,
        config: { method: "heuristic", action: "score_only", threshold: 0, score_column_name: "quality_score" },
        configFields: [
            { key: "method", label: "Method", type: "select", options: [{ value: "heuristic", label: "Heuristic" }, { value: "ai", label: "AI (needs API key)" }, { value: "both", label: "Both" }], default: "heuristic" },
            { key: "action", label: "Action", type: "select", options: [{ value: "score_only", label: "Score Only" }, { value: "filter", label: "Filter Below Threshold" }, { value: "flag", label: "Flag Low Quality" }], default: "score_only" },
            { key: "threshold", label: "Score Threshold", type: "number", default: 0 },
        ],
    },
];

// ── Sortable Step Component ─────────────────────────────
function SortableStep({ step, onToggle, onConfigChange, expanded, onToggleExpand }: {
    step: PipelineStepDef; onToggle: () => void; onConfigChange: (key: string, value: any) => void; expanded: boolean; onToggleExpand: () => void;
}) {
    const { attributes, listeners, setNodeRef, transform, transition } = useSortable({ id: step.id });
    const style = { transform: CSS.Transform.toString(transform), transition };

    return (
        <div ref={setNodeRef} style={style} className={`rounded-lg border transition-colors ${step.enabled ? "border-primary/40 bg-zinc-900/40" : "border-border/30 bg-zinc-900/20 opacity-60"}`}>
            <div className="flex items-center gap-2 px-3 py-2.5">
                <button {...attributes} {...listeners} className="cursor-grab active:cursor-grabbing p-1 text-muted-foreground hover:text-foreground">
                    <GripVertical className="h-4 w-4" />
                </button>
                <span className="text-lg">{step.icon}</span>
                <div className="flex-1 min-w-0">
                    <p className="text-sm font-medium">{step.label}</p>
                    <p className="text-xs text-muted-foreground truncate">{step.description}</p>
                </div>
                <button onClick={onToggleExpand} className="p-1 text-muted-foreground hover:text-foreground">
                    {expanded ? <ChevronUp className="h-4 w-4" /> : <Settings2 className="h-4 w-4" />}
                </button>
                <button
                    onClick={onToggle}
                    className={`relative w-10 h-5 rounded-full transition-colors ${step.enabled ? "bg-primary" : "bg-zinc-700"}`}
                >
                    <span className={`absolute top-0.5 left-0.5 w-4 h-4 rounded-full bg-white transition-transform ${step.enabled ? "translate-x-5" : ""}`} />
                </button>
            </div>
            {expanded && step.enabled && (
                <div className="px-3 pb-3 border-t border-border/30 pt-2 grid gap-2">
                    {step.configFields.map((field) => (
                        <div key={field.key} className="flex items-center gap-2">
                            <label className="text-xs text-muted-foreground w-36 shrink-0">{field.label}</label>
                            {field.type === "select" ? (
                                <select
                                    value={step.config[field.key] ?? field.default}
                                    onChange={(e) => onConfigChange(field.key, e.target.value)}
                                    className="flex-1 rounded border border-border/50 bg-zinc-800/50 px-2 py-1 text-xs focus:outline-none focus:ring-1 focus:ring-primary/50"
                                >
                                    {field.options?.map((o) => <option key={o.value} value={o.value}>{o.label}</option>)}
                                </select>
                            ) : field.type === "boolean" ? (
                                <button
                                    onClick={() => onConfigChange(field.key, !step.config[field.key])}
                                    className={`w-8 h-4 rounded-full transition-colors ${step.config[field.key] ? "bg-emerald-600" : "bg-zinc-700"}`}
                                >
                                    <span className={`block w-3 h-3 rounded-full bg-white transition-transform ml-0.5 ${step.config[field.key] ? "translate-x-4" : ""}`} />
                                </button>
                            ) : (
                                <input
                                    type={field.type === "number" ? "number" : "text"}
                                    value={step.config[field.key] ?? field.default}
                                    onChange={(e) => onConfigChange(field.key, field.type === "number" ? parseFloat(e.target.value) || 0 : e.target.value)}
                                    className="flex-1 rounded border border-border/50 bg-zinc-800/50 px-2 py-1 text-xs focus:outline-none focus:ring-1 focus:ring-primary/50"
                                />
                            )}
                        </div>
                    ))}
                </div>
            )}
        </div>
    );
}

// ── Main Page ───────────────────────────────────────────
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

    // Pipeline config drawer
    const [drawerOpen, setDrawerOpen] = useState(false);
    const [pipelineSteps, setPipelineSteps] = useState<PipelineStepDef[]>(DEFAULT_STEPS);
    const [expandedStepId, setExpandedStepId] = useState<string | null>(null);
    const [submitting, setSubmitting] = useState(false);

    const sensors = useSensors(
        useSensor(PointerSensor),
        useSensor(KeyboardSensor, { coordinateGetter: sortableKeyboardCoordinates })
    );

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
        } catch { } finally { setLoading(false); }
    }, [datasetId]);

    useEffect(() => { fetchData(); }, [fetchData]);

    useEffect(() => {
        if (!dataset || dataset.status !== "processing") return;
        const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
        const ws = new WebSocket(`${protocol}//${window.location.host}/api/ws/ingestion/${datasetId}`);
        ws.onmessage = (event) => {
            try { const d = JSON.parse(event.data); setWsProgress(d.progress); setWsStatus(d.message); if (d.status === "ready" || d.status === "failed") fetchData(); } catch { }
        };
        return () => ws.close();
    }, [dataset?.status, datasetId, fetchData]);

    // ── Drawer handlers ──
    const handleDragEnd = (event: DragEndEvent) => {
        const { active, over } = event;
        if (over && active.id !== over.id) {
            setPipelineSteps((items) => {
                const oldIndex = items.findIndex((i) => i.id === active.id);
                const newIndex = items.findIndex((i) => i.id === over.id);
                return arrayMove(items, oldIndex, newIndex);
            });
        }
    };

    const toggleStep = (id: string) => {
        setPipelineSteps((prev) => prev.map((s) => s.id === id ? { ...s, enabled: !s.enabled } : s));
    };

    const updateStepConfig = (id: string, key: string, value: any) => {
        setPipelineSteps((prev) => prev.map((s) => s.id === id ? { ...s, config: { ...s.config, [key]: value } } : s));
    };

    const handleRunPipeline = async () => {
        const enabledSteps = pipelineSteps
            .filter((s) => s.enabled)
            .map((s) => ({
                step: s.step,
                config: s.config,
            }));

        if (enabledSteps.length === 0) return;

        setSubmitting(true);
        try {
            const res = await api.post("/api/jobs/", {
                dataset_id: datasetId,
                mode: "common",
                steps: enabledSteps,
            });
            setDrawerOpen(false);
            router.push(`/jobs/${res.data.id}`);
        } catch (err) {
            console.error("Failed to create job", err);
        } finally {
            setSubmitting(false);
        }
    };

    if (loading) return <div className="flex items-center justify-center py-20"><Loader2 className="h-8 w-8 animate-spin text-muted-foreground" /></div>;
    if (!dataset) return (
        <div className="text-center py-20">
            <AlertCircle className="h-10 w-10 text-red-400 mx-auto" />
            <p className="mt-4 text-lg text-muted-foreground">Dataset not found</p>
            <Button variant="outline" className="mt-4" onClick={() => router.push("/datasets")}><ArrowLeft className="mr-2 h-4 w-4" /> Back to Datasets</Button>
        </div>
    );

    const enabledCount = pipelineSteps.filter((s) => s.enabled).length;

    return (
        <div className="space-y-6">
            {/* Header */}
            <div className="flex items-center gap-3">
                <Button variant="ghost" size="icon" onClick={() => router.push("/datasets")}><ArrowLeft className="h-5 w-5" /></Button>
                <div>
                    <h1 className="text-2xl font-bold tracking-tight">{dataset.name}</h1>
                    <p className="text-sm text-muted-foreground">{dataset.detected_format?.toUpperCase()} · {formatBytes(dataset.size_bytes)} · {dataset.source_type || "Upload"}</p>
                </div>
            </div>

            {/* Processing / Error indicator */}
            {(dataset.status === "processing" || dataset.status === "pending") && (
                <Card className="border-blue-800/50 bg-blue-950/20"><CardContent className="py-4">
                    <div className="flex items-center gap-3">
                        <Loader2 className="h-5 w-5 animate-spin text-blue-400" />
                        <div className="flex-1"><p className="font-medium text-blue-300">{dataset.status === "pending" ? "Queued..." : "Processing..."}</p>{wsStatus && <p className="text-sm text-blue-400/80">{wsStatus}</p>}</div>
                        {wsProgress !== null && <div className="w-32"><div className="h-2 rounded-full bg-blue-900/50 overflow-hidden"><div className="h-full rounded-full bg-blue-500 transition-all duration-500" style={{ width: `${wsProgress}%` }} /></div><p className="mt-1 text-xs text-blue-400 text-right">{wsProgress}%</p></div>}
                    </div>
                </CardContent></Card>
            )}
            {dataset.status === "failed" && (
                <Card className="border-red-800/50 bg-red-950/20"><CardContent className="py-4 flex items-start gap-3"><AlertCircle className="h-5 w-5 text-red-400 mt-0.5" /><div><p className="font-medium text-red-300">Ingestion Failed</p><p className="text-sm text-red-400/80">{dataset.error_message || "Unknown error"}</p></div></CardContent></Card>
            )}

            {/* Stats Cards */}
            {dataset.status === "ready" && (
                <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
                    {[
                        { icon: <BarChart3 className="h-3.5 w-3.5" />, label: "Rows", value: dataset.row_count?.toLocaleString() || "—" },
                        { icon: <Columns3 className="h-3.5 w-3.5" />, label: "Columns", value: String(dataset.column_count || "—") },
                        { icon: <Hash className="h-3.5 w-3.5" />, label: "Tokens", value: stats?.estimated_tokens?.toLocaleString() || "—" },
                        { icon: <Globe2 className="h-3.5 w-3.5" />, label: "Language", value: stats?.detected_language?.toUpperCase() || "—" },
                        { icon: <FileText className="h-3.5 w-3.5" />, label: "Size", value: formatBytes(dataset.size_bytes) },
                    ].map((card, i) => (
                        <Card key={i} className="bg-card/50 border-border/50"><CardContent className="py-4">
                            <div className="flex items-center gap-2 text-muted-foreground text-xs font-medium uppercase tracking-wider">{card.icon}{card.label}</div>
                            <p className="mt-1 text-2xl font-bold tabular-nums">{card.value}</p>
                        </CardContent></Card>
                    ))}
                </div>
            )}

            {/* View Tabs + Action Button */}
            {dataset.status === "ready" && (
                <div className="flex items-center justify-between">
                    <div className="flex gap-1 rounded-lg bg-zinc-900/50 p-1">
                        {["preview", "stats"].map((v) => (
                            <button key={v} onClick={() => setActiveView(v as any)} className={`px-3 py-1.5 rounded-md text-sm font-medium transition-colors capitalize ${activeView === v ? "bg-primary text-primary-foreground" : "text-muted-foreground hover:text-foreground"}`}>
                                {v === "preview" ? "Preview" : "Column Stats"}
                            </button>
                        ))}
                    </div>
                    <Button className="gap-2" onClick={() => setDrawerOpen(true)}>
                        <Play className="h-4 w-4" /> Start Processing
                    </Button>
                </div>
            )}

            {/* Preview Table */}
            {dataset.status === "ready" && activeView === "preview" && preview && (
                <Card className="border-border/50 bg-card/50 overflow-hidden"><CardContent className="p-0">
                    <div className="overflow-x-auto">
                        <table className="w-full text-sm">
                            <thead><tr className="border-b border-border/50 bg-zinc-900/30">
                                {preview.columns.map((col) => (<th key={col} className="px-4 py-2.5 text-left font-medium text-muted-foreground whitespace-nowrap"><div>{col}</div><div className="text-[10px] text-muted-foreground/60 font-normal">{preview.dtypes[col]}</div></th>))}
                            </tr></thead>
                            <tbody>
                                {preview.rows.map((row, i) => (<tr key={i} className="border-b border-border/20 hover:bg-zinc-900/20">
                                    {preview.columns.map((col) => (<td key={col} className="px-4 py-2 max-w-[300px] truncate text-zinc-300">{row[col] !== null && row[col] !== undefined ? String(row[col]) : <span className="text-zinc-600 italic">null</span>}</td>))}
                                </tr>))}
                            </tbody>
                        </table>
                    </div>
                    <div className="px-4 py-2 border-t border-border/30 text-xs text-muted-foreground">Showing {preview.row_count} of {dataset.row_count?.toLocaleString()} rows</div>
                </CardContent></Card>
            )}

            {/* Column Stats */}
            {dataset.status === "ready" && activeView === "stats" && stats && (
                <div className="grid gap-4 md:grid-cols-2">
                    {stats.columns.map((col) => (
                        <Card key={col.name} className="border-border/50 bg-card/50"><CardContent className="py-4">
                            <div className="flex items-center justify-between mb-3"><h4 className="font-semibold">{col.name}</h4><span className="text-xs bg-zinc-800 px-2 py-0.5 rounded text-muted-foreground">{col.dtype}</span></div>
                            <div className="grid grid-cols-3 gap-3 text-sm">
                                <div><p className="text-xs text-muted-foreground">Nulls</p><p className="font-medium">{col.null_count} <span className="text-muted-foreground text-xs">({col.null_percentage}%)</span></p></div>
                                <div><p className="text-xs text-muted-foreground">Unique</p><p className="font-medium">{col.unique_count.toLocaleString()}</p></div>
                                {col.min !== null && col.min !== undefined && <div><p className="text-xs text-muted-foreground">Range</p><p className="font-medium text-xs">{col.min?.toFixed(2)} — {col.max?.toFixed(2)}</p></div>}
                                {col.mean !== null && col.mean !== undefined && <div><p className="text-xs text-muted-foreground">Mean</p><p className="font-medium">{col.mean?.toFixed(2)}</p></div>}
                                {col.std !== null && col.std !== undefined && <div><p className="text-xs text-muted-foreground">Std</p><p className="font-medium">{col.std?.toFixed(2)}</p></div>}
                            </div>
                            {col.sample_values.length > 0 && (
                                <div className="mt-3 border-t border-border/30 pt-2"><p className="text-xs text-muted-foreground mb-1">Sample values</p>
                                    <div className="flex flex-wrap gap-1">{col.sample_values.slice(0, 5).map((v, i) => (<span key={i} className="text-xs bg-zinc-900 border border-border/40 px-1.5 py-0.5 rounded max-w-[150px] truncate inline-block">{String(v)}</span>))}</div>
                                </div>
                            )}
                        </CardContent></Card>
                    ))}
                </div>
            )}

            {/* ── Pipeline Config Drawer (overlay) ── */}
            {drawerOpen && (
                <div className="fixed inset-0 z-50 flex justify-end">
                    <div className="absolute inset-0 bg-black/50 backdrop-blur-sm" onClick={() => setDrawerOpen(false)} />
                    <div className="relative w-full max-w-md bg-zinc-950 border-l border-border/50 shadow-2xl overflow-y-auto">
                        <div className="sticky top-0 z-10 bg-zinc-950/95 backdrop-blur border-b border-border/30 px-5 py-4 flex items-center justify-between">
                            <div>
                                <h2 className="text-lg font-bold">Configure Pipeline</h2>
                                <p className="text-xs text-muted-foreground mt-0.5">{enabledCount} steps enabled · Drag to reorder</p>
                            </div>
                            <button onClick={() => setDrawerOpen(false)} className="p-1 text-muted-foreground hover:text-foreground"><X className="h-5 w-5" /></button>
                        </div>

                        <div className="p-5 space-y-3">
                            <DndContext sensors={sensors} collisionDetection={closestCenter} onDragEnd={handleDragEnd}>
                                <SortableContext items={pipelineSteps.map((s) => s.id)} strategy={verticalListSortingStrategy}>
                                    {pipelineSteps.map((step) => (
                                        <SortableStep
                                            key={step.id}
                                            step={step}
                                            onToggle={() => toggleStep(step.id)}
                                            onConfigChange={(key, value) => updateStepConfig(step.id, key, value)}
                                            expanded={expandedStepId === step.id}
                                            onToggleExpand={() => setExpandedStepId(expandedStepId === step.id ? null : step.id)}
                                        />
                                    ))}
                                </SortableContext>
                            </DndContext>
                        </div>

                        <div className="sticky bottom-0 bg-zinc-950/95 backdrop-blur border-t border-border/30 px-5 py-4">
                            <Button className="w-full gap-2" size="lg" onClick={handleRunPipeline} disabled={submitting || enabledCount === 0}>
                                {submitting ? <Loader2 className="h-4 w-4 animate-spin" /> : <Play className="h-4 w-4" />}
                                Run {enabledCount} Step{enabledCount !== 1 ? "s" : ""}
                            </Button>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}
