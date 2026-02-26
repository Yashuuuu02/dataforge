"use client";

import { useState, useCallback, useEffect } from "react";
import { useRouter } from "next/navigation";
import { useDropzone } from "react-dropzone";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import {
    Database,
    Upload,
    FileSpreadsheet,
    Cloud,
    Globe,
    HardDrive,
    Loader2,
    CheckCircle2,
    XCircle,
    Clock,
    FileText,
    FileJson,
    Table2,
    File,
    ChevronRight,
} from "lucide-react";
import api from "@/lib/api";

type DatasetItem = {
    id: string;
    name: string;
    detected_format: string | null;
    row_count: number | null;
    column_count: number | null;
    size_bytes: number | null;
    status: "pending" | "processing" | "ready" | "failed";
    created_at: string;
    error_message: string | null;
};

const FORMAT_ICONS: Record<string, React.ReactNode> = {
    csv: <Table2 className="h-4 w-4 text-green-500" />,
    tsv: <Table2 className="h-4 w-4 text-green-500" />,
    json: <FileJson className="h-4 w-4 text-yellow-500" />,
    jsonl: <FileJson className="h-4 w-4 text-yellow-500" />,
    parquet: <Database className="h-4 w-4 text-blue-500" />,
    xlsx: <FileSpreadsheet className="h-4 w-4 text-emerald-500" />,
    pdf: <FileText className="h-4 w-4 text-red-500" />,
    txt: <FileText className="h-4 w-4 text-gray-400" />,
    md: <FileText className="h-4 w-4 text-gray-400" />,
    html: <Globe className="h-4 w-4 text-orange-500" />,
    docx: <FileText className="h-4 w-4 text-blue-400" />,
};

const STATUS_BADGE: Record<string, { className: string; icon: React.ReactNode; label: string }> = {
    pending: { className: "bg-zinc-700/60 text-zinc-300", icon: <Clock className="h-3 w-3" />, label: "Pending" },
    processing: { className: "bg-blue-900/50 text-blue-400", icon: <Loader2 className="h-3 w-3 animate-spin" />, label: "Processing" },
    ready: { className: "bg-emerald-900/50 text-emerald-400", icon: <CheckCircle2 className="h-3 w-3" />, label: "Ready" },
    failed: { className: "bg-red-900/50 text-red-400", icon: <XCircle className="h-3 w-3" />, label: "Failed" },
};

function formatBytes(bytes: number | null): string {
    if (!bytes) return "—";
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1048576) return `${(bytes / 1024).toFixed(1)} KB`;
    if (bytes < 1073741824) return `${(bytes / 1048576).toFixed(1)} MB`;
    return `${(bytes / 1073741824).toFixed(2)} GB`;
}

function formatDate(dateStr: string): string {
    return new Date(dateStr).toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });
}

type TabId = "upload" | "s3" | "url" | "huggingface" | "gdrive";

export default function DatasetsPage() {
    const router = useRouter();
    const [activeTab, setActiveTab] = useState<TabId>("upload");
    const [datasets, setDatasets] = useState<DatasetItem[]>([]);
    const [loading, setLoading] = useState(true);
    const [uploadProgress, setUploadProgress] = useState<number | null>(null);
    const [uploadError, setUploadError] = useState<string | null>(null);

    // Connector form states
    const [s3Form, setS3Form] = useState({ bucket: "", prefix: "", access_key: "", secret_key: "", region: "us-east-1", dataset_name: "", key: "" });
    const [urlForm, setUrlForm] = useState({ urls: "", scrape_mode: "auto", dataset_name: "" });
    const [hfForm, setHfForm] = useState({ dataset_id: "", config: "", split: "train", hf_token: "", dataset_name: "" });
    const [connectorLoading, setConnectorLoading] = useState(false);
    const [connectorError, setConnectorError] = useState<string | null>(null);
    const [connectorSuccess, setConnectorSuccess] = useState<string | null>(null);

    // Fetch datasets
    const fetchDatasets = useCallback(async () => {
        try {
            const res = await api.get("/api/datasets/");
            setDatasets(res.data.datasets || []);
        } catch {
            // Not authenticated or error
        } finally {
            setLoading(false);
        }
    }, []);

    useEffect(() => {
        fetchDatasets();
        const interval = setInterval(fetchDatasets, 5000);
        return () => clearInterval(interval);
    }, [fetchDatasets]);

    // Drag & drop upload
    const onDrop = useCallback(async (acceptedFiles: globalThis.File[]) => {
        if (acceptedFiles.length === 0) return;
        const file = acceptedFiles[0];
        setUploadError(null);

        const CHUNK_SIZE = 100 * 1024 * 1024; // 100MB

        if (file.size > CHUNK_SIZE) {
            // Chunked upload
            const totalChunks = Math.ceil(file.size / CHUNK_SIZE);
            const uploadId = crypto.randomUUID();

            for (let i = 0; i < totalChunks; i++) {
                const start = i * CHUNK_SIZE;
                const end = Math.min(start + CHUNK_SIZE, file.size);
                const chunk = file.slice(start, end);

                const formData = new FormData();
                formData.append("upload_id", uploadId);
                formData.append("chunk_index", i.toString());
                formData.append("total_chunks", totalChunks.toString());
                formData.append("filename", file.name);
                formData.append("chunk_data", chunk);

                try {
                    const res = await api.post("/api/ingestion/upload/chunk", formData);
                    setUploadProgress(res.data.progress);
                    if (res.data.status === "complete") {
                        setUploadProgress(null);
                        fetchDatasets();
                    }
                } catch (err: any) {
                    setUploadError(err.response?.data?.detail?.message || "Chunk upload failed");
                    setUploadProgress(null);
                    return;
                }
            }
        } else {
            // Single upload
            const formData = new FormData();
            formData.append("file", file);
            formData.append("dataset_name", file.name.replace(/\.[^.]+$/, ""));

            try {
                setUploadProgress(50);
                await api.post("/api/ingestion/upload", formData);
                setUploadProgress(null);
                fetchDatasets();
            } catch (err: any) {
                const detail = err.response?.data?.detail;
                setUploadError(typeof detail === "string" ? detail : detail?.message || "Upload failed");
                setUploadProgress(null);
            }
        }
    }, [fetchDatasets]);

    const { getRootProps, getInputProps, isDragActive } = useDropzone({
        onDrop,
        multiple: false,
    });

    // Connector submit handlers
    const handleS3Import = async () => {
        setConnectorLoading(true);
        setConnectorError(null);
        setConnectorSuccess(null);
        try {
            await api.post("/api/ingestion/connect/s3/import", {
                bucket: s3Form.bucket,
                key: s3Form.key,
                access_key: s3Form.access_key,
                secret_key: s3Form.secret_key,
                region: s3Form.region,
                dataset_name: s3Form.dataset_name || s3Form.key.split("/").pop(),
            });
            setConnectorSuccess("S3 import started!");
            fetchDatasets();
        } catch (err: any) {
            setConnectorError(err.response?.data?.detail?.message || "S3 import failed");
        } finally {
            setConnectorLoading(false);
        }
    };

    const handleUrlImport = async () => {
        setConnectorLoading(true);
        setConnectorError(null);
        setConnectorSuccess(null);
        try {
            const urls = urlForm.urls.split(/[\n,]/).map((u: string) => u.trim()).filter(Boolean);
            await api.post("/api/ingestion/connect/url", {
                urls,
                scrape_mode: urlForm.scrape_mode,
                dataset_name: urlForm.dataset_name || undefined,
            });
            setConnectorSuccess("URL import started!");
            fetchDatasets();
        } catch (err: any) {
            setConnectorError(err.response?.data?.detail?.message || "URL import failed");
        } finally {
            setConnectorLoading(false);
        }
    };

    const handleHFImport = async () => {
        setConnectorLoading(true);
        setConnectorError(null);
        setConnectorSuccess(null);
        try {
            await api.post("/api/ingestion/connect/huggingface", {
                dataset_id: hfForm.dataset_id,
                config: hfForm.config || undefined,
                split: hfForm.split || "train",
                hf_token: hfForm.hf_token || undefined,
                dataset_name: hfForm.dataset_name || undefined,
            });
            setConnectorSuccess("HuggingFace import started!");
            fetchDatasets();
        } catch (err: any) {
            setConnectorError(err.response?.data?.detail?.message || "HuggingFace import failed");
        } finally {
            setConnectorLoading(false);
        }
    };

    const TABS: { id: TabId; label: string; icon: React.ReactNode }[] = [
        { id: "upload", label: "Upload", icon: <Upload className="h-4 w-4" /> },
        { id: "s3", label: "S3", icon: <Cloud className="h-4 w-4" /> },
        { id: "url", label: "URL", icon: <Globe className="h-4 w-4" /> },
        { id: "huggingface", label: "HuggingFace", icon: <Database className="h-4 w-4" /> },
        { id: "gdrive", label: "Google Drive", icon: <HardDrive className="h-4 w-4" /> },
    ];

    return (
        <div className="space-y-8">
            {/* Header */}
            <div>
                <h1 className="text-3xl font-bold tracking-tight">Datasets</h1>
                <p className="mt-2 text-muted-foreground">Upload, import, and manage your data sources</p>
            </div>

            {/* Import Section */}
            <Card className="border-border/50 bg-card/50 backdrop-blur">
                <CardHeader className="pb-4">
                    <CardTitle className="text-lg">Import Data</CardTitle>
                    {/* Tabs */}
                    <div className="flex gap-1 mt-3 rounded-lg bg-zinc-900/50 p-1 w-fit">
                        {TABS.map((tab) => (
                            <button
                                key={tab.id}
                                onClick={() => { setActiveTab(tab.id); setConnectorError(null); setConnectorSuccess(null); }}
                                className={`flex items-center gap-2 px-3 py-1.5 rounded-md text-sm font-medium transition-colors ${activeTab === tab.id
                                    ? "bg-primary text-primary-foreground"
                                    : "text-muted-foreground hover:text-foreground hover:bg-zinc-800/50"
                                    }`}
                            >
                                {tab.icon}
                                {tab.label}
                            </button>
                        ))}
                    </div>
                </CardHeader>
                <CardContent>
                    {connectorError && (
                        <div className="mb-4 rounded-lg border border-red-800/50 bg-red-950/30 p-3 text-sm text-red-400">
                            {connectorError}
                        </div>
                    )}
                    {connectorSuccess && (
                        <div className="mb-4 rounded-lg border border-emerald-800/50 bg-emerald-950/30 p-3 text-sm text-emerald-400">
                            {connectorSuccess}
                        </div>
                    )}

                    {/* Upload Tab */}
                    {activeTab === "upload" && (
                        <div>
                            <div
                                {...getRootProps()}
                                className={`cursor-pointer rounded-xl border-2 border-dashed p-10 text-center transition-colors ${isDragActive ? "border-primary bg-primary/5" : "border-border/60 hover:border-primary/50 hover:bg-zinc-900/30"
                                    }`}
                            >
                                <input {...getInputProps()} />
                                <div className="flex flex-col items-center gap-3">
                                    <div className="rounded-full bg-primary/10 p-4">
                                        <Upload className="h-8 w-8 text-primary" />
                                    </div>
                                    {isDragActive ? (
                                        <p className="text-lg font-medium text-primary">Drop your file here...</p>
                                    ) : (
                                        <>
                                            <p className="text-lg font-medium">Drag & drop a file, or click to browse</p>
                                            <p className="text-sm text-muted-foreground">
                                                CSV, JSON, JSONL, Parquet, Excel, TXT, PDF, HTML, DOCX — up to 10GB
                                            </p>
                                        </>
                                    )}
                                    {uploadProgress !== null && (
                                        <div className="w-full max-w-xs">
                                            <div className="h-2 rounded-full bg-zinc-800 overflow-hidden">
                                                <div
                                                    className="h-full rounded-full bg-primary transition-all duration-300"
                                                    style={{ width: `${uploadProgress}%` }}
                                                />
                                            </div>
                                            <p className="mt-1 text-xs text-muted-foreground">{Math.round(uploadProgress)}% uploaded</p>
                                        </div>
                                    )}
                                    {uploadError && (
                                        <p className="text-sm text-red-400">{uploadError}</p>
                                    )}
                                </div>
                            </div>
                        </div>
                    )}

                    {/* S3 Tab */}
                    {activeTab === "s3" && (
                        <div className="grid gap-4 max-w-lg">
                            <Input placeholder="Bucket name" value={s3Form.bucket} onChange={(e) => setS3Form({ ...s3Form, bucket: e.target.value })} />
                            <Input placeholder="Key (file path)" value={s3Form.key} onChange={(e) => setS3Form({ ...s3Form, key: e.target.value })} />
                            <Input placeholder="Access Key" value={s3Form.access_key} onChange={(e) => setS3Form({ ...s3Form, access_key: e.target.value })} />
                            <Input placeholder="Secret Key" type="password" value={s3Form.secret_key} onChange={(e) => setS3Form({ ...s3Form, secret_key: e.target.value })} />
                            <Input placeholder="Region (default: us-east-1)" value={s3Form.region} onChange={(e) => setS3Form({ ...s3Form, region: e.target.value })} />
                            <Input placeholder="Dataset name (optional)" value={s3Form.dataset_name} onChange={(e) => setS3Form({ ...s3Form, dataset_name: e.target.value })} />
                            <Button onClick={handleS3Import} disabled={connectorLoading || !s3Form.bucket || !s3Form.key} className="w-fit">
                                {connectorLoading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Cloud className="mr-2 h-4 w-4" />}
                                Import from S3
                            </Button>
                        </div>
                    )}

                    {/* URL Tab */}
                    {activeTab === "url" && (
                        <div className="grid gap-4 max-w-lg">
                            <textarea
                                className="min-h-[100px] w-full rounded-lg border border-border bg-zinc-900/50 px-3 py-2 text-sm placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary/50"
                                placeholder="Enter URLs (one per line or comma-separated)"
                                value={urlForm.urls}
                                onChange={(e) => setUrlForm({ ...urlForm, urls: e.target.value })}
                            />
                            <select
                                className="rounded-lg border border-border bg-zinc-900/50 px-3 py-2 text-sm focus:outline-none focus:ring-2 focus:ring-primary/50"
                                value={urlForm.scrape_mode}
                                onChange={(e) => setUrlForm({ ...urlForm, scrape_mode: e.target.value })}
                            >
                                <option value="auto">Auto-detect (download or scrape)</option>
                                <option value="download">Direct file download</option>
                                <option value="scrape">Scrape page content</option>
                            </select>
                            <Input placeholder="Dataset name (optional)" value={urlForm.dataset_name} onChange={(e) => setUrlForm({ ...urlForm, dataset_name: e.target.value })} />
                            <Button onClick={handleUrlImport} disabled={connectorLoading || !urlForm.urls.trim()} className="w-fit">
                                {connectorLoading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Globe className="mr-2 h-4 w-4" />}
                                Import from URL
                            </Button>
                        </div>
                    )}

                    {/* HuggingFace Tab */}
                    {activeTab === "huggingface" && (
                        <div className="grid gap-4 max-w-lg">
                            <Input placeholder="Dataset ID (e.g. tatsu-lab/alpaca)" value={hfForm.dataset_id} onChange={(e) => setHfForm({ ...hfForm, dataset_id: e.target.value })} />
                            <Input placeholder="Config / subset (optional)" value={hfForm.config} onChange={(e) => setHfForm({ ...hfForm, config: e.target.value })} />
                            <Input placeholder="Split (default: train)" value={hfForm.split} onChange={(e) => setHfForm({ ...hfForm, split: e.target.value })} />
                            <Input placeholder="HF Token (for private datasets)" type="password" value={hfForm.hf_token} onChange={(e) => setHfForm({ ...hfForm, hf_token: e.target.value })} />
                            <Input placeholder="Dataset name (optional)" value={hfForm.dataset_name} onChange={(e) => setHfForm({ ...hfForm, dataset_name: e.target.value })} />
                            <Button onClick={handleHFImport} disabled={connectorLoading || !hfForm.dataset_id} className="w-fit">
                                {connectorLoading ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <Database className="mr-2 h-4 w-4" />}
                                Import from HuggingFace
                            </Button>
                        </div>
                    )}

                    {/* Google Drive Tab */}
                    {activeTab === "gdrive" && (
                        <div className="grid gap-4 max-w-lg">
                            <p className="text-sm text-muted-foreground">
                                Google Drive integration requires OAuth2 setup. Configure <code className="text-xs bg-zinc-800 px-1 py-0.5 rounded">GOOGLE_CLIENT_ID</code> and <code className="text-xs bg-zinc-800 px-1 py-0.5 rounded">GOOGLE_CLIENT_SECRET</code> environment variables.
                            </p>
                            <Button variant="outline" disabled className="w-fit">
                                <HardDrive className="mr-2 h-4 w-4" />
                                Connect Google Drive
                            </Button>
                        </div>
                    )}
                </CardContent>
            </Card>

            {/* Dataset List */}
            <Card className="border-border/50 bg-card/50 backdrop-blur">
                <CardHeader>
                    <CardTitle className="text-lg">Your Datasets</CardTitle>
                </CardHeader>
                <CardContent>
                    {loading ? (
                        <div className="flex items-center justify-center py-12">
                            <Loader2 className="h-6 w-6 animate-spin text-muted-foreground" />
                        </div>
                    ) : datasets.length === 0 ? (
                        <div className="flex flex-col items-center justify-center py-12">
                            <div className="rounded-full bg-primary/10 p-4">
                                <Database className="h-10 w-10 text-primary" />
                            </div>
                            <h3 className="mt-4 text-lg font-semibold">No datasets yet</h3>
                            <p className="mt-2 max-w-sm text-center text-sm text-muted-foreground">
                                Upload your first dataset to start preparing data for fine-tuning, RAG, or ML training.
                            </p>
                        </div>
                    ) : (
                        <div className="overflow-x-auto">
                            <table className="w-full text-sm">
                                <thead>
                                    <tr className="border-b border-border/50 text-left text-muted-foreground">
                                        <th className="pb-3 pr-4 font-medium">Name</th>
                                        <th className="pb-3 pr-4 font-medium">Format</th>
                                        <th className="pb-3 pr-4 font-medium">Rows</th>
                                        <th className="pb-3 pr-4 font-medium">Size</th>
                                        <th className="pb-3 pr-4 font-medium">Status</th>
                                        <th className="pb-3 pr-4 font-medium">Created</th>
                                        <th className="pb-3 font-medium"></th>
                                    </tr>
                                </thead>
                                <tbody>
                                    {datasets.map((ds) => {
                                        const badge = STATUS_BADGE[ds.status] || STATUS_BADGE.pending;
                                        return (
                                            <tr
                                                key={ds.id}
                                                onClick={() => router.push(`/datasets/${ds.id}`)}
                                                className="cursor-pointer border-b border-border/30 transition-colors hover:bg-zinc-900/30"
                                            >
                                                <td className="py-3 pr-4">
                                                    <div className="flex items-center gap-2">
                                                        {FORMAT_ICONS[ds.detected_format || ""] || <File className="h-4 w-4 text-gray-400" />}
                                                        <span className="font-medium">{ds.name}</span>
                                                    </div>
                                                </td>
                                                <td className="py-3 pr-4 text-muted-foreground uppercase text-xs tracking-wider">
                                                    {ds.detected_format || "—"}
                                                </td>
                                                <td className="py-3 pr-4 tabular-nums">
                                                    {ds.row_count?.toLocaleString() || "—"}
                                                </td>
                                                <td className="py-3 pr-4 text-muted-foreground">{formatBytes(ds.size_bytes)}</td>
                                                <td className="py-3 pr-4">
                                                    <span className={`inline-flex items-center gap-1.5 rounded-full px-2.5 py-0.5 text-xs font-medium ${badge.className}`}>
                                                        {badge.icon}
                                                        {badge.label}
                                                    </span>
                                                </td>
                                                <td className="py-3 pr-4 text-muted-foreground">{formatDate(ds.created_at)}</td>
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
