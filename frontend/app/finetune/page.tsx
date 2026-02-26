"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/lib/api";
import { Card, CardContent, CardDescription, CardHeader, CardTitle, CardFooter } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Label } from "@/components/ui/label";
import { Textarea } from "@/components/ui/textarea";
import { Switch } from "@/components/ui/switch";
import { Slider } from "@/components/ui/slider";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { CheckCircle2, ChevronRight, FileJson, FileText, Settings, Wand2, ArrowRight, Loader2, Play } from "lucide-react";
import { DatasetAnalysisCard } from "./analysis-card";
import { toast } from "sonner";
import { JobProgress } from "@/components/job-progress";
import { BarChart, Bar, XAxis, YAxis, Tooltip as RechartsTooltip, ResponsiveContainer, CartesianGrid } from 'recharts';

export default function FinetunePage() {
    const router = useRouter();
    const [step, setStep] = useState(1);

    // Step 1: Selection
    const [selectedDatasetId, setSelectedDatasetId] = useState<string>("");
    const [analysisResult, setAnalysisResult] = useState<any>(null);
    const [isAnalyzing, setIsAnalyzing] = useState(false);

    // Step 2: Config
    const [config, setConfig] = useState({
        run_deduplication: true,
        run_noise_removal: true,
        run_pii_scrubbing: false,
        run_quality_scoring: true,
        output_format: "llama3",
        system_prompt: "You are a helpful AI assistant.",
        max_tokens_per_example: 4096,
        run_response_quality: true,
        run_balancer: false,
        run_augmentation: false,
        train_split: 0.9,
        val_split: 0.1,
    });

    // Step 4: Run
    const [activeJobId, setActiveJobId] = useState<string | null>(null);
    const [jobResult, setJobResult] = useState<any>(null);

    // Queries
    const { data: datasets } = useQuery({
        queryKey: ["datasets"],
        queryFn: () => api.get("/datasets").then(r => r.data)
    });

    const selectedDataset = datasets?.find((d: any) => d.id === selectedDatasetId);

    const handleAnalyze = async () => {
        if (!selectedDatasetId) return;
        setIsAnalyzing(true);
        try {
            const res = await api.post(`/agent/analyze/${selectedDatasetId}`);
            setAnalysisResult(res.data);
            toast.success("Analysis complete");
        } catch (error) {
            toast.error("Analysis failed");
        } finally {
            setIsAnalyzing(false);
        }
    };

    const handleRunFinetune = async () => {
        try {
            const res = await api.post("/jobs/finetune", {
                dataset_id: selectedDatasetId,
                config: config
            });
            setActiveJobId(res.data.id);
            setStep(4);
            toast.success("Finetuning pipeline started");
        } catch (error) {
            toast.error("Failed to start pipeline");
        }
    };

    const handleJobComplete = async () => {
        if (!activeJobId) return;
        try {
            const res = await api.get(`/jobs/${activeJobId}/finetune-result`);
            setJobResult(res.data);
            toast.success("Job complete!");
        } catch (e) {
            toast.error("Failed to fetch results");
        }
    };

    const outputFormats = [
        { id: "llama3", name: "Llama 3", desc: "<|begin_of_text|><|start_header_id|>...", preview: "<|start_header_id|>system<|end_header_id|>..." },
        { id: "llama2", name: "Llama 2", desc: "<s>[INST] <<SYS>>...", preview: "<s>[INST] <<SYS>>..." },
        { id: "mistral", name: "Mistral", desc: "<s>[INST] instruction...", preview: "<s>[INST] You are..." },
        { id: "gemma", name: "Gemma", desc: "<start_of_turn>user...", preview: "<start_of_turn>user..." },
        { id: "openai", name: "OpenAI JSONL", desc: "{\"messages\": [...]}", preview: "{\"messages\": [{\"role\": \"system\",..." },
        { id: "alpaca", name: "Alpaca JSON", desc: "{\"instruction\":...}", preview: "{\"instruction\": \"...\", \"input\":..." },
        { id: "sharegpt", name: "ShareGPT", desc: "{\"conversations\": [...]}", preview: "{\"conversations\": [{\"from\":..." },
    ];

    return (
        <div className="mx-auto max-w-5xl space-y-8">
            <div className="flex items-center justify-between">
                <div>
                    <h1 className="text-3xl font-bold tracking-tight">Fine-Tune Mode</h1>
                    <p className="text-muted-foreground mt-2">
                        Prepare datasets for LLM fine-tuning. Automatically format, clean, balance, and split your data.
                    </p>
                </div>
            </div>

            {/* Stepper Header */}
            <div className="flex items-center justify-between rounded-lg border bg-card p-4">
                {[
                    { num: 1, label: "Select Dataset", icon: Database },
                    { num: 2, label: "Configure Pipeline", icon: Settings },
                    { num: 3, label: "Preview", icon: FileJson },
                    { num: 4, label: "Run & Results", icon: Play }
                ].map((s, i) => (
                    <div key={s.num} className={`flex items-center gap-3 ${step === s.num ? "text-primary" : step > s.num ? "text-green-500" : "text-muted-foreground"}`}>
                        <div className={`flex h-8 w-8 items-center justify-center rounded-full border-2 ${step === s.num ? "border-primary bg-primary/10" : step > s.num ? "border-green-500 bg-green-500/10" : "border-muted"}`}>
                            {step > s.num ? <CheckCircle2 className="h-4 w-4" /> : <s.icon className="h-4 w-4" />}
                        </div>
                        <span className="font-medium">{s.label}</span>
                        {i < 3 && <ChevronRight className="mx-4 h-4 w-4 text-muted" />}
                    </div>
                ))}
            </div>

            {/* Step 1: Select */}
            {step === 1 && (
                <div className="space-y-6 animate-in slide-in-from-bottom-2">
                    <Card>
                        <CardHeader>
                            <CardTitle>Select Dataset</CardTitle>
                            <CardDescription>Choose a dataset to prepare for fine-tuning.</CardDescription>
                        </CardHeader>
                        <CardContent className="space-y-6">
                            <Select value={selectedDatasetId} onValueChange={setSelectedDatasetId}>
                                <SelectTrigger className="w-full">
                                    <SelectValue placeholder="Select a dataset..." />
                                </SelectTrigger>
                                <SelectContent>
                                    {datasets?.map((d: any) => (
                                        <SelectItem key={d.id} value={d.id}>
                                            <div className="flex items-center gap-2">
                                                <Database className="h-4 w-4 text-muted-foreground" />
                                                <span>{d.name}</span>
                                                <Badge variant="secondary" className="ml-2">{d.row_count} rows</Badge>
                                            </div>
                                        </SelectItem>
                                    ))}
                                </SelectContent>
                            </Select>

                            {selectedDataset && !analysisResult && (
                                <div className="flex justify-center p-6 border rounded-lg border-dashed">
                                    <div className="text-center space-y-4">
                                        <Wand2 className="mx-auto h-8 w-8 text-primary" />
                                        <p className="text-muted-foreground">Analyze this dataset to auto-detect its format and get recommendations.</p>
                                        <Button onClick={handleAnalyze} disabled={isAnalyzing}>
                                            {isAnalyzing && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                                            Analyze with AI
                                        </Button>
                                    </div>
                                </div>
                            )}

                            {analysisResult && (
                                <DatasetAnalysisCard analysis={analysisResult} />
                            )}
                        </CardContent>
                        <CardFooter className="flex justify-end">
                            <Button
                                onClick={() => setStep(2)}
                                disabled={!selectedDatasetId}
                            >
                                Continue to Configuration <ArrowRight className="ml-2 h-4 w-4" />
                            </Button>
                        </CardFooter>
                    </Card>
                </div>
            )}

            {/* Step 2: Configure */}
            {step === 2 && (
                <div className="space-y-6 animate-in slide-in-from-bottom-2">
                    <div className="grid gap-6 md:grid-cols-2">
                        <Card>
                            <CardHeader>
                                <CardTitle>Pre-Processing Steps</CardTitle>
                                <CardDescription>Standard cleaning pipeline applied before formatting.</CardDescription>
                            </CardHeader>
                            <CardContent className="space-y-6">
                                <div className="flex items-center justify-between">
                                    <div className="space-y-0.5">
                                        <Label>Deduplication</Label>
                                        <p className="text-sm text-muted-foreground">Remove exact duplicate rows</p>
                                    </div>
                                    <Switch checked={config.run_deduplication} onCheckedChange={(c) => setConfig({ ...config, run_deduplication: c })} />
                                </div>
                                <div className="flex items-center justify-between">
                                    <div className="space-y-0.5">
                                        <Label>Noise Removal</Label>
                                        <p className="text-sm text-muted-foreground">Strip HTML, fix encodings, normalize whitespace</p>
                                    </div>
                                    <Switch checked={config.run_noise_removal} onCheckedChange={(c) => setConfig({ ...config, run_noise_removal: c })} />
                                </div>
                                <div className="flex items-center justify-between">
                                    <div className="space-y-0.5">
                                        <Label>Response Quality Filter</Label>
                                        <p className="text-sm text-muted-foreground">Filter out short/cut-off responses and model refusals</p>
                                    </div>
                                    <Switch checked={config.run_response_quality} onCheckedChange={(c) => setConfig({ ...config, run_response_quality: c })} />
                                </div>
                                <div className="flex items-center justify-between">
                                    <div className="space-y-0.5">
                                        <Label>PII Scrubbing</Label>
                                        <p className="text-sm text-muted-foreground">Redact sensitive info (slow)</p>
                                    </div>
                                    <Switch checked={config.run_pii_scrubbing} onCheckedChange={(c) => setConfig({ ...config, run_pii_scrubbing: c })} />
                                </div>
                                <div className="flex items-center justify-between">
                                    <div className="space-y-0.5">
                                        <Label>Category Balancer</Label>
                                        <p className="text-sm text-muted-foreground">Undersample majority classes</p>
                                    </div>
                                    <Switch checked={config.run_balancer} onCheckedChange={(c) => setConfig({ ...config, run_balancer: c })} />
                                </div>
                            </CardContent>
                        </Card>

                        <Card>
                            <CardHeader>
                                <CardTitle>Formatting & Output</CardTitle>
                                <CardDescription>How the final dataset should be structured.</CardDescription>
                            </CardHeader>
                            <CardContent className="space-y-6">
                                <div className="space-y-3">
                                    <Label>Target Output Format</Label>
                                    <div className="grid grid-cols-2 gap-2">
                                        {outputFormats.map(fmt => (
                                            <div
                                                key={fmt.id}
                                                onClick={() => setConfig({ ...config, output_format: fmt.id })}
                                                className={`cursor-pointer rounded-md border p-3 hover:border-primary transition-colors ${config.output_format === fmt.id ? "border-primary bg-primary/5 ring-1 ring-primary" : ""}`}
                                            >
                                                <div className="font-medium text-sm">{fmt.name}</div>
                                                <div className="text-xs text-muted-foreground truncate">{fmt.desc}</div>
                                            </div>
                                        ))}
                                    </div>
                                </div>

                                <div className="space-y-3">
                                    <Label>System Prompt (Injected)</Label>
                                    <Textarea
                                        value={config.system_prompt}
                                        onChange={(e) => setConfig({ ...config, system_prompt: e.target.value })}
                                        className="h-20 text-sm font-mono"
                                        placeholder="You are a helpful assistant."
                                    />
                                </div>

                                <div className="space-y-4 mt-6">
                                    <div className="flex justify-between items-center">
                                        <Label>Max Tokens per Example (Filter)</Label>
                                        <span className="text-sm text-muted-foreground font-mono">{config.max_tokens_per_example}</span>
                                    </div>
                                    <Slider
                                        value={[config.max_tokens_per_example]}
                                        onValueChange={(v) => setConfig({ ...config, max_tokens_per_example: v[0] })}
                                        min={512} max={16384} step={512}
                                    />
                                </div>

                                <div className="space-y-4 mt-6">
                                    <div className="flex justify-between items-center">
                                        <Label>Train / Validation Split</Label>
                                        <span className="text-sm text-muted-foreground font-mono">{Math.round(config.train_split * 100)}% / {Math.round(config.val_split * 100)}%</span>
                                    </div>
                                    <Slider
                                        value={[config.train_split]}
                                        onValueChange={(v) => setConfig({ ...config, train_split: v[0], val_split: 1.0 - v[0] })}
                                        min={0.5} max={0.99} step={0.01}
                                    />
                                </div>
                            </CardContent>
                        </Card>
                    </div>

                    <div className="flex justify-between">
                        <Button variant="outline" onClick={() => setStep(1)}>Back</Button>
                        <Button onClick={() => setStep(3)}>Continue to Preview <ArrowRight className="ml-2 h-4 w-4" /></Button>
                    </div>
                </div>
            )}

            {/* Step 3: Preview */}
            {step === 3 && (
                <div className="space-y-6 animate-in slide-in-from-bottom-2">
                    <Alert>
                        <Wand2 className="h-4 w-4" />
                        <AlertTitle>Format Preview</AlertTitle>
                        <AlertDescription>
                            DataForge will auto-detect the input format (ShareGPT, Alpaca, Chat, etc.) and map it into the format shown below.
                        </AlertDescription>
                    </Alert>

                    <Card>
                        <CardHeader>
                            <CardTitle>{outputFormats.find(f => f.id === config.output_format)?.name} Format Spec</CardTitle>
                        </CardHeader>
                        <CardContent>
                            <div className="bg-muted p-4 rounded-md overflow-x-auto whitespace-pre font-mono text-sm text-muted-foreground">
                                {config.output_format === "llama3" && `<|begin_of_text|><|start_header_id|>system<|end_header_id|>\n${config.system_prompt}<|eot_id|><|start_header_id|>user<|end_header_id|>\n{instruction}<|eot_id|><|start_header_id|>assistant<|end_header_id|>\n{output}<|eot_id|>`}
                                {config.output_format === "mistral" && `<s>[INST] ${config.system_prompt}\\n\\n{instruction} [/INST] {output}</s>`}
                                {config.output_format === "openai" && `{"messages": [\n  {"role": "system", "content": "${config.system_prompt}"},\n  {"role": "user", "content": "{instruction}"},\n  {"role": "assistant", "content": "{output}"}\n]}`}
                                {config.output_format === "alpaca" && `{"instruction": "{instruction}", "input": "{input}", "output": "{output}"}`}
                                {config.output_format === "gemma" && `<start_of_turn>user\n${config.system_prompt}\\n\\n{instruction}<end_of_turn>\n<start_of_turn>model\n{output}<end_of_turn>`}
                                {config.output_format === "llama2" && `<s>[INST] <<SYS>>${config.system_prompt}<</SYS>> {instruction} [/INST] {output} </s>`}
                                {config.output_format === "sharegpt" && `{"conversations": [\n  {"from": "system", "value": "${config.system_prompt}"},\n  {"from": "human", "value": "{instruction}"},\n  {"from": "gpt", "value": "{output}"}\n]}`}
                            </div>
                        </CardContent>
                    </Card>

                    <div className="flex justify-between">
                        <Button variant="outline" onClick={() => setStep(2)}>Back</Button>
                        <Button onClick={handleRunFinetune} size="lg" className="bg-green-600 hover:bg-green-700">
                            Start Fine-Tune Execution <Play className="ml-2 h-4 w-4 fill-current" />
                        </Button>
                    </div>
                </div>
            )}

            {/* Step 4: Run & Results */}
            {step === 4 && activeJobId && (
                <div className="space-y-6 animate-in slide-in-from-bottom-2">
                    <Card>
                        <CardHeader>
                            <CardTitle>Pipeline Execution</CardTitle>
                            <CardDescription>Job ID: {activeJobId}</CardDescription>
                        </CardHeader>
                        <CardContent>
                            {!jobResult ? (
                                <JobProgress jobId={activeJobId} onComplete={handleJobComplete} />
                            ) : (
                                <div className="space-y-8">
                                    <div className="flex items-center gap-3 text-green-500">
                                        <CheckCircle2 className="h-8 w-8" />
                                        <div>
                                            <p className="font-bold text-lg text-foreground">Fine-Tuning Preparation Complete</p>
                                            <p className="text-sm text-muted-foreground">Generated Train and Validation splits.</p>
                                        </div>
                                    </div>

                                    <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                                        <Card className="bg-muted/50 border-none shadow-none">
                                            <CardContent className="p-4 flex flex-col items-center justify-center h-full">
                                                <p className="text-sm font-medium text-muted-foreground">Total Examples</p>
                                                <p className="text-3xl font-bold">{jobResult.stats?.train_examples + jobResult.stats?.val_examples}</p>
                                            </CardContent>
                                        </Card>
                                        <Card className="bg-primary/5 border-primary/20 shadow-none">
                                            <CardContent className="p-4 flex flex-col items-center justify-center h-full">
                                                <p className="text-sm font-medium text-primary">Train Split</p>
                                                <p className="text-3xl font-bold text-primary">{jobResult.stats?.train_examples}</p>
                                            </CardContent>
                                        </Card>
                                        <Card className="bg-green-500/5 border-green-500/20 shadow-none">
                                            <CardContent className="p-4 flex flex-col items-center justify-center h-full">
                                                <p className="text-sm font-medium text-green-500">Val Split</p>
                                                <p className="text-3xl font-bold text-green-500">{jobResult.stats?.val_examples}</p>
                                            </CardContent>
                                        </Card>
                                        <Card className="bg-muted/50 border-none shadow-none">
                                            <CardContent className="p-4 flex flex-col items-center justify-center h-full">
                                                <p className="text-sm font-medium text-muted-foreground">Avg Tokens</p>
                                                <p className="text-3xl font-bold">{Math.round(jobResult.stats?.avg_tokens || 0)}</p>
                                            </CardContent>
                                        </Card>
                                    </div>

                                    <Card className="border-primary/20 bg-primary/5">
                                        <CardHeader>
                                            <CardTitle className="text-lg flex items-center gap-2"><FileText className="h-5 w-5" /> Download Artifacts</CardTitle>
                                        </CardHeader>
                                        <CardContent className="flex flex-wrap gap-4">
                                            <Button asChild className="gap-2">
                                                <a href={jobResult.urls?.train_url} download><FileJson className="h-4 w-4" /> train.jsonl</a>
                                            </Button>
                                            <Button asChild variant="secondary" className="gap-2 border-primary/20 hover:bg-primary/10">
                                                <a href={jobResult.urls?.val_url} download><FileJson className="h-4 w-4" /> val.jsonl</a>
                                            </Button>
                                            <Button asChild variant="outline" className="gap-2 ml-auto">
                                                <a href={jobResult.urls?.config_url} download><Settings className="h-4 w-4" /> training_config.json</a>
                                            </Button>
                                        </CardContent>
                                    </Card>
                                </div>
                            )}
                        </CardContent>
                    </Card>
                </div>
            )}
        </div>
    );
}
