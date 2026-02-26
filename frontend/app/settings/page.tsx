"use client";

import { useState, useEffect } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle, CardFooter } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
    Select,
    SelectContent,
    SelectItem,
    SelectTrigger,
    SelectValue,
} from "@/components/ui/select";
import { Eye, EyeOff, Loader2, CheckCircle2, XCircle, Settings2 } from "lucide-react";
import api from "@/lib/api";

const PROVIDERS = [
    { id: "openai", name: "OpenAI", defaultModel: "gpt-4o-mini", models: ["gpt-4o", "gpt-4o-mini", "gpt-3.5-turbo"] },
    { id: "anthropic", name: "Anthropic", defaultModel: "claude-3-haiku-20240307", models: ["claude-3-opus-20240229", "claude-3-sonnet-20240229", "claude-3-haiku-20240307"] },
    { id: "groq", name: "Groq", defaultModel: "llama3-8b-8192", models: ["llama3-70b-8192", "llama3-8b-8192", "mixtral-8x7b-32768"] },
    { id: "mistral", name: "Mistral", defaultModel: "mistral-tiny", models: ["mistral-large-latest", "mistral-small-latest", "mistral-tiny"] },
    { id: "ollama", name: "Ollama (Local)", defaultModel: "llama3", models: ["llama3", "mistral", "phi3"] },
];

export default function SettingsPage() {
    const [provider, setProvider] = useState("openai");
    const [apiKey, setApiKey] = useState("");
    const [hasKey, setHasKey] = useState(false);
    const [isApiKeyTouched, setIsApiKeyTouched] = useState(false);
    const [model, setModel] = useState("gpt-4o-mini");
    const [baseUrl, setBaseUrl] = useState("");
    const [showKey, setShowKey] = useState(false);

    const [loading, setLoading] = useState(true);
    const [saving, setSaving] = useState(false);
    const [testing, setTesting] = useState(false);
    const [testResult, setTestResult] = useState<{ valid: boolean; error?: string } | null>(null);

    useEffect(() => {
        async function load() {
            try {
                const res = await api.get("/api/settings/llm");
                if (res.data.provider) setProvider(res.data.provider);
                if (res.data.model) setModel(res.data.model);
                if (res.data.base_url) setBaseUrl(res.data.base_url);
                setHasKey(res.data.has_key);
                if (res.data.has_key) {
                    setApiKey("••••••••••••••••••••••••"); // fake mask
                }
            } catch (err) { }
            finally { setLoading(false); }
        }
        load();
    }, []);

    const handleProviderChange = (val: string) => {
        setProvider(val);
        const p = PROVIDERS.find(p => p.id === val);
        if (p) setModel(p.defaultModel);
        if (val === "ollama") setBaseUrl("http://localhost:11434");
        else setBaseUrl("");
    };

    const currentProvider = PROVIDERS.find(p => p.id === provider);

    const handleSave = async () => {
        setSaving(true);
        setTestResult(null);
        try {
            // If they haven't touched the masked key, we don't send it. 
            // But the backend endpoint currently requires it to test.
            // So if it's untouched, we just test the existing connection.
            if (!isApiKeyTouched && hasKey) {
                const tRes = await api.post("/api/settings/llm/test");
                setTestResult(tRes.data);
            } else {
                const res = await api.put("/api/settings/llm", {
                    provider,
                    api_key: apiKey,
                    model,
                    base_url: baseUrl || null
                });
                setTestResult(res.data);
                setHasKey(true);
                setApiKey("••••••••••••••••••••••••");
                setIsApiKeyTouched(false);
            }
        } catch (err: any) {
            setTestResult({
                valid: false,
                error: err.response?.data?.detail || "Connection failed. Check your API key."
            });
        } finally {
            setSaving(false);
        }
    };

    const handleTest = async () => {
        setTesting(true);
        setTestResult(null);
        try {
            const res = await api.post("/api/settings/llm/test");
            setTestResult(res.data);
        } catch (err: any) {
            setTestResult({
                valid: false,
                error: "Failed to connect to the saved provider."
            });
        } finally {
            setTesting(false);
        }
    };

    if (loading) return <div className="flex justify-center p-20"><Loader2 className="h-8 w-8 animate-spin" /></div>;

    return (
        <div className="max-w-2xl mx-auto space-y-8">
            <div>
                <h1 className="text-3xl font-bold tracking-tight">Settings</h1>
                <p className="mt-2 text-muted-foreground">Configure AI providers and application preferences.</p>
            </div>

            <Card className="border-border/50 bg-card/50">
                <CardHeader>
                    <CardTitle className="flex items-center gap-2">
                        <Settings2 className="h-5 w-5" /> AI Provider Configuration
                    </CardTitle>
                    <CardDescription>
                        DataForge uses an LLM to automatically analyze datasets and orchestrate data cleaning pipelines.
                        Without an API key, DataForge will fall back to using static heuristics.
                    </CardDescription>
                </CardHeader>
                <CardContent className="space-y-6">
                    <div className="space-y-2">
                        <Label>Provider</Label>
                        <Select value={provider} onValueChange={handleProviderChange}>
                            <SelectTrigger className="w-full bg-zinc-900/50">
                                <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                                {PROVIDERS.map((p) => (
                                    <SelectItem key={p.id} value={p.id}>{p.name}</SelectItem>
                                ))}
                            </SelectContent>
                        </Select>
                    </div>

                    <div className="space-y-2">
                        <Label>API Key</Label>
                        <div className="relative">
                            <Input
                                type={showKey ? "text" : "password"}
                                placeholder={provider === "ollama" ? "Not required for local Ollama" : `Enter your ${currentProvider?.name} API Key`}
                                value={apiKey}
                                onChange={(e) => {
                                    setApiKey(e.target.value);
                                    setIsApiKeyTouched(true);
                                }}
                                disabled={provider === "ollama"}
                                className="bg-zinc-900/50 pr-10"
                            />
                            <button
                                type="button"
                                onClick={() => setShowKey(!showKey)}
                                className="absolute right-3 top-1/2 -translate-y-1/2 text-muted-foreground hover:text-foreground"
                                disabled={provider === "ollama"}
                            >
                                {showKey ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}
                            </button>
                        </div>
                    </div>

                    <div className="space-y-2">
                        <Label>Model</Label>
                        <Select value={model} onValueChange={setModel}>
                            <SelectTrigger className="w-full bg-zinc-900/50">
                                <SelectValue />
                            </SelectTrigger>
                            <SelectContent>
                                {currentProvider?.models.map((m) => (
                                    <SelectItem key={m} value={m}>{m}</SelectItem>
                                ))}
                            </SelectContent>
                        </Select>
                    </div>

                    {provider === "ollama" && (
                        <div className="space-y-2">
                            <Label>Base URL</Label>
                            <Input
                                value={baseUrl}
                                onChange={(e) => setBaseUrl(e.target.value)}
                                placeholder="http://localhost:11434"
                                className="bg-zinc-900/50"
                            />
                        </div>
                    )}

                    {testResult && (
                        <div className={`p-3 rounded-md text-sm flex items-start gap-2 ${testResult.valid ? "bg-emerald-950/30 text-emerald-400 border border-emerald-900/50" : "bg-red-950/30 text-red-400 border border-red-900/50"}`}>
                            {testResult.valid ? <CheckCircle2 className="h-5 w-5 shrink-0 mt-0.5" /> : <XCircle className="h-5 w-5 shrink-0 mt-0.5" />}
                            <div>
                                <p className="font-medium">{testResult.valid ? "Connection Successful" : "Connection Failed"}</p>
                                {testResult.error && <p className="mt-1 opacity-80">{testResult.error}</p>}
                            </div>
                        </div>
                    )}
                </CardContent>
                <CardFooter className="flex justify-between border-t border-border/30 pt-6">
                    <Button variant="outline" onClick={handleTest} disabled={testing || (!hasKey && !isApiKeyTouched && provider !== "ollama")}>
                        {testing && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                        Test Connection
                    </Button>
                    <Button onClick={handleSave} disabled={saving || (!isApiKeyTouched && provider !== "ollama" && testResult?.valid)}>
                        {saving && <Loader2 className="mr-2 h-4 w-4 animate-spin" />}
                        Save Settings
                    </Button>
                </CardFooter>
            </Card>
        </div>
    );
}
