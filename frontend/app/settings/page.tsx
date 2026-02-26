"use client";

import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Settings as SettingsIcon, Key, Brain, Save } from "lucide-react";

export default function SettingsPage() {
    const [provider, setProvider] = useState("openai");
    const [apiKey, setApiKey] = useState("");

    return (
        <div className="space-y-8">
            {/* Header */}
            <div>
                <h1 className="text-3xl font-bold tracking-tight">Settings</h1>
                <p className="mt-2 text-muted-foreground">
                    Configure your DataForge workspace and integrations
                </p>
            </div>

            {/* LLM Provider */}
            <Card>
                <CardHeader>
                    <CardTitle className="flex items-center gap-2 text-lg">
                        <Brain className="h-5 w-5 text-primary" />
                        LLM Provider
                    </CardTitle>
                    <CardDescription>
                        Configure the AI model provider for the agent and data transformations
                    </CardDescription>
                </CardHeader>
                <CardContent className="space-y-4">
                    <div className="space-y-2">
                        <label htmlFor="provider-select" className="text-sm font-medium">
                            Provider
                        </label>
                        <select
                            id="provider-select"
                            value={provider}
                            onChange={(e) => setProvider(e.target.value)}
                            className="flex h-10 w-full max-w-xs rounded-md border border-input bg-background px-3 py-2 text-sm ring-offset-background focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                        >
                            <option value="openai">OpenAI</option>
                            <option value="anthropic">Anthropic</option>
                            <option value="google">Google AI</option>
                            <option value="azure">Azure OpenAI</option>
                            <option value="local">Local / Ollama</option>
                        </select>
                    </div>

                    <div className="space-y-2">
                        <label htmlFor="api-key-input" className="text-sm font-medium">
                            API Key
                        </label>
                        <div className="flex gap-2">
                            <div className="relative flex-1 max-w-md">
                                <Key className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
                                <Input
                                    id="api-key-input"
                                    type="password"
                                    placeholder="sk-..."
                                    value={apiKey}
                                    onChange={(e) => setApiKey(e.target.value)}
                                    className="pl-10"
                                />
                            </div>
                            <Button variant="outline" className="gap-2">
                                <Save className="h-4 w-4" />
                                Save
                            </Button>
                        </div>
                        <p className="text-xs text-muted-foreground">
                            Your API key is encrypted and stored securely. It will never be logged.
                        </p>
                    </div>
                </CardContent>
            </Card>

            {/* General Settings */}
            <Card>
                <CardHeader>
                    <CardTitle className="flex items-center gap-2 text-lg">
                        <SettingsIcon className="h-5 w-5 text-muted-foreground" />
                        General
                    </CardTitle>
                    <CardDescription>General workspace settings and preferences</CardDescription>
                </CardHeader>
                <CardContent>
                    <div className="flex h-24 items-center justify-center rounded-lg border border-dashed border-border">
                        <p className="text-sm text-muted-foreground">
                            Additional settings coming in Phase 2
                        </p>
                    </div>
                </CardContent>
            </Card>
        </div>
    );
}
