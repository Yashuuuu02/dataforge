"use client";

import { useState } from "react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Card } from "@/components/ui/card";
import { Bot, Send, Sparkles } from "lucide-react";

interface Message {
    id: string;
    role: "user" | "assistant";
    content: string;
}

export default function AgentPage() {
    const [messages] = useState<Message[]>([]);
    const [input, setInput] = useState("");

    const suggestions = [
        "Clean and deduplicate my dataset",
        "Convert CSV to instruction-following format",
        "Generate quality scores for each row",
        "Create a RAG-optimized chunking pipeline",
    ];

    return (
        <div className="flex h-[calc(100vh-8rem)] flex-col">
            {/* Header */}
            <div className="mb-6">
                <h1 className="text-3xl font-bold tracking-tight">AI Agent</h1>
                <p className="mt-2 text-muted-foreground">
                    Chat with the AI agent to prepare your data
                </p>
            </div>

            {/* Chat area */}
            <Card className="flex flex-1 flex-col overflow-hidden">
                <div className="flex-1 overflow-y-auto p-6">
                    {messages.length === 0 ? (
                        <div className="flex h-full flex-col items-center justify-center">
                            <div className="rounded-full bg-primary/10 p-4">
                                <Bot className="h-12 w-12 text-primary" />
                            </div>
                            <h3 className="mt-4 text-lg font-semibold">DataForge AI Agent</h3>
                            <p className="mt-2 max-w-md text-center text-sm text-muted-foreground">
                                I can help you clean, transform, and prepare your data for fine-tuning, RAG, or ML training. Tell me what you need!
                            </p>

                            {/* Suggestions */}
                            <div className="mt-8 grid grid-cols-2 gap-3">
                                {suggestions.map((suggestion) => (
                                    <button
                                        key={suggestion}
                                        className="flex items-center gap-2 rounded-lg border border-border bg-card px-4 py-3 text-left text-sm text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
                                        onClick={() => setInput(suggestion)}
                                    >
                                        <Sparkles className="h-4 w-4 shrink-0 text-primary" />
                                        {suggestion}
                                    </button>
                                ))}
                            </div>
                        </div>
                    ) : (
                        <div className="space-y-4">
                            {messages.map((msg) => (
                                <div
                                    key={msg.id}
                                    className={`flex ${msg.role === "user" ? "justify-end" : "justify-start"}`}
                                >
                                    <div
                                        className={`max-w-[80%] rounded-lg px-4 py-2 text-sm ${msg.role === "user"
                                                ? "bg-primary text-primary-foreground"
                                                : "bg-accent text-foreground"
                                            }`}
                                    >
                                        {msg.content}
                                    </div>
                                </div>
                            ))}
                        </div>
                    )}
                </div>

                {/* Input */}
                <div className="border-t border-border p-4">
                    <div className="flex gap-2">
                        <Input
                            id="agent-input"
                            placeholder="Describe what you want to do with your data..."
                            value={input}
                            onChange={(e) => setInput(e.target.value)}
                            className="flex-1"
                        />
                        <Button size="icon" disabled={!input.trim()}>
                            <Send className="h-4 w-4" />
                        </Button>
                    </div>
                    <p className="mt-2 text-xs text-muted-foreground">
                        LLM integration coming in Phase 2. This is a preview of the chat interface.
                    </p>
                </div>
            </Card>
        </div>
    );
}
