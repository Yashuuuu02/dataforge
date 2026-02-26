"use client";

import { useState, useEffect, useRef } from "react";
import { useRouter } from "next/navigation";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Loader2, Bot, User, Send, CheckCircle2, AlertTriangle, Layers, GripVertical, Play, Edit3, Save } from "lucide-react";
import { DndContext, closestCenter, KeyboardSensor, PointerSensor, useSensor, useSensors, DragEndEvent } from "@dnd-kit/core";
import { arrayMove, SortableContext, sortableKeyboardCoordinates, useSortable, verticalListSortingStrategy } from "@dnd-kit/sortable";
import { CSS } from "@dnd-kit/utilities";
import api from "@/lib/api";

type DatasetInfo = { id: string; name: string; format: string; size: number; status: string; };
type DatasetAnalysis = {
    dataset_type: string;
    detected_language: string;
    estimated_quality: number;
    row_count: number;
    recommended_mode: string;
    summary: string;
    issues_detected: { severity: string; type: string; description: string }[];
};
type WorkflowStep = { id: string; step: string; label: string; config: any; reason: string; is_required: boolean; can_be_skipped: boolean; };
type WorkflowPlan = { steps: WorkflowStep[]; estimated_duration_seconds: number; estimated_rows_after: number; explanation: string; };

type Message = { id: string; role: "user" | "agent"; content: string; suggestions?: string[]; workflow?: WorkflowPlan; isStreaming?: boolean; };

// ── Sortable Step Component for Workflow Preview ──
function SortableWorkflowStep({ step, onRemove, onConfigEdit }: { step: WorkflowStep; onRemove: () => void; onConfigEdit: (newConfig: any) => void }) {
    const { attributes, listeners, setNodeRef, transform, transition } = useSortable({ id: step.id });
    const style = { transform: CSS.Transform.toString(transform), transition };
    const [editing, setEditing] = useState(false);
    const [tempConfig, setTempConfig] = useState(JSON.stringify(step.config, null, 2));

    const handleSave = () => {
        try { onConfigEdit(JSON.parse(tempConfig)); setEditing(false); } catch { alert("Invalid JSON"); }
    };

    return (
        <div ref={setNodeRef} style={style} className="rounded-md border border-border/40 bg-zinc-900/40 p-3 mb-2">
            <div className="flex items-start gap-2">
                <button {...attributes} {...listeners} className="cursor-grab mt-1 p-0.5 text-muted-foreground opacity-50 hover:opacity-100"><GripVertical className="h-4 w-4" /></button>
                <div className="flex-1 min-w-0">
                    <div className="flex items-center justify-between">
                        <p className="text-sm font-semibold">{step.label}</p>
                        <div className="flex gap-1">
                            <button onClick={() => setEditing(!editing)} className="p-1 text-muted-foreground hover:text-primary"><Edit3 className="h-3 w-3" /></button>
                            <button onClick={onRemove} className="p-1 text-muted-foreground hover:text-red-400"><AlertTriangle className="h-3 w-3" /></button>
                        </div>
                    </div>
                    <p className="text-xs text-muted-foreground mt-0.5 mb-1">{step.reason}</p>
                    {editing ? (
                        <div className="mt-2 space-y-2">
                            <textarea value={tempConfig} onChange={(e) => setTempConfig(e.target.value)} className="w-full h-24 bg-zinc-950 border border-border/50 text-xs p-2 rounded focus:outline-none focus:ring-1 focus:ring-primary font-mono" />
                            <div className="flex justify-end gap-2"><Button size="sm" variant="ghost" className="h-6 text-xs" onClick={() => setEditing(false)}>Cancel</Button><Button size="sm" className="h-6 text-xs gap-1" onClick={handleSave}><Save className="h-3 w-3" /> Save</Button></div>
                        </div>
                    ) : (
                        <div className="text-[10px] bg-zinc-950/50 p-1.5 rounded text-muted-foreground font-mono truncate">{JSON.stringify(step.config)}</div>
                    )}
                </div>
            </div>
        </div>
    );
}

export default function AgentPage() {
    const router = useRouter();
    const [datasets, setDatasets] = useState<DatasetInfo[]>([]);
    const [selectedDatasetId, setSelectedDatasetId] = useState<string>("");

    const [analysis, setAnalysis] = useState<DatasetAnalysis | null>(null);
    const [analyzing, setAnalyzing] = useState(false);

    const [messages, setMessages] = useState<Message[]>([]);
    const [input, setInput] = useState("");
    const [isTyping, setIsTyping] = useState(false);

    const [sessionId, setSessionId] = useState<string>("");

    const messagesEndRef = useRef<HTMLDivElement>(null);

    const sensors = useSensors(useSensor(PointerSensor), useSensor(KeyboardSensor, { coordinateGetter: sortableKeyboardCoordinates }));

    useEffect(() => {
        api.get("/api/datasets").then(res => setDatasets(res.data.items || [])).catch(() => { });
        setSessionId(Math.random().toString(36).substring(2, 10)); // Generate simple session ID
    }, []);

    useEffect(() => { messagesEndRef.current?.scrollIntoView({ behavior: "smooth" }); }, [messages]);

    useEffect(() => {
        if (!selectedDatasetId) return;
        setAnalysis(null);
        setMessages([]);
        setAnalyzing(true);
        api.post(`/api/agent/analyze/${selectedDatasetId}`).then(res => {
            setAnalysis(res.data);
            setMessages([{
                id: "1", role: "agent",
                content: `I've analyzed your dataset. It looks like a **${res.data.dataset_type.replace('_', ' ')}** dataset containing ~${res.data.row_count.toLocaleString()} rows.\n\n` +
                    `**Quality Score:** ${res.data.estimated_quality}/10\n\n` +
                    `${res.data.summary}\n\n` +
                    `What would you like to do? I can automatically configure a cleanup pipeline for you.`,
                suggestions: ["Clean this up for ML training", "Fix the PII and duplicates", "What are the issues?"]
            }]);
        }).catch(() => {
            setMessages([{ id: "1", role: "agent", content: "I couldn't analyze the dataset. Is the backend running?", suggestions: [] }]);
        }).finally(() => setAnalyzing(false));
    }, [selectedDatasetId]);

    const handleSend = async (text: string) => {
        if (!text.trim() || !selectedDatasetId || isTyping) return;

        const userMsg: Message = { id: Date.now().toString(), role: "user", content: text };
        setMessages(prev => [...prev, userMsg]);
        setInput("");
        setIsTyping(true);

        const agentMsgId = (Date.now() + 1).toString();
        setMessages(prev => [...prev, { id: agentMsgId, role: "agent", content: "", isStreaming: true }]);

        try {
            const res = await fetch("/api/agent/chat", {
                method: "POST",
                headers: { "Content-Type": "application/json", "Authorization": `Bearer ${localStorage.getItem('token')}` },
                body: JSON.stringify({ message: text, dataset_id: selectedDatasetId, session_id: sessionId })
            });

            if (!res.ok) throw new Error("Failed to connect");
            const reader = res.body?.getReader();
            const decoder = new TextDecoder();

            let accumulatedText = "";
            let finalWorkflow: WorkflowPlan | undefined;
            let finalSuggestions: string[] | undefined;

            while (reader) {
                const { done, value } = await reader.read();
                if (done) break;

                const chunk = decoder.decode(value);
                const lines = chunk.split("\n\n");

                for (const line of lines) {
                    if (line.startsWith("data: ")) {
                        try {
                            const data = JSON.parse(line.substring(6));
                            if (data.type === "chunk" && data.content) {
                                accumulatedText += data.content;
                                setMessages(prev => prev.map(m => m.id === agentMsgId ? { ...m, content: accumulatedText } : m));
                            } else if (data.type === "done") {
                                finalWorkflow = data.workflow;
                                finalSuggestions = data.suggestions;
                            } else if (data.type === "error") {
                                accumulatedText += "\n\nError: " + data.content;
                            }
                        } catch (e) { }
                    }
                }
            }

            setMessages(prev => prev.map(m => m.id === agentMsgId ? { ...m, content: accumulatedText, isStreaming: false, workflow: finalWorkflow, suggestions: finalSuggestions } : m));

        } catch (err) {
            setMessages(prev => prev.map(m => m.id === agentMsgId ? { ...m, content: "Oops, something went wrong communicating with the server.", isStreaming: false } : m));
        } finally {
            setIsTyping(false);
        }
    };

    const handleRunWorkflow = async (workflow: WorkflowPlan, messageId: string) => {
        setMessages(prev => prev.map(m => m.id === messageId ? { ...m, content: m.content + "\n\n*Running pipeline...*" } : m));
        try {
            const res = await api.post("/api/agent/workflow/run", { dataset_id: selectedDatasetId, workflow });
            router.push(`/jobs/${res.data.job_id}`);
        } catch (e) {
            alert("Failed to run workflow");
        }
    };

    // Workflow Reordering Handler
    const handleDragEnd = (event: DragEndEvent, messageId: string) => {
        const { active, over } = event;
        if (over && active.id !== over.id) {
            setMessages(prev => prev.map(m => {
                if (m.id === messageId && m.workflow) {
                    const oldIndex = m.workflow.steps.findIndex(s => s.id === active.id);
                    const newIndex = m.workflow.steps.findIndex(s => s.id === over.id);
                    return { ...m, workflow: { ...m.workflow, steps: arrayMove(m.workflow.steps, oldIndex, newIndex) } };
                }
                return m;
            }));
        }
    };

    return (
        <div className="flex h-[calc(100vh-8rem)] gap-4 overflow-hidden">
            {/* LEFT PANEL - Dataset context */}
            <div className="w-[350px] flex-shrink-0 flex flex-col gap-4 overflow-y-auto pr-2 pb-4">
                <div>
                    <h1 className="text-2xl font-bold tracking-tight">AI Data Agent</h1>
                    <p className="text-sm text-muted-foreground mt-1">Talk to the agent to analyze and clean your data automatically.</p>
                </div>

                <div className="space-y-2">
                    <label className="text-sm font-medium">Select Dataset to work on</label>
                    <Select value={selectedDatasetId} onValueChange={setSelectedDatasetId}>
                        <SelectTrigger className="w-full bg-zinc-900/50"><SelectValue placeholder="Select dataset..." /></SelectTrigger>
                        <SelectContent>{datasets.map(d => <SelectItem key={d.id} value={d.id}>{d.name}</SelectItem>)}</SelectContent>
                    </Select>
                </div>

                {analyzing && <div className="mt-8 flex justify-center"><Loader2 className="h-6 w-6 animate-spin text-muted-foreground" /></div>}

                {analysis && !analyzing && (
                    <Card className="border-border/50 bg-card/50">
                        <CardHeader className="pb-3 pt-4 px-4 bg-primary/5 border-b border-border/30">
                            <CardTitle className="text-sm flex items-center gap-2"><Layers className="h-4 w-4 text-primary" /> Dataset Context</CardTitle>
                        </CardHeader>
                        <CardContent className="p-4 space-y-4">
                            <div className="flex justify-between items-center text-sm">
                                <span className="text-muted-foreground">Type:</span>
                                <span className="font-medium bg-zinc-800 px-2 py-0.5 rounded text-xs capitalize">{analysis.dataset_type.replace('_', ' ')}</span>
                            </div>
                            <div className="flex justify-between items-center text-sm">
                                <span className="text-muted-foreground">Rows:</span>
                                <span className="font-medium font-mono text-xs">{analysis.row_count.toLocaleString()}</span>
                            </div>
                            <div className="flex justify-between items-center text-sm">
                                <span className="text-muted-foreground">Quality Score:</span>
                                <span className={`font-medium ${analysis.estimated_quality > 7 ? 'text-emerald-400' : analysis.estimated_quality > 4 ? 'text-yellow-400' : 'text-red-400'}`}>{analysis.estimated_quality}/10</span>
                            </div>

                            {analysis.issues_detected.length > 0 && (
                                <div className="pt-2 border-t border-border/30">
                                    <p className="text-xs font-medium text-muted-foreground mb-2 items-center flex gap-1"><AlertTriangle className="h-3 w-3" /> Issues Detected</p>
                                    <ul className="space-y-1.5 list-disc pl-4">
                                        {analysis.issues_detected.map((iss, i) => (
                                            <li key={i} className={`text-xs ${iss.severity === 'high' ? 'text-red-400' : iss.severity === 'medium' ? 'text-yellow-400' : 'text-blue-300'}`}>
                                                {iss.description}
                                            </li>
                                        ))}
                                    </ul>
                                </div>
                            )}
                        </CardContent>
                    </Card>
                )}
            </div>

            {/* RIGHT PANEL - Chat */}
            <Card className="flex-1 flex flex-col border-border/50 bg-zinc-950/50 relative overflow-hidden">
                {!selectedDatasetId ? (
                    <div className="flex-1 flex items-center justify-center text-muted-foreground flex-col gap-4">
                        <Bot className="h-12 w-12 opacity-20" />
                        <p>Select a dataset to begin the conversation.</p>
                    </div>
                ) : (
                    <>
                        {/* Messages Area */}
                        <div className="flex-1 overflow-y-auto p-4 space-y-6">
                            {messages.map(msg => (
                                <div key={msg.id} className={`flex gap-3 ${msg.role === "user" ? "flex-row-reverse" : ""}`}>
                                    <div className={`w-8 h-8 rounded-full flex items-center justify-center shrink-0 ${msg.role === "user" ? "bg-primary text-primary-foreground" : "bg-zinc-800 text-zinc-300"}`}>
                                        {msg.role === "user" ? <User className="h-4 w-4" /> : <Bot className="h-4 w-4" />}
                                    </div>
                                    <div className={`flex flex-col gap-2 max-w-[85%] ${msg.role === "user" ? "items-end" : "items-start"}`}>
                                        <div className={`p-3 rounded-2xl text-sm ${msg.role === "user" ? "bg-primary text-primary-foreground rounded-br-none" : "bg-zinc-900 border border-border/50 rounded-bl-none text-zinc-200"}`}>
                                            <div className="whitespace-pre-wrap">{msg.content}</div>
                                            {msg.isStreaming && <span className="inline-block w-1.5 h-4 ml-1 align-bottom bg-zinc-400 animate-pulse" />}
                                        </div>

                                        {/* Workflow Preview Card */}
                                        {msg.workflow && !msg.isStreaming && (
                                            <Card className="w-full max-w-lg mt-2 border-dashed border-primary/30 bg-primary/5">
                                                <CardHeader className="py-3 px-4 flex flex-row items-center justify-between space-y-0">
                                                    <div>
                                                        <CardTitle className="text-sm text-primary">Proposed Pipeline</CardTitle>
                                                        <CardDescription className="text-xs">Estimated reduction: {msg.workflow.estimated_rows_after.toLocaleString()} rows remaining.</CardDescription>
                                                    </div>
                                                </CardHeader>
                                                <CardContent className="px-4 pb-4">
                                                    <DndContext sensors={sensors} collisionDetection={closestCenter} onDragEnd={(e) => handleDragEnd(e, msg.id)}>
                                                        <SortableContext items={msg.workflow.steps.map(s => s.id)} strategy={verticalListSortingStrategy}>
                                                            {msg.workflow.steps.map((step) => (
                                                                <SortableWorkflowStep
                                                                    key={step.id}
                                                                    step={step}
                                                                    onRemove={() => setMessages(prev => prev.map(m => m.id === msg.id && m.workflow ? { ...m, workflow: { ...m.workflow, steps: m.workflow.steps.filter(s => s.id !== step.id) } } : m))}
                                                                    onConfigEdit={(newConf) => setMessages(prev => prev.map(m => m.id === msg.id && m.workflow ? { ...m, workflow: { ...m.workflow, steps: m.workflow.steps.map(s => s.id === step.id ? { ...s, config: newConf } : s) } } : m))}
                                                                />
                                                            ))}
                                                        </SortableContext>
                                                    </DndContext>
                                                    <Button className="w-full mt-2 gap-2 shadow-lg shadow-primary/20" size="sm" onClick={() => handleRunWorkflow(msg.workflow!, msg.id)}>
                                                        <Play className="h-4 w-4" /> Approve & Run Pipeline
                                                    </Button>
                                                </CardContent>
                                            </Card>
                                        )}

                                        {/* Suggestion Chips */}
                                        {msg.suggestions && !msg.isStreaming && (
                                            <div className="flex flex-wrap gap-2 mt-1">
                                                {msg.suggestions.map(sug => (
                                                    <button key={sug} onClick={() => handleSend(sug)} className="text-xs px-3 py-1.5 rounded-full bg-zinc-800/80 hover:bg-zinc-700 text-zinc-300 transition-colors border border-border/40">
                                                        {sug}
                                                    </button>
                                                ))}
                                            </div>
                                        )}
                                    </div>
                                </div>
                            ))}
                            <div ref={messagesEndRef} />
                        </div>

                        {/* Input Area */}
                        <div className="p-4 border-t border-border/50 bg-zinc-950">
                            <form onSubmit={(e) => { e.preventDefault(); handleSend(input); }} className="relative flex items-center">
                                <Input
                                    value={input}
                                    onChange={(e) => setInput(e.target.value)}
                                    placeholder={isTyping ? "Agent is typing..." : "Ask the agent to configure a pipeline..."}
                                    disabled={isTyping}
                                    className="pr-12 bg-zinc-900/50 border-zinc-800 h-12 rounded-xl"
                                />
                                <Button type="submit" disabled={!input.trim() || isTyping} size="icon" className="absolute right-1.5 h-9 w-9 rounded-lg">
                                    <Send className="h-4 w-4 mr-0.5 mt-0.5" />
                                </Button>
                            </form>
                            <div className="text-center mt-2">
                                <span className="text-[10px] text-muted-foreground uppercase tracking-widest">Powered by DataForge Agent</span>
                            </div>
                        </div>
                    </>
                )}
            </Card>
        </div>
    );
}

