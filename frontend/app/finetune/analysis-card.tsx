export function DatasetAnalysisCard({ analysis }: { analysis: any }) {
    if (!analysis) return null;
    return (
        <div className="rounded-lg border bg-muted/50 p-4 space-y-4">
            <div className="font-semibold text-sm">Dataset Analysis Highlights</div>
            <p className="text-sm text-muted-foreground">{analysis?.summary || "No summary available."}</p>
            <div className="flex gap-2 flex-wrap">
                {analysis?.duplicate_ratio > 0.05 && (
                    <Badge variant="destructive">Warning: {Math.round(analysis.duplicate_ratio * 100)}% duplicates</Badge>
                )}
                {analysis?.has_pii && (
                    <Badge variant="destructive">PII Detected</Badge>
                )}
                <Badge variant="secondary">{analysis?.detected_type || "Unknown Type"}</Badge>
            </div>
        </div>
    );
}
