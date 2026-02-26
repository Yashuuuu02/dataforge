"use client";

import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { GitBranch, Plus } from "lucide-react";

export default function WorkflowsPage() {
    return (
        <div className="space-y-8">
            {/* Header */}
            <div className="flex items-center justify-between">
                <div>
                    <h1 className="text-3xl font-bold tracking-tight">Workflows</h1>
                    <p className="mt-2 text-muted-foreground">
                        Create and manage reusable data processing pipelines
                    </p>
                </div>
                <Button className="gap-2">
                    <Plus className="h-4 w-4" />
                    Create Workflow
                </Button>
            </div>

            {/* Empty state */}
            <Card className="border-dashed">
                <CardContent className="flex flex-col items-center justify-center py-16">
                    <div className="rounded-full bg-primary/10 p-4">
                        <GitBranch className="h-10 w-10 text-primary" />
                    </div>
                    <h3 className="mt-4 text-lg font-semibold">No workflows yet</h3>
                    <p className="mt-2 max-w-sm text-center text-sm text-muted-foreground">
                        Workflows let you define reusable data processing pipelines. Create your first workflow to streamline your data preparation.
                    </p>
                    <Button className="mt-6 gap-2">
                        <Plus className="h-4 w-4" />
                        Create Your First Workflow
                    </Button>
                </CardContent>
            </Card>
        </div>
    );
}
