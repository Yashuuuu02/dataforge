"""The core AI Agent for dataset conversation and pipeline creation."""

import json
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional
from uuid import uuid4

from ai.litellm_client import LiteLLMClient
from ai.dataset_analyzer import DatasetAnalysis
from ai.workflow_builder import WorkflowBuilder, WorkflowPlan

@dataclass
class AgentSession:
    session_id: str
    dataset_id: str
    conversation_history: List[Dict[str, str]]
    current_workflow: Optional[WorkflowPlan]
    dataset_analysis: Optional[DatasetAnalysis]
    pending_clarification: bool = False

    def to_dict(self) -> dict:
        return {
            "session_id": self.session_id,
            "dataset_id": self.dataset_id,
            "conversation_history": self.conversation_history,
            "current_workflow": self.current_workflow.to_dict() if self.current_workflow else None,
            "dataset_analysis": asdict(self.dataset_analysis) if self.dataset_analysis else None,
            "pending_clarification": self.pending_clarification
        }

    @classmethod
    def from_dict(cls, data: dict):
        # minimal mock for loading back from dict
        wf = data.get("current_workflow")
        # reconstruct instances if needed (in a full app we'd map this properly)
        return cls(
            session_id=data["session_id"],
            dataset_id=data.get("dataset_id", ""),
            conversation_history=data.get("conversation_history", []),
            current_workflow=WorkflowPlan(**wf) if wf else None,
            dataset_analysis=DatasetAnalysis(**data["dataset_analysis"]) if data.get("dataset_analysis") else None,
            pending_clarification=data.get("pending_clarification", False)
        )

@dataclass
class AgentResponse:
    message: str
    action: str  # "chat" | "workflow_ready" | "clarify" | "run_approved" | "error"
    workflow: Optional[WorkflowPlan] = None
    suggestions: List[str] = None


class DataForgeAgent:
    def __init__(self, llm_client: LiteLLMClient, workflow_builder: WorkflowBuilder):
        self.llm = llm_client
        self.builder = workflow_builder

    async def chat(self, user_message: str, session: AgentSession) -> AgentResponse:
        """Process a user conversational turn."""
        if not self.llm:
            return AgentResponse(
                 message="I'm running in heuristic mode (no API key configured). You can configure a pipeline from the UI directly, or add an API key in settings for AI features.",
                 action="error",
                 workflow=session.current_workflow,
                 suggestions=["Go to Settings", "Cancel"]
            )

        # Update History
        session.conversation_history.append({"role": "user", "content": user_message})

        # Base Prompt
        system_prompt = """You are DataForge Agent, an expert AI assistant that helps users prepare datasets for machine learning. 
Your goal is to guide the user to create a pipeline (workflow) capable of cleaning or structuring their data appropriately. 

Use function calling to perform your actions.
- If the user wants a pipeline built: use `build_pipeline`
- If the user wants a pipeline modified: use `refine_pipeline`
- If the user is approving the pipeline and wants to run it: use `run_approved_pipeline`
- Otherwise, converse naturally and ask questions to clarify their goal.

Always be polite, concise, and focused on data tasks."""

        analysis_str = json.dumps(asdict(session.dataset_analysis)) if session.dataset_analysis else "Not yet analyzed."

        tools = [
            {
                "type": "function",
                "function": {
                    "name": "build_pipeline",
                    "description": "Constructs a new data cleaning pipeline for the user from their requirements.",
                    "parameters": {"type": "object", "properties": {}, "required": []}
                }
            },
            {
                "type": "function",
                "function": {
                    "name": "refine_pipeline",
                    "description": "Modifies the currently pending pipeline according to user feedback.",
                    "parameters": {"type": "object", "properties": {}, "required": []}
                }
            },
            {
                "type": "function",
                "function": {
                     "name": "run_approved_pipeline",
                     "description": "Call this to indicate the user has approved the pipeline and wants to execute it now.",
                     "parameters": {"type": "object", "properties": {}, "required": []}
                }
            }
        ]

        messages = [
            {"role": "system", "content": system_prompt + f"\n\nDataset Info: {analysis_str}"},
        ] + session.conversation_history

        # Use LiteLLM directly to get tool calls
        try:
             res = await litellm.acompletion(
                 model=self.llm._prepare_kwargs()["model"],
                 api_key=self.llm.api_key,
                 messages=messages,
                 tools=tools,
                 tool_choice="auto",
                 temperature=0.7
             )
             msg = res.choices[0].message
        except Exception as e:
             return AgentResponse(f"I encountered an error connecting to the AI provider: {e}", "error")

        if msg.tool_calls:
             call = msg.tool_calls[0]
             name = call.function.name
             
             if name == "build_pipeline":
                  plan = await self.builder.build_from_text(user_message, session.dataset_analysis, session.conversation_history)
                  session.current_workflow = plan
                  res_msg = f"I've drafted a pipeline that will {plan.explanation.lower()} Here are the {len(plan.steps)} steps I recommend. Shall we run it, or would you like to tweak it?"
                  session.conversation_history.append({"role": "assistant", "content": res_msg})
                  
                  return AgentResponse(
                      message=res_msg,
                      action="workflow_ready",
                      workflow=plan,
                      suggestions=["Run pipeline", "Remove PII step", "Increase quality threshold"]
                  )
             
             elif name == "refine_pipeline":
                  if not session.current_workflow:
                       return AgentResponse("I need a pipeline to refine first. What are we trying to achieve?", "chat", suggestions=["Clean the dataset"])
                       
                  plan = await self.builder.refine(session.current_workflow, user_message, session.conversation_history)
                  session.current_workflow = plan
                  res_msg = "I've updated the pipeline config as you requested. Ready to execute it?"
                  session.conversation_history.append({"role": "assistant", "content": res_msg})
                  
                  return AgentResponse(
                      message=res_msg,
                      action="workflow_ready",
                      workflow=plan,
                      suggestions=["Run pipeline", "Looks good"]
                  )
                  
             elif name == "run_approved_pipeline":
                  if not session.current_workflow:
                       return AgentResponse("You have no pending pipeline configured to run.", "error")
                       
                  res_msg = "Excellent. I'm dispatching the job now. You'll be redirected to monitor its progress."
                  session.conversation_history.append({"role": "assistant", "content": res_msg})
                  return AgentResponse(
                      message=res_msg,
                      action="run_approved",
                      workflow=session.current_workflow,
                      suggestions=[]
                  )

        # Normal Chat logic
        content = msg.content or "I couldn't quite understand that. Are you trying to prepare this dataset for AI training?"
        session.conversation_history.append({"role": "assistant", "content": content})
        
        return AgentResponse(
            message=content,
            action="chat",
            workflow=session.current_workflow,
            suggestions=["Build a pipeline to clean this data", "Extract a sample for testing"]
        )

import litellm # needed for acompletion above locally
