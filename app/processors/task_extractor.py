import json
import logging
from enum import Enum
from typing import Optional

from langchain_groq import ChatGroq
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.output_parsers import PydanticOutputParser
from pydantic import BaseModel, Field

from app.processors.base import LiveProcessor
from app.config import settings

logger = logging.getLogger("ai-worker")

class PriorityEnum(str, Enum):
    high = "high"
    medium = "medium"
    low = "low"

class ToDo(BaseModel):
    description: str = Field(description="The explicitly stated task or action item")
    reminder: Optional[str] = Field(description="When the task needs to be reminded or completed by, if explicitly stated, otherwise null", default=None)
    priority: Optional[PriorityEnum] = Field(description="Priority of the task if explicitly stated, otherwise null", default=None)

class ToDoList(BaseModel):
    todos: list[ToDo] = Field(description="List of extracted ToDos")

class TaskExtractorProcessor(LiveProcessor):
    def __init__(self, nc):
        """
        Args:
            nc: The NATS client connection.
        """
        self.nc = nc
        self.subject = settings.nats_todos_subject
        
        # Initialize Groq LLM using the provided model and api key
        self.llm = ChatGroq(
            api_key=settings.groq_api_key, 
            model_name=settings.groq_extraction_model,
            temperature=0.0 # Lowest temperature for strict extraction
        )
        
        self.parser = PydanticOutputParser(pydantic_object=ToDoList)
        
        self.prompt = ChatPromptTemplate.from_messages([
            ("system", """You are an expert assistant designed to extract explicit tasks or action items from dictated transcripts.
            
EXTRACT ONLY EXPLICITLY STATED TASKS OR ACTION ITEMS.
DO NOT include general thoughts, background context, ideas, or observations.
IF YOU ARE UNSURE IF SOMETHING IS A TASK, DO NOT CREATE IT.

{format_instructions}"""),
            ("user", "Transcript:\n{transcript}")
        ])
        
        self.chain = self.prompt | self.llm | self.parser
        
    async def process(self, transcription_text: str, metadata: dict) -> None:
        logger.info(f"TaskExtractorProcessor: Starting extraction for file {metadata.get('filename', 'unknown')}")
        
        try:
            result: ToDoList = await self.chain.ainvoke({
                "transcript": transcription_text, 
                "format_instructions": self.parser.get_format_instructions()
            })
            
            extracted_todos = result.todos
            logger.info(f"TaskExtractorProcessor: Extracted {len(extracted_todos)} ToDos")
            
            for todo in extracted_todos:
                payload = {
                    "todo": todo.model_dump(mode='json'),
                    "metadata": metadata
                }
                logger.info(f"TaskExtractorProcessor: Publishing ToDo to {self.subject}: {todo.description}")
                logger.info(f"Payload: {json.dumps(payload)}")
                await self.nc.publish(self.subject, json.dumps(payload).encode())
                
        except Exception as e:
            logger.error(f"TaskExtractorProcessor: Error extracting ToDos: {e}", exc_info=True)
