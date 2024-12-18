from __future__ import annotations

import os
import json
import typing as t
from pathlib import Path

import openai
from openai import OpenAI
from sqlmesh.core.prompts.cube_docs import get_all_docs


class OpenAIClient:
    """Client for interacting with OpenAI API."""

    DEFAULT_MODEL = "o1-preview"  # Using o1 model
    
    def __init__(
        self,
        api_key: t.Optional[str] = None,
        organization: t.Optional[str] = None,
        model: t.Optional[str] = None,
    ):
        """Initialize OpenAI client.
        
        Args:
            api_key: OpenAI API key. If not provided, will look for OPENAI_API_KEY env var
            organization: OpenAI organization ID. If not provided, will look for OPENAI_ORG_ID env var
            model: OpenAI model to use. Defaults to most capable model
        """
        self.api_key = api_key or os.getenv("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError(
                "OpenAI API key not provided. Set OPENAI_API_KEY environment variable or pass api_key parameter."
            )
        
        self.organization = organization or os.getenv("OPENAI_ORG_ID")
        self.model = model or self.DEFAULT_MODEL
        
        self.client = OpenAI(
            api_key=self.api_key,
            organization=self.organization,
        )

    def generate_yaml(
        self,
        json_input: dict,
        sql_input: str,
        prompt_template: str
    ) -> str:
        """Generate YAML from JSON and SQL input using OpenAI.
        
        Args:
            json_input: JSON data to convert to YAML
            sql_input: SQL definitions of the models
            prompt_template: Template string containing instructions for the model
            
        Returns:
            Generated YAML string
        """
        # Get Cube documentation
        cube_docs = get_all_docs()
        
        # Replace placeholders in template with actual inputs
        prompt = (
            "You are a data modeling expert specializing in Cube Semantic Layer modeling.\n\n" +
            prompt_template.replace("{{input_json}}", json.dumps(json_input, indent=2))
            .replace("{{input_sql}}", sql_input)
            .replace("{{cube_docs}}", cube_docs)
        )
        
        response = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        
        return response.choices[0].message.content
