"""Registries agregados de tools, resources e prompts (Fase 1 do ROADMAP)."""

from gateway.registries.prompt_registry import PromptRegistry
from gateway.registries.resource_registry import ResourceRegistry
from gateway.registries.tool_registry import ToolRegistry

__all__ = ["PromptRegistry", "ResourceRegistry", "ToolRegistry"]
