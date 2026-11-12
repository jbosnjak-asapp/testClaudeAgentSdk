from typing import Any, Optional

from claude_agent_sdk import ClaudeAgentOptions
from temporalio import activity


def readonly_agent_options(
    cwd: Optional[str],
    max_turns: int = 10,
    output_format: Optional[dict[str, Any]] = None,
) -> ClaudeAgentOptions:
    """Locked-down options for an unattended Claude agent.

    - Only Read/Grep/Glob exist (`tools`); `allowed_tools` auto-approves them.
    - No user/project settings, plugins, hooks or skills (`setting_sources=[]`).
    - No MCP servers from the machine's config (`strict_mcp_config`).
    """
    return ClaudeAgentOptions(
        cwd=cwd,
        max_turns=max_turns,
        tools=["Read", "Grep", "Glob"],
        allowed_tools=["Read", "Grep", "Glob"],
        setting_sources=[],
        strict_mcp_config=True,
        output_format=output_format,
        # Reading a PDF/image yields one large JSON message (base64 content);
        # the default 1MB buffer is too small for that.
        max_buffer_size=32 * 1024 * 1024,
        stderr=lambda line: activity.logger.warning("claude stderr: %s", line),
    )
