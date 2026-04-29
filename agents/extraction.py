import os
import json
import vertexai
from vertexai.generative_models import FunctionDeclaration, GenerativeModel, Tool, ToolConfig
from agents.state import PipelineState

vertexai.init(project=os.environ["GCP_PROJECT_ID"], location="us-central1")
model = GenerativeModel("gemini-2.5-flash-lite")

MAX_RETRIES = 2  # maximum reflection-retry cycles

KPI_TOOL_NAME = "return_extracted_kpis"
REFLECTION_TOOL_NAME = "return_kpi_reflection"

KPI_TOOL = Tool(
    function_declarations=[
        FunctionDeclaration(
            name=KPI_TOOL_NAME,
            description="Return financial KPIs extracted from a filing text chunk.",
            parameters={
                "type": "object",
                "properties": {
                    "revenue_usd_millions": {
                        "type": "number",
                        "description": "Total net sales or revenue in USD millions.",
                    },
                    "net_income_usd_millions": {
                        "type": "number",
                        "description": "Net income attributable to the company in USD millions.",
                    },
                    "eps_diluted": {
                        "type": "number",
                        "description": "Diluted earnings per share.",
                    },
                    "gross_margin_pct": {
                        "type": "number",
                        "description": "Gross profit as a percentage of revenue.",
                    },
                    "operating_cash_flow": {
                        "type": "number",
                        "description": "Operating cash flow in USD millions.",
                    },
                    "revenue_yoy_growth_pct": {
                        "type": "number",
                        "description": "Year-over-year revenue growth percentage.",
                    },
                },
            },
        )
    ]
)

REFLECTION_TOOL = Tool(
    function_declarations=[
        FunctionDeclaration(
            name=REFLECTION_TOOL_NAME,
            description="Return a quality review of extracted financial KPIs.",
            parameters={
                "type": "object",
                "properties": {
                    "looks_correct": {
                        "type": "boolean",
                        "description": "Whether the KPI extraction looks correct.",
                    },
                    "concern": {
                        "type": "string",
                        "description": "Specific concern, or 'none' if there is no concern.",
                    },
                    "fields_to_recheck": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "KPI field names that should be rechecked.",
                    },
                },
                "required": ["looks_correct", "concern", "fields_to_recheck"],
            },
        )
    ]
)


def tool_config(function_name: str) -> ToolConfig:
    return ToolConfig(
        function_calling_config=ToolConfig.FunctionCallingConfig(
            mode=ToolConfig.FunctionCallingConfig.Mode.ANY,
            allowed_function_names=[function_name],
        )
    )


def get_function_args(response, function_name: str) -> dict:
    for candidate in response.candidates:
        for function_call in candidate.function_calls:
            if function_call.name == function_name:
                return dict(function_call.args)
    return {}


def chunk_text(text: str, max_chars: int = 8000) -> list[str]:
    return [text[i:i+max_chars] for i in range(0, min(len(text), 40000), max_chars)]


def extract_kpis_from_chunk(chunk: str, focus_hint: str = "") -> dict:
    """
    Extract KPIs from one chunk.
    focus_hint is passed on retry so the LLM knows what to look harder for.
    """
    focus_section = ""
    if focus_hint:
        focus_section = f"""
IMPORTANT: On a previous attempt the following concern was raised:
{focus_hint}
Pay extra attention to finding the correct values for the flagged fields.
Look for alternative phrasings, tables, or footnotes that may contain them.
"""

    prompt = f"""
You are a financial analyst extracting KPIs from a quarterly earnings filing.
Call the {KPI_TOOL_NAME} tool with the extracted KPI values.
If a value is not found in this chunk, omit that field.
{focus_section}

Keys required:
- revenue_usd_millions (float, total net sales/revenue in millions)
- net_income_usd_millions (float, net income attributable to company in millions)
- eps_diluted (float, diluted earnings per share)
- gross_margin_pct (float, gross profit as % of revenue e.g. 24.5)
- operating_cash_flow (float, operating cash flow in millions)
- revenue_yoy_growth_pct (float, year-over-year revenue growth %)

Text:
{chunk}
"""
    try:
        response = model.generate_content(
            prompt,
            tools=[KPI_TOOL],
            tool_config=tool_config(KPI_TOOL_NAME),
        )
        return get_function_args(response, KPI_TOOL_NAME)
    except Exception:
        return {}


def merge_kpis(kpi_list: list[dict]) -> dict:
    merged = {
        "revenue_usd_millions":     None,
        "net_income_usd_millions":  None,
        "eps_diluted":              None,
        "gross_margin_pct":         None,
        "operating_cash_flow":      None,
        "revenue_yoy_growth_pct":   None,
    }
    for kpis in kpi_list:
        for key in merged:
            if merged[key] is None and kpis.get(key) is not None:
                merged[key] = kpis[key]
    return merged


def reflect_on_kpis(kpis: dict, ticker: str) -> dict:
    """
    Ask Gemini to evaluate its own extracted KPIs.
    Returns: {looks_correct: bool, concern: str, fields_to_recheck: list}
    This is the agentic step — LLM judges its own output.
    """
    prompt = f"""
You are a senior financial analyst reviewing KPI extraction results for {ticker}.
Evaluate whether these extracted values are reasonable for a large US company.

Extracted KPIs:
{json.dumps(kpis, indent=2)}

Check for these issues:
1. Are any required fields null that should realistically be present?
2. Are any values implausible (e.g. revenue < 0, gross margin > 100% or < 0%)?
3. Are any values suspiciously round numbers that might be placeholders?
4. Does EPS seem consistent with net income (if both present)?
5. Does gross margin seem reasonable for this type of company?

Call the {REFLECTION_TOOL_NAME} tool with your review.
"""
    try:
        response = model.generate_content(
            prompt,
            tools=[REFLECTION_TOOL],
            tool_config=tool_config(REFLECTION_TOOL_NAME),
        )
        reflection = get_function_args(response, REFLECTION_TOOL_NAME)
        return reflection or {
            "looks_correct": True,
            "concern": "none",
            "fields_to_recheck": [],
        }
    except Exception as e:
        print(f"[ExtractionAgent] Reflection failed: {e}")
        # If reflection itself fails, assume KPIs are fine and continue
        return {"looks_correct": True, "concern": "none", "fields_to_recheck": []}


def run_extraction(raw_text: str, focus_hint: str = "") -> tuple[dict, int]:
    """Run extraction across all chunks and return merged KPIs + token count."""
    chunks = chunk_text(raw_text)
    all_kpis = []
    total_tokens = 0

    for i, chunk in enumerate(chunks):
        kpis = extract_kpis_from_chunk(chunk, focus_hint=focus_hint)
        all_kpis.append(kpis)
        total_tokens += len(chunk.split())
        print(f"[ExtractionAgent] Chunk {i+1}/{len(chunks)}: {kpis}")

        merged = merge_kpis(all_kpis)
        if all(v is not None for v in merged.values()):
            print("[ExtractionAgent] All KPIs found — stopping early")
            break

    return merge_kpis(all_kpis), total_tokens


def extraction_agent(state: PipelineState) -> PipelineState:
    """
    Agent 2: Extract KPIs with agentic reflect-and-retry loop.
    The LLM extracts, evaluates its own output, and retries if needed.
    """
    print(f"[ExtractionAgent] Starting for {state['ticker']} {state['quarter']}")

    if state.get("error"):
        return state

    retry_count    = state.get("retry_count", 0)
    focus_hint     = state.get("reflection_notes") or ""
    total_tokens   = state.get("tokens_used", 0)

    # ── Step 1: Extract ───────────────────────────────────────
    print(f"[ExtractionAgent] Attempt {retry_count + 1}/{MAX_RETRIES + 1}"
          + (f" — focus: {focus_hint[:80]}" if focus_hint else ""))

    kpis, tokens = run_extraction(state["raw_text"], focus_hint=focus_hint)
    total_tokens += tokens

    print(f"[ExtractionAgent] Extracted: {kpis}")

    # ── Step 2: Reflect on own output ─────────────────────────
    print("[ExtractionAgent] Reflecting on extraction quality...")
    reflection = reflect_on_kpis(kpis, state["ticker"])
    print(f"[ExtractionAgent] Reflection: {reflection}")

    # ── Step 3: Decide — accept or flag for retry ─────────────
    if reflection["looks_correct"]:
        print("[ExtractionAgent] Reflection passed — KPIs accepted")
        return {
            **state,
            "extracted_kpis":   kpis,
            "tokens_used":      total_tokens,
            "reflection_notes": reflection["concern"],
            # retry_count unchanged — extraction succeeded
        }
    else:
        print(f"[ExtractionAgent] Reflection flagged issue: {reflection['concern']}")
        # Store concern as reflection_notes so next retry uses it as focus_hint
        # Increment retry_count so graph knows to loop back
        return {
            **state,
            "extracted_kpis":   kpis,       # keep current best attempt
            "tokens_used":      total_tokens,
            "reflection_notes": reflection["concern"],
            "retry_count":      retry_count + 1,
        }
