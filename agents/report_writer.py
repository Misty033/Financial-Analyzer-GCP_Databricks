import os
import vertexai
from vertexai.generative_models import GenerativeModel
from agents.state import PipelineState
import json

vertexai.init(project=os.environ["GCP_PROJECT_ID"], location="us-central1")
model = GenerativeModel("gemini-2.5-flash-lite")


def build_prompt(state: PipelineState) -> str:
    kpis = state["extracted_kpis"]
    benchmarks = state["peer_benchmarks"]
    flagged = state["flagged_kpis"]

    # Build caveat note for flagged KPIs
    caveat = ""
    if flagged:
        caveat = f"\nNOTE: The following KPIs have low confidence and must include a caveat: {flagged}\n"

    return f"""
You are a senior financial analyst. Write a structured investment brief 
for {state['ticker']} based on their {state['quarter']} earnings report.

Use exactly these 5 sections with these headers:
## Executive Summary
## KPI Highlights  
## Peer Comparison
## Key Risks
## Outlook

Financial KPIs:
{json.dumps(kpis, indent=2)}

Sector Benchmarks:
- Sector median gross margin: {benchmarks.get('sector_median_gross_margin')}%
- Company gross margin: {benchmarks.get('company_gross_margin')}%
- Sector rank: {benchmarks.get('sector_rank')} of {benchmarks.get('total_peers')}
- Margin delta vs median: {benchmarks.get('margin_delta_vs_median')}%
{caveat}

Keep the brief concise — 400 words maximum.
For any flagged KPI, add: *(low confidence — verify against source)*
"""


def report_writer_agent(state: PipelineState) -> PipelineState:
    """
    Agent 5: Generate structured investment brief using Gemini.
    """
    print(f"[ReportWriterAgent] Starting for {state['ticker']}")

    if state.get("error"):
        return state

    prompt = build_prompt(state)
    response = model.generate_content(prompt)
    report = response.text

    print(f"[ReportWriterAgent] Report generated ({len(report)} chars)")
    return {**state, "final_report": report}