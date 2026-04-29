from typing import TypedDict, Optional
from pydantic import BaseModel


class FinancialKPIs(BaseModel):
    revenue_usd_millions: float
    net_income_usd_millions: float
    eps_diluted: float
    gross_margin_pct: float
    operating_cash_flow: float
    revenue_yoy_growth_pct: Optional[float] = None


class PipelineState(TypedDict):
    # Inputs
    ticker: str
    quarter: str                    

    # IngestionAgent outputs
    gcs_pdf_path: str
    raw_text: str

    # ExtractionAgent outputs
    extracted_kpis: Optional[dict]
    tokens_used: int

    # BenchmarkingAgent outputs
    peer_benchmarks: Optional[dict]

    # ValidationAgent outputs
    confidence_score: float
    flagged_kpis: list
    hitl_required: bool

    # ── New agentic fields ──────────────────────────
    retry_count: int                    # how many times extraction retried
    reflection_notes: Optional[str]     # what LLM said was wrong last time
    validation_reasoning: Optional[str] # why validator accepted/rejected
    # ────────────────────────────────────────────────

    # ReportWriterAgent outputs
    final_report: Optional[str]

    # Metadata
    run_id: str
    pipeline_version: str
    error: Optional[str]