# Agentic AI Financial Analyzer on GCP and Databricks

> End-to-end agentic pipeline that autonomously processes SEC 10-Q 
> filings, extracts KPIs, benchmarks against dynamically discovered 
> peers, validates against independent sources, and generates 
> structured investment briefs — reducing per-report processing time 
> from 3-4 hours to under 30 seconds.

---

## Architecture

![Architecture](docs/screenshots/mlflow_experiments.png)

### Two-plane design
- **GCP (Agent Plane)** — Cloud Run executes all 5 agents, 
  calls external APIs (SEC EDGAR, yfinance, Vertex AI)
- **Databricks (Data Plane)** — Delta Lake stores results, 
  MLflow tracks experiments, Lakehouse Monitoring detects drift

---

## Agents

| Agent | Role | Agentic Behavior |
|---|---|---|
| IngestionAgent | SEC EDGAR → GCS | — |
| ExtractionAgent | Gemini extracts KPIs | Reflects on own output, retries with focused prompt |
| BenchmarkingAgent | Dynamic peer scoring | 9-feature similarity ranking |
| ValidationAgent | Cross-check vs yfinance | LLM reasons about discrepancies |
| ReportWriterAgent | Gemini writes brief | Confidence-gated with caveats |

---

## Agentic Design

This system uses two agentic patterns on top of LangGraph orchestration:

**1. Reflection + Retry (ExtractionAgent)**  
After extracting KPIs, Gemini evaluates its own output for 
plausibility. If it flags a concern, the pipeline loops back 
with a focused prompt — bounded to 2 retries for auditability.

**2. LLM-as-Judge (ValidationAgent)**  
Rather than pure threshold rules, the validator asks Gemini to 
reason about whether EPS discrepancies are explainable — stock 
splits, basic vs diluted share count, one-time charges.

---

## Dynamic Peer Discovery

Peers are not hardcoded. For each target company:

1. Seed candidate pool from sector map
2. Fetch 9 features per candidate via yfinance
3. Min-max normalise all features
4. Compute weighted similarity score vs target
5. Rank and select top 5
6. Cache in Delta Lake with 90-day TTL

## Features and weights:
log_market_cap      0.15    ebitda_margin    0.15
roic                0.10    revenue_growth   0.10
net_debt/ebitda     0.10    asset_turnover   0.10
ev_to_revenue       0.08    pe_ratio         0.07
price_to_book       0.05    sub_industry     0.10


![Peer Scores](docs/screenshots/peer_discovery.png)

---

## Evaluation Framework

4-layer evaluation tracked in MLflow:

| Layer | Metric | Result |
|---|---|---|
| Extraction | Revenue accuracy (within 5%) | XX% |
| Extraction | Hallucination rate | XX% |
| Validation | F1 score | 0.XX |
| Report | LLM judge overall score | X.X/5 |
| Pipeline | Success rate | XX% |
| Pipeline | Avg latency | XXs |

---

## Observability

![MLflow](docs/screenshots/mlflow_nested_runs.png)

- **MLflow nested runs** — per-agent latency, tokens, confidence
- **Lakehouse Monitoring** — weekly drift reports on extracted_kpis
- **GCP Cloud Monitoring** — pipeline health alerts

![Monitoring](docs/screenshots/lakehouse_monitoring.png)

---

## HITL Review Queue

Low-confidence reports (score < 0.75) are routed to a 
Streamlit review UI instead of auto-publishing.

![Streamlit](docs/screenshots/streamlit_ui.png)

---

## Tech Stack

| Layer | Technology |
|---|---|
| Agent orchestration | LangGraph |
| LLM | Gemini Flash (Vertex AI) |
| PDF parsing | pdfplumber |
| Peer discovery | yfinance + similarity scoring |
| Storage | Delta Lake (Databricks) |
| Experiment tracking | MLflow (Databricks) |
| Data monitoring | Lakehouse Monitoring (Databricks) |
| Scheduling | Databricks Workflows |
| Serving | GCP Cloud Run Job |
| Review UI | Streamlit on GCP App Engine |
| CI/CD | GitHub Actions |

---

## Project Structure
agents/              5 specialized AI agents
pipeline/            LangGraph graph + runner
databricks_utils/    Delta Lake writer + notebooks
app/                 Streamlit HITL review UI

---

## Results

- XX SEC 10-Q filings processed across 5 retail companies
- XX quarters of historical data in Delta Lake
- Average pipeline latency: XXs per report
- Retry loop fired on XX% of reports
- HITL triggered on XX% of reports

## Author
Misty Roy
