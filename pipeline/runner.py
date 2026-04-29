import os
import uuid
import time
from dotenv import load_dotenv
load_dotenv()
load_dotenv(".env.local")

import mlflow
from agents.state import PipelineState
from agents.ingestion import ingestion_agent
from agents.extraction import extraction_agent
from agents.benchmarking import benchmarking_agent
from agents.validation import validation_agent
from agents.report_writer import report_writer_agent
from pipeline.graph import build_pipeline
from databricks_utils.writer import write_to_databricks

mlflow.set_tracking_uri("databricks")
mlflow.set_experiment("/Shared/financial-analyst-pipeline")


def tracked(agent_fn, agent_name):
    """
    Returns a wrapped version of agent_fn that logs to MLflow.
    This wrapper is what gets registered as a node in the graph.
    """
    def wrapper(state: PipelineState) -> PipelineState:
        with mlflow.start_run(run_name=agent_name, nested=True):
            start = time.time()
            result = agent_fn(state)
            latency = round(time.time() - start, 2)

            mlflow.log_metric("latency_sec", latency)
            mlflow.log_metric("retry_count", result.get("retry_count", 0))
            mlflow.log_metric("had_error",   int(bool(result.get("error"))))

            if agent_name == "extraction_agent":
                mlflow.log_metric("tokens_used", result.get("tokens_used", 0))
                mlflow.log_metric(
                    "kpi_fields_found",
                    sum(1 for v in (result.get("extracted_kpis") or {}).values()
                        if v is not None)
                )
                if result.get("extracted_kpis"):
                    mlflow.log_dict(result["extracted_kpis"], "extracted_kpis.json")
                if result.get("reflection_notes"):
                    mlflow.log_text(result["reflection_notes"], "reflection_notes.txt")

            elif agent_name == "validation_agent":
                mlflow.log_metric("confidence_score",  result.get("confidence_score", 0))
                mlflow.log_metric("flagged_kpi_count", len(result.get("flagged_kpis", [])))
                mlflow.log_metric("hitl_required",     int(result.get("hitl_required", False)))
                if result.get("validation_reasoning"):
                    mlflow.log_text(
                        result["validation_reasoning"],
                        "validation_reasoning.txt"
                    )

            elif agent_name == "report_writer_agent":
                report = result.get("final_report") or ""
                mlflow.log_metric("report_length_chars", len(report))
                if report:
                    mlflow.log_text(report, "investment_brief.md")

            print(f"[MLflow] {agent_name} logged — latency: {latency}s")
            return result
    return wrapper


def run_pipeline(ticker: str, quarter: str):
    # End any stale run from previous crash
    if mlflow.active_run():
        mlflow.end_run()

    run_id = str(uuid.uuid4())[:8]

    initial_state = {
        "ticker":               ticker,
        "quarter":              quarter,
        "gcs_pdf_path":         "",
        "raw_text":             "",
        "extracted_kpis":       None,
        "tokens_used":          0,
        "peer_benchmarks":      None,
        "confidence_score":     0.0,
        "flagged_kpis":         [],
        "hitl_required":        False,
        "final_report":         None,
        "retry_count":          0,
        "reflection_notes":     None,
        "validation_reasoning": None,
        "run_id":               run_id,
        "pipeline_version":     "1.0.0",
        "error":                None,
    }

    with mlflow.start_run(run_name=f"{ticker}_{quarter}"):
        mlflow.log_params({
            "ticker":           ticker,
            "quarter":          quarter,
            "pipeline_version": "1.0.0",
            "model":            "gemini-2.5-flash-lite"
        })

        pipeline_start = time.time()

        # Build graph with tracked agents
        # The graph handles ALL routing including retry loops
        pipeline = build_pipeline(
            ingestion_fn=    tracked(ingestion_agent,    "ingestion_agent"),
            extraction_fn=   tracked(extraction_agent,   "extraction_agent"),
            benchmarking_fn= tracked(benchmarking_agent, "benchmarking_agent"),
            validation_fn=   tracked(validation_agent,   "validation_agent"),
            report_writer_fn=tracked(report_writer_agent,"report_writer_agent"),
        )

        # invoke() now uses the full graph with retry edges
        final_state = pipeline.invoke(initial_state)

        total_time = round(time.time() - pipeline_start, 2)

        mlflow.log_metrics({
            "total_latency_sec": total_time,
            "confidence_score":  final_state.get("confidence_score", 0),
            "tokens_used":       final_state.get("tokens_used", 0),
            "flagged_kpi_count": len(final_state.get("flagged_kpis", [])),
            "hitl_required":     int(final_state.get("hitl_required", False)),
            "pipeline_success":  0 if final_state.get("error") else 1,
            "retry_count":       final_state.get("retry_count", 0),
        })

        if final_state.get("final_report"):
            mlflow.log_text(final_state["final_report"], "investment_brief.md")
        if final_state.get("extracted_kpis"):
            mlflow.log_dict(final_state["extracted_kpis"], "extracted_kpis.json")

        write_to_databricks(final_state)

        print(f"\n{'='*50}")
        print(f"Pipeline complete for {ticker} {quarter}")
        print(f"Total time:    {total_time}s")
        print(f"Confidence:    {final_state.get('confidence_score')}")
        print(f"Retry count:   {final_state.get('retry_count')}")
        print(f"HITL required: {final_state.get('hitl_required')}")
        if final_state.get("final_report"):
            print(f"\n--- Investment Brief ---")
            print(f"{final_state['final_report'][:500]}...")
        print(f"{'='*50}\n")

        return final_state


if __name__ == "__main__":
    ticker  = os.environ.get("TICKER",  "WMT")
    quarter = os.environ.get("QUARTER", "2024-Q3")
    run_pipeline(ticker, quarter)