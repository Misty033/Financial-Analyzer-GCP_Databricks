from langgraph.graph import StateGraph, END
from agents.state import PipelineState

MAX_RETRIES = 2


def route_after_extraction(state: PipelineState) -> str:
    retry_count      = state.get("retry_count", 0)
    reflection_notes = state.get("reflection_notes", "")

    if (reflection_notes
            and reflection_notes != "none"
            and retry_count > 0
            and retry_count <= MAX_RETRIES):
        print(f"[Router] Extraction retry {retry_count}/{MAX_RETRIES}")
        return "extraction"

    return "benchmarking"


def route_after_validation(state: PipelineState) -> str:
    if state.get("error"):
        return "end"

    retry_count = state.get("retry_count", 0)
    confidence  = state.get("confidence_score", 0)
    hitl        = state.get("hitl_required", False)

    if confidence < 0.75 and retry_count < MAX_RETRIES:
        print(f"[Router] Validation failed (conf={confidence}) "
              f"— retrying extraction ({retry_count}/{MAX_RETRIES})")
        return "extraction"

    if hitl:
        return "end"

    return "report_writer"


def build_pipeline(
    ingestion_fn=None,
    extraction_fn=None,
    benchmarking_fn=None,
    validation_fn=None,
    report_writer_fn=None,
):
    """
    Build the LangGraph pipeline.
    Accepts optional wrapped agent functions so runner.py
    can inject MLflow-tracked versions without changing graph logic.
    Falls back to plain agent imports if not provided.
    """
    from agents.ingestion import ingestion_agent
    from agents.extraction import extraction_agent
    from agents.benchmarking import benchmarking_agent
    from agents.validation import validation_agent
    from agents.report_writer import report_writer_agent

    graph = StateGraph(PipelineState)

    graph.add_node("ingestion",     ingestion_fn     or ingestion_agent)
    graph.add_node("extraction",    extraction_fn    or extraction_agent)
    graph.add_node("benchmarking",  benchmarking_fn  or benchmarking_agent)
    graph.add_node("validation",    validation_fn    or validation_agent)
    graph.add_node("report_writer", report_writer_fn or report_writer_agent)

    graph.set_entry_point("ingestion")
    graph.add_edge("ingestion", "extraction")

    graph.add_conditional_edges(
        "extraction",
        route_after_extraction,
        {"extraction": "extraction", "benchmarking": "benchmarking"}
    )

    graph.add_edge("benchmarking", "validation")

    graph.add_conditional_edges(
        "validation",
        route_after_validation,
        {"extraction": "extraction", "report_writer": "report_writer", "end": END}
    )

    graph.add_edge("report_writer", END)

    return graph.compile()