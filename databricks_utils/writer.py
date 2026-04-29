import os
import json
from datetime import datetime
from databricks import sql


def get_connection():
    return sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"].replace("https://", ""),
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"]
    )


def write_to_databricks(state: dict):
    """Write all agent results to Delta Lake tables."""
    conn = get_connection()
    cursor = conn.cursor()
    now = datetime.utcnow().isoformat()

    # Write to raw_filings
    cursor.execute("""
        INSERT INTO main.financial.raw_filings
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        state["ticker"],
        state["quarter"],
        now[:10],
        state.get("gcs_pdf_path", ""),
        "10-Q",
        "error" if state.get("error") else "success",
        now
    ))

    if state.get("error"):
        conn.close()
        return

    # Write to extracted_kpis
    kpis = state.get("extracted_kpis", {})
    cursor.execute("""
        INSERT INTO main.financial.extracted_kpis
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        state["ticker"],
        state["quarter"],
        kpis.get("revenue_usd_millions"),
        kpis.get("net_income_usd_millions"),
        kpis.get("eps_diluted"),
        kpis.get("gross_margin_pct"),
        kpis.get("operating_cash_flow"),
        kpis.get("revenue_yoy_growth_pct"),
        state.get("confidence_score", 0),
        json.dumps(state.get("flagged_kpis", [])),
        state.get("hitl_required", False),
        False,
        state.get("pipeline_version", "1.0.0"),
        now
    ))

    # Write to peer_benchmarks
    bench      = state.get("peer_benchmarks", {})
    peers_used = ",".join(bench.get("peers", []))

    cursor.execute("""
        INSERT INTO main.financial.peer_benchmarks
            (ticker, quarter, sector_median_margin,
            relative_rank, margin_delta_pct, benchmark_ts, peers_used)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        state["ticker"],
        state["quarter"],
        bench.get("sector_median_gross_margin"),
        bench.get("sector_rank"),
        bench.get("margin_delta_vs_median"),
        now,
        peers_used,
    ))

    # Write investment brief
    if state.get("final_report"):
        cursor.execute("""
            INSERT INTO main.financial.investment_briefs
            VALUES (?, ?, ?, ?, ?)
        """, (
            state["ticker"],
            state["quarter"],
            state["final_report"],
            state.get("run_id", ""),
            now
        ))

    conn.close()
    print(f"[DatabricksWriter] All tables updated for "
          f"{state['ticker']} {state['quarter']}")