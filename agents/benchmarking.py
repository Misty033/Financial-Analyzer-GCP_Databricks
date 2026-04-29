import yfinance as yf
from agents.state import PipelineState
from agents.peer_discovery import discover_peers


def get_peer_metrics(tickers: list[str]) -> dict:
    metrics = {}
    for ticker in tickers:
        try:
            info = yf.Ticker(ticker).info
            metrics[ticker] = {
                "gross_margin_pct":  round((info.get("grossMargins") or 0) * 100, 2),
                "pe_ratio":          info.get("trailingPE"),
                "revenue_growth":    round((info.get("revenueGrowth") or 0) * 100, 2),
                "ebitda_margin":     round((info.get("ebitdaMargins") or 0) * 100, 2),
            }
        except Exception as e:
            print(f"[BenchmarkingAgent] Could not fetch {ticker}: {e}")
    return metrics


def benchmarking_agent(state: PipelineState) -> PipelineState:
    print(f"[BenchmarkingAgent] Starting for "
          f"{state['ticker']} {state['quarter']}")

    if state.get("error"):
        return state

    # Dynamic peer discovery — cache aware
    peers = discover_peers(state["ticker"])
    print(f"[BenchmarkingAgent] Using peers: {peers}")

    if not peers:
        print("[BenchmarkingAgent] No peers found — skipping benchmarking")
        return {**state, "peer_benchmarks": {}}

    peer_metrics = get_peer_metrics(peers)

    # Compute sector median gross margin
    margins = [
        m["gross_margin_pct"]
        for m in peer_metrics.values()
        if m.get("gross_margin_pct") is not None
    ]
    sector_median = (
        round(sorted(margins)[len(margins) // 2], 2)
        if margins else None
    )

    company_margin = (state["extracted_kpis"] or {}).get("gross_margin_pct")

    # Rank company among peers
    all_margins = {
        state["ticker"]: company_margin,
        **{t: m["gross_margin_pct"] for t, m in peer_metrics.items()}
    }
    sorted_tickers = sorted(
        all_margins.items(),
        key=lambda x: x[1] if x[1] is not None else -999,
        reverse=True
    )
    rank = next(
        (i + 1 for i, (t, _) in enumerate(sorted_tickers)
         if t == state["ticker"]),
        None
    )

    benchmarks = {
        "peers":                      peers,
        "peer_metrics":               peer_metrics,
        "sector_median_gross_margin": sector_median,
        "company_gross_margin":       company_margin,
        "sector_rank":                rank,
        "total_peers":                len(peers) + 1,
        "margin_delta_vs_median":     round(
            (company_margin or 0) - (sector_median or 0), 2
        ),
        "discovery_method":           "dynamic",
    }

    print(f"[BenchmarkingAgent] Rank {rank}/{len(peers)+1}, "
          f"margin delta: {benchmarks['margin_delta_vs_median']}%")

    return {**state, "peer_benchmarks": benchmarks}