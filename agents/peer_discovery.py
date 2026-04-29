import os
import math
from datetime import datetime, timezone
from databricks import sql
import yfinance as yf

CACHE_TTL_DAYS = 90
TOP_N_PEERS   = 5

WEIGHTS = {
    "log_market_cap":    0.15,
    "ebitda_margin":     0.15,
    "roic":              0.10,
    "revenue_growth_1y": 0.10,
    "net_debt_to_ebitda":0.10,
    "asset_turnover":    0.10,
    "ev_to_revenue":     0.08,
    "pe_ratio":          0.07,
    "price_to_book":     0.05,
    "sub_industry_match":0.10,
}


# ── Yfinance calls ─────────────────────────────────────────────

def fetch_yf_peers(ticker: str) -> list[str]:
    """
    yfinance has no direct peers endpoint.
    Use a hardcoded sector map as seed — then score and rank dynamically.
    This is still dynamic peer SCORING even if the candidate pool is seeded.
    """
    sector_candidates = {
        # Retail / Consumer Defensive
        "WMT": ["TGT", "COST", "KR", "AMZN", "BJ",  "DG",  "DLTR"],
        "TGT": ["WMT", "COST", "KR", "AMZN", "BJ",  "DG",  "DLTR"],
        "COST": ["WMT", "TGT", "KR", "AMZN", "BJ",  "DG",  "DLTR"],
        "AMZN": ["WMT", "TGT", "COST","EBAY","JD",  "BABA","SHOP"],
        "KR":  ["WMT", "TGT", "COST","ACI", "SFM", "CASY","GO"],
        # Add more as needed
    }
    # For unknown tickers, fetch sector from yfinance and map
    if ticker not in sector_candidates:
        info   = yf.Ticker(ticker).info
        sector = info.get("sector", "")
        print(f"[PeerDiscovery] Unknown ticker {ticker} — "
              f"sector: {sector}. Using empty candidate pool.")
        return []

    return sector_candidates[ticker]

def extract_features(ticker: str) -> dict | None:
    """
    Fetch all features for one ticker using yfinance.
    No API key needed, no rate limits beyond yfinance throttling.
    """
    try:
        info = yf.Ticker(ticker).info

        if not info or info.get("regularMarketPrice") is None:
            print(f"[PeerDiscovery] No data returned for {ticker}")
            return None

        market_cap = info.get("marketCap") or 0
        ebitda     = info.get("ebitda") or 1
        total_debt = info.get("totalDebt") or 0
        cash       = info.get("totalCash") or 0
        net_debt   = total_debt - cash
        revenue    = info.get("totalRevenue") or 1
        assets     = info.get("totalAssets") or 1

        return {
            "ticker":            ticker,
            "name":              info.get("longName", ticker),
            "sub_industry":      info.get("industry", ""),
            "sector":            info.get("sector", ""),
            "log_market_cap":    math.log(market_cap) if market_cap > 0 else None,
            "ebitda_margin":     info.get("ebitdaMargins"),
            "roic":              info.get("returnOnAssets"),
            "revenue_growth_1y": info.get("revenueGrowth"),
            "net_debt_to_ebitda":round(net_debt / ebitda, 4) if ebitda else None,
            "asset_turnover":    info.get("assetTurnover") or
                                 round(revenue / assets, 4),
            "ev_to_revenue":     info.get("enterpriseToRevenue"),
            "pe_ratio":          info.get("trailingPE"),
            "price_to_book":     info.get("priceToBook"),
        }

    except Exception as e:
        print(f"[PeerDiscovery] Feature fetch failed for {ticker}: {e}")
        return None


# ── Normalisation + scoring ───────────────────────────────────

def normalise_features(
    target: dict,
    candidates: list[dict]
) -> tuple[dict, list[dict]]:
    """
    Min-max normalise each feature across target + all candidates.
    Returns normalised target and normalised candidates.
    All values scaled to 0-1.
    """
    all_records = [target] + candidates
    feature_keys = [k for k in WEIGHTS if k != "sub_industry_match"]

    mins = {}
    maxs = {}
    for key in feature_keys:
        values = [
            r[key] for r in all_records
            if r.get(key) is not None
        ]
        if values:
            mins[key] = min(values)
            maxs[key] = max(values)

    def normalise(record: dict) -> dict:
        norm = record.copy()
        for key in feature_keys:
            val = record.get(key)
            lo  = mins.get(key)
            hi  = maxs.get(key)
            if val is None or lo is None or hi is None:
                norm[key] = 0.5  # neutral imputation for missing
            elif hi == lo:
                norm[key] = 1.0
            else:
                norm[key] = (val - lo) / (hi - lo)
        return norm

    norm_target     = normalise(target)
    norm_candidates = [normalise(c) for c in candidates]
    return norm_target, norm_candidates


def sub_industry_score(
    target_industry: str,
    target_sector: str,
    candidate: dict
) -> float:
    """
    Binary + partial match on industry.
    1.0 = same sub-industry
    0.5 = same sector different sub-industry
    0.0 = different sector
    """
    if candidate.get("sub_industry") == target_industry:
        return 1.0
    if candidate.get("sector") == target_sector:
        return 0.5
    return 0.0


def compute_similarity(
    norm_target: dict,
    norm_candidate: dict,
    raw_candidate: dict,
    target_industry: str,
    target_sector: str,
) -> float:
    """
    Weighted similarity score between target and one candidate.
    Uses 1 - abs(diff) so closer = higher score.
    """
    score = 0.0
    feature_keys = [k for k in WEIGHTS if k != "sub_industry_match"]

    for key in feature_keys:
        t_val = norm_target.get(key, 0.5)
        c_val = norm_candidate.get(key, 0.5)
        similarity = 1.0 - abs(t_val - c_val)
        score += similarity * WEIGHTS[key]

    # Add sub-industry bonus
    si_score = sub_industry_score(
        target_industry,
        target_sector,
        raw_candidate
    )
    score += si_score * WEIGHTS["sub_industry_match"]

    return round(score, 4)


# ── Delta Lake cache ──────────────────────────────────────────

def get_db_connection():
    return sql.connect(
        server_hostname=os.environ["DATABRICKS_HOST"].replace("https://", ""),
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"]
    )


def read_cache(ticker: str) -> list[str] | None:
    """
    Read peer list from Delta Lake cache.
    Returns list of selected peer tickers if cache is valid.
    Returns None if cache is missing or expired.
    """
    try:
        conn   = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT candidate_ticker, fetched_ts, is_manually_valid
            FROM main.financial.peer_discovery
            WHERE ticker = ?
              AND is_selected_peer = TRUE
            ORDER BY similarity_score DESC
        """, (ticker,))

        rows = cursor.fetchall()
        conn.close()

        if not rows:
            print(f"[PeerDiscovery] No cache found for {ticker}")
            return None

        # Check TTL on first row
        fetched_ts       = rows[0][1]
        is_manually_valid = rows[0][2]

        if is_manually_valid:
            print(f"[PeerDiscovery] Cache manually marked valid for {ticker}")
            return [r[0] for r in rows]

        if isinstance(fetched_ts, str):
            fetched_ts = datetime.fromisoformat(fetched_ts)

        age_days = (datetime.now(timezone.utc) - fetched_ts).days
        if age_days > CACHE_TTL_DAYS:
            print(f"[PeerDiscovery] Cache expired for {ticker} "
                  f"({age_days} days old > {CACHE_TTL_DAYS} day TTL)")
            return None

        peers = [r[0] for r in rows]
        print(f"[PeerDiscovery] Cache hit for {ticker} "
              f"({age_days} days old) → peers: {peers}")
        return peers

    except Exception as e:
        print(f"[PeerDiscovery] Cache read failed: {e}")
        return None


def write_cache(
    ticker: str,
    target_features: dict,
    scored_candidates: list[dict],
    selected_peers: list[str],
    cache_version: int = 1,
):
    """Write all candidates with scores to Delta Lake."""
    try:
        conn   = get_db_connection()
        cursor = conn.cursor()
        now    = datetime.utcnow().isoformat()

        # Delete old entries for this ticker
        cursor.execute(
            "DELETE FROM main.financial.peer_discovery WHERE ticker = ?",
            (ticker,)
        )

        for c in scored_candidates:
            is_selected = c["ticker"] in selected_peers
            cursor.execute("""
                INSERT INTO main.financial.peer_discovery VALUES
                (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                ticker,
                target_features.get("sub_industry", ""),
                c["ticker"],
                c.get("name", ""),
                c.get("log_market_cap"),
                c.get("ebitda_margin"),
                c.get("roic"),
                c.get("revenue_growth_1y"),
                c.get("net_debt_to_ebitda"),
                c.get("asset_turnover"),
                c.get("ev_to_revenue"),
                c.get("pe_ratio"),
                c.get("price_to_book"),
                c.get("si_score", 0.0),
                c.get("similarity_score", 0.0),
                is_selected,
                cache_version,
                "ttl_expired",
                now,
                False,
                now,
            ))

        conn.close()
        print(f"[PeerDiscovery] Cache written for {ticker} "
              f"— {len(scored_candidates)} candidates, "
              f"{len(selected_peers)} selected")

    except Exception as e:
        print(f"[PeerDiscovery] Cache write failed: {e}")


# ── Main entry point ──────────────────────────────────────────

def discover_peers(ticker: str) -> list[str]:
    """
    Main entry point for BenchmarkingAgent.
    Returns top 5 peer tickers using cache or live discovery.
    """
    # Step 1 — check cache first
    cached = read_cache(ticker)
    if cached:
        return cached

    print(f"[PeerDiscovery] Running live discovery for {ticker}")

    # Step 2 — fetch target features
    target = extract_features(ticker)
    if not target:
        print(f"[PeerDiscovery] Could not fetch features for {ticker} "
              f"— falling back to yfinance peers list")
        return fetch_yf_peers(ticker)[:TOP_N_PEERS]

    target_industry = target.get("sub_industry", "")
    target_sector   = target.get("sector", "")

    # Step 3 — get candidate pool from yfinance
    candidate_pool = fetch_yf_peers(ticker)
    print(f"[PeerDiscovery] yfinance returned {len(candidate_pool)} candidates: "
        f"{candidate_pool}")

    if not candidate_pool:
        print(f"[PeerDiscovery] No candidates found for {ticker}")
        return []

    # Step 4 — fetch features for all candidates
    candidates = []
    for peer_ticker in candidate_pool:
        if peer_ticker == ticker:
            continue
        features = extract_features(peer_ticker)
        if features:
            candidates.append(features)
        else:
            print(f"[PeerDiscovery] Skipping {peer_ticker} — no features")

    if not candidates:
        print(f"[PeerDiscovery] No valid candidates after feature fetch")
        return candidate_pool[:TOP_N_PEERS]

    # Step 5 — normalise features across target + candidates
    norm_target, norm_candidates = normalise_features(target, candidates)

    # Step 6 — score each candidate
    for i, candidate in enumerate(candidates):
        score = compute_similarity(
            norm_target,
            norm_candidates[i],
            candidate,
            target_industry,
            target_sector,
        )
        candidate["similarity_score"] = score
        candidate["si_score"] = sub_industry_score(
            target_industry, target_sector, candidate
        )

    # Step 7 — rank and select top N
    ranked = sorted(
        candidates,
        key=lambda x: x["similarity_score"],
        reverse=True
    )

    top_peers = [c["ticker"] for c in ranked[:TOP_N_PEERS]]
    print(f"[PeerDiscovery] Top {TOP_N_PEERS} peers for {ticker}: {top_peers}")
    print(f"[PeerDiscovery] Scores: "
          + ", ".join(f"{c['ticker']}={c['similarity_score']}"
                      for c in ranked[:TOP_N_PEERS]))

    # Step 8 — write to cache
    write_cache(ticker, target, ranked, top_peers)

    return top_peers