import time
from dotenv import load_dotenv
load_dotenv('.env.local')

from pipeline.runner import run_pipeline

# 5 companies, 4 quarters each = 20 runs
# Gives enough data for monitoring to be meaningful
TICKERS = ["WMT", "TGT", "COST", "AMZN", "KR"]
QUARTERS = ["2024-Q1", "2024-Q2", "2024-Q3", "2023-Q4"]

results = []

for ticker in TICKERS:
    for quarter in QUARTERS:
        print(f"\n>>> Running {ticker} {quarter}")
        try:
            state = run_pipeline(ticker, quarter)
            results.append({
                "ticker": ticker,
                "quarter": quarter,
                "success": not bool(state.get("error")),
                "confidence": state.get("confidence_score"),
                "hitl": state.get("hitl_required")
            })
        except Exception as e:
            print(f"!!! Failed {ticker} {quarter}: {e}")
            results.append({
                "ticker": ticker,
                "quarter": quarter,
                "success": False,
                "confidence": 0,
                "hitl": False
            })
        # Be polite to SEC EDGAR — don't hammer their API
        time.sleep(3)

# Print summary
print("\n" + "="*60)
print("BATCH RUN SUMMARY")
print("="*60)
for r in results:
    status = "OK" if r["success"] else "FAIL"
    hitl   = "HITL" if r["hitl"] else "AUTO"
    print(f"{r['ticker']} {r['quarter']} | {status} | "
          f"conf={r['confidence']:.3f} | {hitl}")

success_rate = sum(r["success"] for r in results) / len(results) * 100
avg_conf     = sum(r["confidence"] for r in results) / len(results)
print(f"\nSuccess rate: {success_rate:.0f}%")
print(f"Avg confidence: {avg_conf:.3f}")