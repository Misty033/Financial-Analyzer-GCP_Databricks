import json
import os
import vertexai
import yfinance as yf
from vertexai.generative_models import FunctionDeclaration, GenerativeModel, Tool, ToolConfig
from agents.state import PipelineState

vertexai.init(project=os.environ["GCP_PROJECT_ID"], location="us-central1")
model = GenerativeModel("gemini-2.5-flash-lite")

DISCREPANCY_TOOL_NAME = "return_discrepancy_reasoning"

DISCREPANCY_TOOL = Tool(
    function_declarations=[
        FunctionDeclaration(
            name=DISCREPANCY_TOOL_NAME,
            description="Return reasoning about whether an EPS discrepancy is explainable.",
            parameters={
                "type": "object",
                "properties": {
                    "is_explainable": {
                        "type": "boolean",
                        "description": "Whether the discrepancy is explainable.",
                    },
                    "reasoning": {
                        "type": "string",
                        "description": "One sentence explaining the discrepancy judgment.",
                    },
                    "confidence_adjustment": {
                        "type": "number",
                        "description": "Confidence multiplier from 0.0 to 1.0.",
                    },
                },
                "required": [
                    "is_explainable",
                    "reasoning",
                    "confidence_adjustment",
                ],
            },
        )
    ]
)


def discrepancy_tool_config() -> ToolConfig:
    return ToolConfig(
        function_calling_config=ToolConfig.FunctionCallingConfig(
            mode=ToolConfig.FunctionCallingConfig.Mode.ANY,
            allowed_function_names=[DISCREPANCY_TOOL_NAME],
        )
    )


def get_function_args(response, function_name: str) -> dict:
    for candidate in response.candidates:
        for function_call in candidate.function_calls:
            if function_call.name == function_name:
                return dict(function_call.args)
    return {}


def get_reported_eps(ticker: str) -> float | None:
    try:
        earnings = yf.Ticker(ticker).quarterly_earnings
        if earnings is None or earnings.empty:
            return None
        return float(earnings["EPS"].iloc[0])
    except Exception as e:
        print(f"[ValidationAgent] Could not fetch EPS: {e}")
        return None


def llm_reason_about_discrepancy(
    ticker: str,
    extracted_eps: float,
    reported_eps: float,
    delta_pct: float,
    kpis: dict
) -> dict:
    """
    Ask LLM to reason about whether an EPS discrepancy is explainable.
    This is the agentic step — LLM judges context, not just threshold.
    """
    prompt = f"""
You are a senior financial analyst reviewing a potential data discrepancy for {ticker}.

Extracted EPS from SEC filing: {extracted_eps}
Reported EPS from market data: {reported_eps}
Discrepancy: {delta_pct:.1f}%

Full extracted KPIs for context:
{json.dumps(kpis, indent=2)}

Reason about whether this discrepancy is explainable. Consider:
1. Could this be basic vs diluted EPS difference?
2. Could there be a one-time item (impairment, restructuring) affecting net income?
3. Could this be a share count difference (stock split, buyback)?
4. Could the filing use a different reporting period than expected?
5. Is the discrepancy small enough to be a rounding difference?

Call the {DISCREPANCY_TOOL_NAME} tool with your judgment.

confidence_adjustment meaning:
- 1.0 = discrepancy is fully explained, no confidence penalty
- 0.7 = partially explained, small penalty
- 0.4 = unexplained, significant penalty
- 0.1 = clearly wrong extraction
"""
    try:
        response = model.generate_content(
            prompt,
            tools=[DISCREPANCY_TOOL],
            tool_config=discrepancy_tool_config(),
        )
        reasoning = get_function_args(response, DISCREPANCY_TOOL_NAME)
        return reasoning or {
            "is_explainable": False,
            "reasoning": "Could not reason about discrepancy",
            "confidence_adjustment": 0.5
        }
    except Exception as e:
        print(f"[ValidationAgent] LLM reasoning failed: {e}")
        # Fall back to conservative penalty if reasoning fails
        return {
            "is_explainable": False,
            "reasoning": "Could not reason about discrepancy",
            "confidence_adjustment": 0.5
        }


def validation_agent(state: PipelineState) -> PipelineState:
    """
    Agent 4: Validate KPIs with LLM reasoning about discrepancies.
    Goes beyond threshold rules — uses LLM judgment for context.
    """
    print(f"[ValidationAgent] Starting for {state['ticker']} {state['quarter']}")

    if state.get("error"):
        return state

    kpis    = state["extracted_kpis"]
    flagged = []
    confidence_factors = []
    reasoning_log = []

    # ── Check 1: Completeness ─────────────────────────────────
    required_fields = [
        "revenue_usd_millions", "net_income_usd_millions",
        "eps_diluted", "gross_margin_pct", "operating_cash_flow"
    ]
    missing = [f for f in required_fields if kpis.get(f) is None]
    if missing:
        flagged.extend(missing)
        confidence_factors.append(0.5)
        reasoning_log.append(f"Missing fields: {missing}")
        print(f"[ValidationAgent] Missing: {missing}")
    else:
        confidence_factors.append(1.0)

    # ── Check 2: EPS cross-check with LLM reasoning ───────────
    reported_eps = get_reported_eps(state["ticker"])
    extracted_eps = kpis.get("eps_diluted")

    if reported_eps and extracted_eps:
        delta_pct = abs(reported_eps - extracted_eps) / abs(reported_eps) * 100
        print(f"[ValidationAgent] EPS delta: {delta_pct:.1f}%")

        if delta_pct > 5:
            # ── Agentic step: ask LLM to reason about discrepancy ──
            print("[ValidationAgent] Asking LLM to reason about discrepancy...")
            reasoning = llm_reason_about_discrepancy(
                state["ticker"],
                extracted_eps,
                reported_eps,
                delta_pct,
                kpis
            )
            print(f"[ValidationAgent] LLM reasoning: {reasoning}")
            reasoning_log.append(
                f"EPS delta {delta_pct:.1f}%: {reasoning['reasoning']}"
            )

            confidence_factors.append(reasoning["confidence_adjustment"])

            if not reasoning["is_explainable"]:
                flagged.append("eps_diluted")
        else:
            # Delta < 5% — no concern
            confidence_factors.append(1.0)
            reasoning_log.append(f"EPS delta {delta_pct:.1f}% — within tolerance")
    else:
        confidence_factors.append(0.8)
        reasoning_log.append("Could not fetch reported EPS for cross-check")

    # ── Check 3: Sanity bounds ────────────────────────────────
    gm = kpis.get("gross_margin_pct")
    if gm is not None and (gm < 0 or gm > 100):
        flagged.append("gross_margin_pct")
        confidence_factors.append(0.3)
        reasoning_log.append(f"Gross margin {gm}% outside valid range 0-100")
    else:
        confidence_factors.append(1.0)

    # ── Check 4: Internal consistency ────────────────────────
    # Net income and revenue should give a reasonable net margin
    revenue   = kpis.get("revenue_usd_millions")
    net_income = kpis.get("net_income_usd_millions")
    if revenue and net_income and revenue > 0:
        net_margin = net_income / revenue * 100
        if net_margin < -50 or net_margin > 50:
            flagged.append("net_income_usd_millions")
            confidence_factors.append(0.4)
            reasoning_log.append(
                f"Net margin {net_margin:.1f}% seems implausible"
            )
        else:
            confidence_factors.append(1.0)

    # ── Final score ───────────────────────────────────────────
    confidence_score = round(
        sum(confidence_factors) / len(confidence_factors), 3
    )
    validation_reasoning = " | ".join(reasoning_log)

    # HITL required if confidence low OR multiple fields flagged
    hitl_required = confidence_score < 0.75 or len(flagged) > 1

    # ── Should we retry extraction? ───────────────────────────
    # Only suggest retry if confidence is low AND we haven't exceeded retries
    should_retry = (
        confidence_score < 0.75
        and state.get("retry_count", 0) < 2
        and len(flagged) > 0
    )

    if should_retry:
        print(f"[ValidationAgent] Low confidence ({confidence_score}) "
              f"— flagging for extraction retry")

    print(f"[ValidationAgent] Confidence: {confidence_score} | "
          f"HITL: {hitl_required} | Retry: {should_retry} | "
          f"Flagged: {flagged}")

    return {
        **state,
        "confidence_score":     confidence_score,
        "flagged_kpis":         flagged,
        "hitl_required":        hitl_required,
        "validation_reasoning": validation_reasoning,
        # Pass retry signal via retry_count increment if needed
        "retry_count": (
            state.get("retry_count", 0) + 1
            if should_retry
            else state.get("retry_count", 0)
        ),
        "reflection_notes": (
            f"ValidationAgent flagged: {flagged}. Reasoning: {validation_reasoning}"
            if should_retry
            else state.get("reflection_notes")
        ),
    }
