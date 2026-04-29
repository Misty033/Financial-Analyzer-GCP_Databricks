import os
import requests
import pdfplumber
import io
from google.cloud import storage
from agents.state import PipelineState

HEADERS = {"User-Agent": "yourname@youremail.com"}  # SEC requires this


def get_cik(ticker: str) -> str:
    """Convert ticker symbol to SEC CIK number."""
    url = "https://www.sec.gov/files/company_tickers.json"
    resp = requests.get(url, headers=HEADERS)
    data = resp.json()
    for entry in data.values():
        if entry["ticker"].upper() == ticker.upper():
            return str(entry["cik_str"]).zfill(10)
    raise ValueError(f"CIK not found for ticker: {ticker}")


def get_filing_url(cik: str, quarter: str) -> str:
    """
    Fetch the 10-Q filing URL for a given CIK and quarter.
    quarter format: '2024-Q3'
    """
    url = f"https://data.sec.gov/submissions/CIK{cik}.json"
    resp = requests.get(url, headers=HEADERS)
    filings = resp.json()["filings"]["recent"]

    year, q = quarter.split("-")
    # Map quarter to approximate month range
    month_map = {"Q1": ["03"], "Q2": ["06"], "Q3": ["09"], "Q4": ["12"]}
    target_months = month_map[q]

    for i, form in enumerate(filings["form"]):
        if form == "10-Q":
            filed_date = filings["filingDate"][i]      # e.g. "2024-08-01"
            filed_month = filed_date[5:7]
            filed_year = filed_date[:4]
            if filed_year == year and filed_month in [
                str(int(m) - 1).zfill(2) for m in target_months
            ] + target_months:
                accession = filings["accessionNumber"][i].replace("-", "")
                doc = filings["primaryDocument"][i]
                return f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession}/{doc}"

    raise ValueError(f"No 10-Q found for {quarter}")


def download_pdf_text(filing_url: str) -> str:
    """Download the filing and extract text."""
    resp = requests.get(filing_url, headers=HEADERS)
    # SEC filings are often HTML not PDF — handle both
    if filing_url.endswith(".pdf"):
        with pdfplumber.open(io.BytesIO(resp.content)) as pdf:
            return "\n".join(p.extract_text() or "" for p in pdf.pages)
    else:
        # HTML filing — return raw text stripped of tags
        from html.parser import HTMLParser
        class TextExtractor(HTMLParser):
            def __init__(self):
                super().__init__()
                self.text = []
            def handle_data(self, data):
                self.text.append(data)
        parser = TextExtractor()
        parser.feed(resp.text)
        return " ".join(parser.text)


def upload_to_gcs(content: bytes, ticker: str, quarter: str) -> str:
    """Upload raw filing to GCS and return the gs:// path."""
    client = storage.Client()
    bucket = client.bucket(os.environ["GCS_BUCKET"])
    blob_name = f"filings/{ticker}/{quarter}/filing.txt"
    blob = bucket.blob(blob_name)
    blob.upload_from_string(content, content_type="text/plain")
    return f"gs://{os.environ['GCS_BUCKET']}/{blob_name}"


def ingestion_agent(state: PipelineState) -> PipelineState:
    """
    Agent 1: Fetch SEC filing, extract text, upload to GCS.
    """
    print(f"[IngestionAgent] Starting for {state['ticker']} {state['quarter']}")
    try:
        cik = get_cik(state["ticker"])
        filing_url = get_filing_url(cik, state["quarter"])
        print(f"[IngestionAgent] Found filing: {filing_url}")

        raw_text = download_pdf_text(filing_url)
        gcs_path = upload_to_gcs(raw_text.encode(), state["ticker"], state["quarter"])

        print(f"[IngestionAgent] Done. GCS path: {gcs_path}")
        return {**state, "gcs_pdf_path": gcs_path, "raw_text": raw_text}

    except Exception as e:
        print(f"[IngestionAgent] ERROR: {e}")
        return {**state, "error": str(e)}