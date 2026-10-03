from mcp.server.fastmcp import FastMCP
import httpx
import json
import re
import asyncio
import html
import sys
from datetime import datetime
from urllib.parse import quote_plus

mcp = FastMCP("ieee_xplore", dependencies=["httpx"])


def log(message: str) -> None:
    # stdout carries the MCP stdio protocol; diagnostics must go to stderr
    print(message, file=sys.stderr, flush=True)


IEEE_METADATA_PATTERN = re.compile(r"xplGlobal\.document\.metadata\s*=\s*(\{.*?\});\s*\n", re.DOTALL)


def clean_ieee_text(text: str | None) -> str:
    """Strip IEEE search highlight markers ([::term::]), HTML tags and entities."""
    if not text:
        return ""
    text = re.sub(r"<[^>]+>", "", text.replace("[::", "").replace("::]", ""))
    return html.unescape(re.sub(r"\s+", " ", text)).strip()


def parse_ieee_document_metadata(page_html: str) -> dict:
    """Read the JSON metadata IEEE embeds in every document page (abstract, doi, keywords, ...)."""
    match = IEEE_METADATA_PATTERN.search(page_html)
    if not match:
        return {}
    try:
        return json.loads(match.group(1))
    except ValueError:
        return {}


@mcp.tool()
async def search_ieee(
    query: str, limit: int = 10, start_year: int = None, end_year: int = None
) -> str:
    """
    Search for papers on IEEE Xplore and retrieve details including abstracts.

    Args:
        query: The search term (e.g., "hr cv screening")
        limit: Maximum number of results to process (default: 10)
        start_year: Optional start year filter (e.g., 2020)
        end_year: Optional end year filter (e.g., 2024)
    """
    url = "https://ieeexplore.ieee.org/rest/search"

    payload = {
        "newsearch": True,
        "queryText": query,
        "highlight": True,
        "returnFacets": ["ALL"],
        "returnType": "SEARCH",
        "matchPubs": True,
    }

    # Add year range filter if provided; an open end defaults to IEEE's earliest year / next year
    if start_year or end_year:
        start = start_year or 1800
        end = end_year or datetime.now().year + 1
        payload["ranges"] = [f"{start}_{end}_Year"]

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json, text/plain, */*",
        "Origin": "https://ieeexplore.ieee.org",
        "Referer": f"https://ieeexplore.ieee.org/search/searchresult.jsp?newsearch=true&queryText={quote_plus(query)}",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    }

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            log(f"Fetching data from IEEE REST API for query: {query}...")
            response = await client.post(url, json=payload, headers=headers)
            response.raise_for_status()

            data = response.json()
            records = data.get("records", [])

            if not records:
                return json.dumps({"error": "No records found."}, indent=2)

            total_records = data.get("totalRecords")
            log(
                f"Found {total_records} records. Processing top {min(len(records), limit)}..."
            )

            # Semaphore to control concurrency (politeness + speed)
            sem = asyncio.Semaphore(5)

            async def process_record(index, record):
                async with sem:
                    try:
                        title = record.get("articleTitle", "")
                        article_number = record.get("articleNumber", "")

                        # Basic info
                        item = {
                            "index": index + 1,
                            "source": "ieee",
                            "title": title,
                            "authors": [
                                a.get("preferredName", "")
                                for a in record.get("authors", [])
                            ],
                            "publication": record.get("publicationTitle", ""),
                            "year": int(record["publicationYear"]) if str(record.get("publicationYear", "")).isdigit() else None,
                            "doi": record.get("doi") or None,
                            "url": f"https://ieeexplore.ieee.org/document/{article_number}"
                            if article_number
                            else "N/A",
                            "pdf_url": f"https://ieeexplore.ieee.org{record.get('pdfLink', '')}"
                            if record.get("pdfLink")
                            else "N/A",
                            "abstract": clean_ieee_text(record.get("abstract")),  # search snippet; replaced by the full abstract below
                        }

                        # Fetch full abstract if possible/needed
                        if article_number:
                            # Small random delay to avoid exact burst patterns
                            await asyncio.sleep(0.1)

                            doc_response = await client.get(
                                item["url"], headers=headers
                            )
                            if doc_response.status_code == 200:
                                doc_text = doc_response.text
                                metadata = parse_ieee_document_metadata(doc_text)
                                if metadata.get("abstract"):
                                    item["abstract"] = clean_ieee_text(metadata["abstract"])

                        log(f"Processed: {title[:50]}...")
                        return item
                    except Exception as e:
                        log(f"Error processing record {index}: {e}")
                        return None

            # Create tasks for all records up to limit
            tasks = [process_record(i, rec) for i, rec in enumerate(records[:limit])]
            results = await asyncio.gather(*tasks)

            # Filter out None results using list comprehension
            clean_results = [r for r in results if r is not None]

            return json.dumps(clean_results, indent=2)

        except Exception as e:
            return json.dumps({"error": f"Error occurred: {str(e)}"}, indent=2)


if __name__ == "__main__":
    mcp.run()
