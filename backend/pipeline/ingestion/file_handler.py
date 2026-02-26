"""Unified file format handler — parses any supported format to a pandas DataFrame."""

import json
import logging
from io import BytesIO
from typing import Optional

import chardet
import pandas as pd

logger = logging.getLogger(__name__)


class FileHandler:
    """Parse files of any supported format into a pandas DataFrame.

    Supported: csv, tsv, json, jsonl, parquet, xlsx, txt, md, pdf, html, docx.
    Never crashes on partial data — returns what was parseable.
    """

    PARSERS = {}

    @classmethod
    def register(cls, fmt: str):
        """Decorator to register a parser function for a format."""
        def wrapper(func):
            cls.PARSERS[fmt] = func
            return func
        return wrapper

    @classmethod
    def parse(cls, file_path: str, fmt: str, **kwargs) -> pd.DataFrame:
        """Parse a file into a DataFrame.

        Args:
            file_path: Path to the file on disk (or BytesIO-compatible).
            fmt: One of the supported format strings.
            **kwargs: Extra args passed to the specific parser.

        Returns:
            pd.DataFrame with parsed data. May be empty if parsing failed.
        """
        parser = cls.PARSERS.get(fmt)
        if parser is None:
            logger.error("No parser registered for format: %s", fmt)
            return pd.DataFrame()

        try:
            df = parser(file_path, **kwargs)
            logger.info("Parsed %s: %d rows, %d columns", fmt, len(df), len(df.columns))
            return df
        except Exception as exc:
            logger.error("Failed to parse %s file %s: %s", fmt, file_path, exc)
            return pd.DataFrame()

    @classmethod
    def preview(cls, file_path: str, fmt: str, n_rows: int = 50) -> pd.DataFrame:
        """Parse only the first n_rows for preview."""
        df = cls.parse(file_path, fmt, nrows=n_rows)
        return df.head(n_rows)


def _detect_encoding(file_path: str) -> str:
    """Detect file encoding using chardet."""
    with open(file_path, "rb") as f:
        raw = f.read(min(100_000, 1_000_000))  # Read up to 100KB for detection
    result = chardet.detect(raw)
    encoding = result.get("encoding", "utf-8") or "utf-8"
    logger.info("Detected encoding: %s (confidence: %s)", encoding, result.get("confidence"))
    return encoding


# ── CSV / TSV ────────────────────────────────────────────

@FileHandler.register("csv")
def parse_csv(file_path: str, nrows: Optional[int] = None, **kwargs) -> pd.DataFrame:
    encoding = _detect_encoding(file_path)
    try:
        return pd.read_csv(file_path, encoding=encoding, nrows=nrows, on_bad_lines="warn")
    except Exception:
        # Fallback with latin-1
        return pd.read_csv(file_path, encoding="latin-1", nrows=nrows, on_bad_lines="skip")


@FileHandler.register("tsv")
def parse_tsv(file_path: str, nrows: Optional[int] = None, **kwargs) -> pd.DataFrame:
    encoding = _detect_encoding(file_path)
    try:
        return pd.read_csv(file_path, sep="\t", encoding=encoding, nrows=nrows, on_bad_lines="warn")
    except Exception:
        return pd.read_csv(file_path, sep="\t", encoding="latin-1", nrows=nrows, on_bad_lines="skip")


# ── JSON ─────────────────────────────────────────────────

@FileHandler.register("json")
def parse_json(file_path: str, nrows: Optional[int] = None, **kwargs) -> pd.DataFrame:
    encoding = _detect_encoding(file_path)
    with open(file_path, "r", encoding=encoding, errors="replace") as f:
        content = f.read().strip()

    # Try array of objects first
    try:
        data = json.loads(content)
        if isinstance(data, list):
            df = pd.DataFrame(data)
        elif isinstance(data, dict):
            # Might be a nested structure — try to normalize
            df = pd.json_normalize(data)
        else:
            df = pd.DataFrame([{"value": data}])
        if nrows is not None:
            df = df.head(nrows)
        return df
    except json.JSONDecodeError:
        pass

    # Fall back to JSONL parsing
    return parse_jsonl(file_path, nrows=nrows)


# ── JSONL ────────────────────────────────────────────────

@FileHandler.register("jsonl")
def parse_jsonl(file_path: str, nrows: Optional[int] = None, **kwargs) -> pd.DataFrame:
    encoding = _detect_encoding(file_path)
    records: list[dict] = []
    skipped = 0
    with open(file_path, "r", encoding=encoding, errors="replace") as f:
        for i, line in enumerate(f):
            if nrows is not None and len(records) >= nrows:
                break
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                skipped += 1
                if skipped <= 10:
                    logger.warning("Skipped malformed JSONL line %d", i + 1)

    if skipped > 0:
        logger.warning("Total skipped JSONL lines: %d", skipped)

    return pd.DataFrame(records) if records else pd.DataFrame()


# ── Parquet ──────────────────────────────────────────────

@FileHandler.register("parquet")
def parse_parquet(file_path: str, nrows: Optional[int] = None, **kwargs) -> pd.DataFrame:
    df = pd.read_parquet(file_path)
    if nrows is not None:
        df = df.head(nrows)
    return df


# ── Excel (XLSX) ─────────────────────────────────────────

@FileHandler.register("xlsx")
def parse_xlsx(file_path: str, nrows: Optional[int] = None, sheet: Optional[str] = None, **kwargs) -> pd.DataFrame:
    try:
        return pd.read_excel(file_path, sheet_name=sheet or 0, nrows=nrows, engine="openpyxl")
    except Exception as exc:
        logger.warning("Excel parse error: %s", exc)
        return pd.DataFrame()


# ── TXT / Markdown ───────────────────────────────────────

@FileHandler.register("txt")
def parse_txt(file_path: str, nrows: Optional[int] = None, **kwargs) -> pd.DataFrame:
    encoding = _detect_encoding(file_path)
    with open(file_path, "r", encoding=encoding, errors="replace") as f:
        lines = []
        for i, line in enumerate(f):
            if nrows is not None and i >= nrows:
                break
            lines.append({"line_number": i + 1, "text": line.rstrip("\n\r")})
    return pd.DataFrame(lines)


@FileHandler.register("md")
def parse_md(file_path: str, nrows: Optional[int] = None, **kwargs) -> pd.DataFrame:
    return parse_txt(file_path, nrows=nrows)


# ── PDF ──────────────────────────────────────────────────

@FileHandler.register("pdf")
def parse_pdf(file_path: str, nrows: Optional[int] = None, **kwargs) -> pd.DataFrame:
    pages: list[dict] = []

    # Try pdfplumber first
    try:
        import pdfplumber
        with pdfplumber.open(file_path) as pdf:
            for i, page in enumerate(pdf.pages):
                if nrows is not None and i >= nrows:
                    break
                text = page.extract_text() or ""
                pages.append({"page_number": i + 1, "text": text})
    except Exception as exc:
        logger.warning("pdfplumber failed, trying pytesseract OCR: %s", exc)

        # Fallback: pytesseract for scanned PDFs
        try:
            import pytesseract
            from pdf2image import convert_from_path
            images = convert_from_path(file_path)
            for i, img in enumerate(images):
                if nrows is not None and i >= nrows:
                    break
                text = pytesseract.image_to_string(img)
                pages.append({"page_number": i + 1, "text": text})
        except Exception as ocr_exc:
            logger.error("OCR fallback also failed: %s", ocr_exc)
            pages.append({"page_number": 1, "text": f"[Parse error: {ocr_exc}]"})

    return pd.DataFrame(pages)


# ── HTML ─────────────────────────────────────────────────

@FileHandler.register("html")
def parse_html(file_path: str, nrows: Optional[int] = None, **kwargs) -> pd.DataFrame:
    from bs4 import BeautifulSoup

    encoding = _detect_encoding(file_path)
    with open(file_path, "r", encoding=encoding, errors="replace") as f:
        content = f.read()

    soup = BeautifulSoup(content, "lxml")

    # Remove script and style elements
    for tag in soup(["script", "style", "nav", "footer", "header"]):
        tag.decompose()

    # Try to find tables first
    tables = soup.find_all("table")
    if tables:
        try:
            dfs = pd.read_html(content)
            if dfs:
                df = dfs[0]
                if nrows is not None:
                    df = df.head(nrows)
                return df
        except Exception:
            pass

    # Fall back to extracting text paragraphs
    paragraphs = soup.find_all(["p", "h1", "h2", "h3", "h4", "h5", "h6", "li"])
    rows: list[dict] = []
    for i, p in enumerate(paragraphs):
        if nrows is not None and i >= nrows:
            break
        text = p.get_text(strip=True)
        if text:
            rows.append({"element": p.name, "text": text})

    if not rows:
        # Last resort — full text
        full_text = soup.get_text(separator="\n", strip=True)
        lines = [l for l in full_text.split("\n") if l.strip()]
        for i, line in enumerate(lines[:nrows] if nrows else lines):
            rows.append({"line_number": i + 1, "text": line})

    return pd.DataFrame(rows)


# ── DOCX ─────────────────────────────────────────────────

@FileHandler.register("docx")
def parse_docx(file_path: str, nrows: Optional[int] = None, **kwargs) -> pd.DataFrame:
    try:
        from docx import Document
    except ImportError:
        logger.error("python-docx not installed")
        return pd.DataFrame()

    doc = Document(file_path)
    rows: list[dict] = []
    for i, para in enumerate(doc.paragraphs):
        if nrows is not None and i >= nrows:
            break
        text = para.text.strip()
        if text:
            rows.append({"paragraph_number": i + 1, "style": para.style.name if para.style else "", "text": text})

    return pd.DataFrame(rows) if rows else pd.DataFrame(columns=["paragraph_number", "style", "text"])
