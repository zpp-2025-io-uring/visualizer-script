from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import struct
from typing import Iterable

from fpdf import FPDF
from pypdf import PdfReader, PdfWriter


@dataclass(frozen=True)
class PdfRenderOptions:
    title_font_family: str = "Helvetica"
    title_font_size: int = 24
    title_margin_top_mm: float = 40.0
    page_margin_mm: float = 10.0


def _sorted_existing(paths: Iterable[Path]) -> list[Path]:
    existing = [Path(p) for p in paths if Path(p).exists()]
    return sorted(existing, key=lambda p: p.name)


def _read_png_size(path: Path) -> tuple[int, int]:
    """Return (width, height) in pixels for a PNG file."""

    with open(path, "rb") as f:
        sig = f.read(8)
        if sig != b"\x89PNG\r\n\x1a\n":
            raise ValueError(f"Not a PNG file: {path}")
        ihdr_len = f.read(4)
        ihdr_type = f.read(4)
        if ihdr_type != b"IHDR":
            raise ValueError(f"Invalid PNG header (missing IHDR): {path}")
        ihdr_data = f.read(13)
        width, height = struct.unpack(">II", ihdr_data[0:8])
        return int(width), int(height)


def _add_image_page(pdf: FPDF, image_path: Path, options: PdfRenderOptions) -> None:
    """Add a single page to `pdf` containing `image_path`, centered and scaled.

    If the image can't be read or has non-positive dimensions, the function
    will leave the PDF unchanged.
    """
    pdf.add_page()

    margin = float(options.page_margin_mm)
    available_w = pdf.w - 2 * margin
    available_h = pdf.h - 2 * margin

    px_w = px_h = 0
    if image_path.suffix.lower() == ".png":
        px_w, px_h = _read_png_size(image_path)

    if px_w <= 0 or px_h <= 0:
        return

    aspect = px_h / px_w
    target_w = available_w
    target_h = target_w * aspect

    if target_h > available_h:
        target_h = available_h
        target_w = target_h / aspect

    x = (pdf.w - target_w) / 2
    y = (pdf.h - target_h) / 2
    pdf.image(str(image_path), x=x, y=y, w=target_w, h=target_h)


def generate_benchmark_summary_pdf(
    *,
    benchmark_name: str,
    images: Iterable[Path],
    output_pdf: Path,
    options: PdfRenderOptions | None = None,
) -> Path:
    """Create a PDF with a title page + one image per subsequent page."""

    options = options or PdfRenderOptions()
    output_pdf = Path(output_pdf)
    output_pdf.parent.mkdir(parents=True, exist_ok=True)

    image_paths = _sorted_existing(images)

    pdf = FPDF(orientation="P", unit="mm", format="A4")
    pdf.set_auto_page_break(auto=False)

    # Title page
    pdf.add_page()
    pdf.set_font(options.title_font_family, style="B", size=options.title_font_size)
    pdf.set_y(options.title_margin_top_mm)
    pdf.multi_cell(0, 12, benchmark_name, align="C")

    # Image pages
    for image_path in image_paths:
        _add_image_page(pdf, image_path, options)

    pdf.output(str(output_pdf))
    return output_pdf


def merge_pdfs(*, input_pdfs: Iterable[Path], output_pdf: Path) -> Path:
    """Concatenate PDFs in-order into output_pdf."""

    output_pdf = Path(output_pdf)
    output_pdf.parent.mkdir(parents=True, exist_ok=True)

    writer = PdfWriter()
    for pdf_path in _sorted_existing(input_pdfs):
        reader = PdfReader(str(pdf_path))
        for page in reader.pages:
            writer.add_page(page)

    with open(output_pdf, "wb") as f:
        writer.write(f)

    return output_pdf
