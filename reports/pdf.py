from datetime import datetime
from pathlib import Path

from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import inch
from reportlab.platypus import (
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

from data.models import Dossier, RedFlagType

OUTPUT_DIR = Path(__file__).parent.parent / "output" / "dossiers"

SEVERITY_COLORS = {
    "high": colors.Color(0.9, 0.2, 0.2),
    "medium": colors.Color(0.9, 0.6, 0.1),
    "low": colors.Color(0.9, 0.8, 0.2),
}


def generate_dossier_pdf(dossier: Dossier, output_dir: Path | None = None) -> Path:
    """Generate a PDF dossier report for bounty submission."""
    if output_dir is None:
        output_dir = OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    filename = f"dossier_{dossier.provider.npi}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
    output_path = output_dir / filename

    doc = SimpleDocTemplate(str(output_path), pagesize=letter,
                            leftMargin=0.75 * inch, rightMargin=0.75 * inch,
                            topMargin=0.75 * inch, bottomMargin=0.75 * inch)

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle("CustomTitle", parent=styles["Title"], fontSize=18, spaceAfter=12)
    heading_style = ParagraphStyle("CustomHeading", parent=styles["Heading2"], fontSize=14,
                                    spaceBefore=16, spaceAfter=8,
                                    textColor=colors.Color(0.2, 0.2, 0.4))
    body_style = styles["BodyText"]
    small_style = ParagraphStyle("Small", parent=body_style, fontSize=8, textColor=colors.grey)

    elements = []

    # --- Title ---
    elements.append(Paragraph("Medicaid Fraud Investigation Dossier", title_style))
    elements.append(Paragraph(f"Generated: {datetime.now().strftime('%B %d, %Y %I:%M %p')}", small_style))
    elements.append(Spacer(1, 12))

    # --- Provider Info ---
    elements.append(Paragraph("Provider Information", heading_style))
    provider = dossier.provider
    info_data = [
        ["NPI", provider.npi],
        ["Name", provider.name or "N/A"],
        ["Specialty", provider.specialty or "N/A"],
        ["Location", f"{provider.city}, {provider.state} {provider.zip_code}".strip(", ")],
    ]
    info_table = Table(info_data, colWidths=[1.5 * inch, 5 * inch])
    info_table.setStyle(TableStyle([
        ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, -1), 10),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.lightgrey),
    ]))
    elements.append(info_table)
    elements.append(Spacer(1, 12))

    # --- Risk Score ---
    score = dossier.scan_result.overall_score
    score_label = "HIGH" if score >= 0.7 else "MEDIUM" if score >= 0.4 else "LOW"
    score_color = "high" if score >= 0.7 else "medium" if score >= 0.4 else "low"
    elements.append(Paragraph(
        f"Overall Risk Score: <b>{score:.0%}</b> ({score_label})",
        ParagraphStyle("Score", parent=body_style, fontSize=12,
                       textColor=SEVERITY_COLORS[score_color])
    ))
    elements.append(Spacer(1, 12))

    # --- Red Flags ---
    if dossier.scan_result.red_flags:
        elements.append(Paragraph("Red Flags", heading_style))
        for i, flag in enumerate(dossier.scan_result.red_flags, 1):
            severity_label = "HIGH" if flag.severity >= 0.7 else "MEDIUM" if flag.severity >= 0.4 else "LOW"
            elements.append(Paragraph(
                f"<b>{i}. [{severity_label}] {flag.flag_type.value.replace('_', ' ').title()}</b>",
                body_style
            ))
            elements.append(Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;{flag.description}", body_style))
            if flag.evidence:
                evidence_str = ", ".join(f"{k}: {v}" for k, v in flag.evidence.items())
                elements.append(Paragraph(f"&nbsp;&nbsp;&nbsp;&nbsp;<i>Evidence: {evidence_str}</i>", small_style))
            elements.append(Spacer(1, 4))
        elements.append(Spacer(1, 8))

    # --- Claims Summary ---
    if dossier.claims_summary:
        elements.append(Paragraph("Claims Summary", heading_style))
        summary = dossier.claims_summary
        summary_data = []

        if "total_claims" in summary:
            summary_data.append(["Total Claims", f"{summary['total_claims']:,}"])
        if "total_billed" in summary:
            summary_data.append(["Total Billed", f"${summary['total_billed']:,.2f}"])
        if "total_paid" in summary:
            summary_data.append(["Total Paid", f"${summary['total_paid']:,.2f}"])
        if "avg_billed_per_claim" in summary:
            summary_data.append(["Avg per Claim", f"${summary['avg_billed_per_claim']:,.2f}"])
        if "max_single_claim" in summary:
            summary_data.append(["Max Single Claim", f"${summary['max_single_claim']:,.2f}"])
        if "date_range_start" in summary:
            summary_data.append(["Date Range", f"{summary['date_range_start']} to {summary['date_range_end']}"])
        if "max_claims_in_a_day" in summary:
            summary_data.append(["Max Claims/Day", f"{summary['max_claims_in_a_day']}"])
        if "avg_claims_per_active_day" in summary:
            summary_data.append(["Avg Claims/Active Day", f"{summary['avg_claims_per_active_day']}"])

        if summary_data:
            summary_table = Table(summary_data, colWidths=[2 * inch, 4.5 * inch])
            summary_table.setStyle(TableStyle([
                ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
                ("FONTSIZE", (0, 0), (-1, -1), 10),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.lightgrey),
            ]))
            elements.append(summary_table)
            elements.append(Spacer(1, 8))

        # Top procedures
        if "top_procedures" in summary and summary["top_procedures"]:
            elements.append(Paragraph("Top Procedures by Volume", heading_style))
            proc_header = [["Procedure Code", "Count", "Total Billed"]]
            proc_rows = [
                [p.get("procedure_code", p.get("PROC_CD", "")),
                 str(p.get("count", "")),
                 f"${p.get('total_billed', 0):,.2f}"]
                for p in summary["top_procedures"]
            ]
            proc_table = Table(proc_header + proc_rows, colWidths=[2.5 * inch, 1.5 * inch, 2.5 * inch])
            proc_table.setStyle(TableStyle([
                ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
                ("BACKGROUND", (0, 0), (-1, 0), colors.Color(0.9, 0.9, 0.95)),
                ("FONTSIZE", (0, 0), (-1, -1), 9),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.lightgrey),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
            ]))
            elements.append(proc_table)
            elements.append(Spacer(1, 8))

    # --- Peer Comparison ---
    if dossier.peer_comparison and "note" not in dossier.peer_comparison:
        elements.append(Paragraph("Peer Comparison", heading_style))
        pc = dossier.peer_comparison
        peer_data = [
            ["Specialty", pc.get("specialty", "N/A")],
            ["Peers in Specialty", f"{pc.get('peer_count', 'N/A'):,}"],
            ["Provider Total Billed", f"${pc.get('provider_total_billed', 0):,.2f}"],
            ["Peer Mean Billed", f"${pc.get('peer_mean_billed', 0):,.2f}"],
            ["Peer Median Billed", f"${pc.get('peer_median_billed', 0):,.2f}"],
            ["Provider Percentile", f"{pc.get('provider_percentile', 'N/A')}th"],
        ]
        if "zscore" in pc:
            peer_data.append(["Z-Score", f"{pc['zscore']}"])

        peer_table = Table(peer_data, colWidths=[2 * inch, 4.5 * inch])
        peer_table.setStyle(TableStyle([
            ("FONTNAME", (0, 0), (0, -1), "Helvetica-Bold"),
            ("FONTSIZE", (0, 0), (-1, -1), 10),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ("TOPPADDING", (0, 0), (-1, -1), 4),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.lightgrey),
        ]))
        elements.append(peer_table)
        elements.append(Spacer(1, 8))

    # --- Monthly Timeline ---
    if dossier.timeline:
        elements.append(Paragraph("Monthly Billing Timeline", heading_style))
        timeline_header = [["Month", "Claims", "Total Billed"]]
        timeline_rows = [
            [t["month"], str(t["claim_count"]), f"${t['total_billed']:,.2f}"]
            for t in dossier.timeline
        ]
        timeline_table = Table(timeline_header + timeline_rows,
                               colWidths=[2.5 * inch, 1.5 * inch, 2.5 * inch])
        timeline_table.setStyle(TableStyle([
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("BACKGROUND", (0, 0), (-1, 0), colors.Color(0.9, 0.9, 0.95)),
            ("FONTSIZE", (0, 0), (-1, -1), 9),
            ("GRID", (0, 0), (-1, -1), 0.5, colors.lightgrey),
            ("BOTTOMPADDING", (0, 0), (-1, -1), 3),
            ("TOPPADDING", (0, 0), (-1, -1), 3),
        ]))
        elements.append(timeline_table)

    # --- Disclaimer ---
    elements.append(Spacer(1, 24))
    elements.append(Paragraph(
        "This report is generated for informational purposes to support fraud investigation. "
        "All data is derived from publicly available HHS Medicaid claims records. "
        "Anomalies identified herein warrant further investigation and do not constitute proof of fraud.",
        ParagraphStyle("Disclaimer", parent=small_style, fontSize=7, textColor=colors.grey)
    ))

    doc.build(elements)
    return output_path
