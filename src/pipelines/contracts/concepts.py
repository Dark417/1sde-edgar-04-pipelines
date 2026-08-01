"""Canonical concept mapping: XBRL tag -> the name gold and the API speak.

Filers report the same economic quantity under different us-gaap tags depending on
industry and on which year's taxonomy their filing agent used. Gold compares values
*within* a canonical concept, so a mapping that silently drops a tag makes a company
look like it stopped reporting revenue.

``preference`` is the tie-break when one filer reports several source tags for the
same canonical concept in the same period. It is explicit and total so that the result
does not depend on join order (AGENTS.global.md rule 5).
"""

from __future__ import annotations

from typing import Final

from .models import ConceptMapping

__all__ = ["CANONICAL_CONCEPTS", "CONCEPT_MAPPINGS", "mappings_for_tag", "tags_to_fetch"]

M = ConceptMapping

CONCEPT_MAPPINGS: Final[tuple[ConceptMapping, ...]] = (
    # Revenue. RevenueFromContractWithCustomerExcludingAssessedTax is the ASC 606 tag
    # and wins where present; Revenues is the legacy tag many smaller filers still use.
    M("revenue_total", "us-gaap", "RevenueFromContractWithCustomerExcludingAssessedTax", 10, "monetary", "Revenue"),
    M("revenue_total", "us-gaap", "RevenueFromContractWithCustomerIncludingAssessedTax", 20, "monetary", "Revenue"),
    M("revenue_total", "us-gaap", "Revenues", 30, "monetary", "Revenue"),
    M("revenue_total", "us-gaap", "SalesRevenueNet", 40, "monetary", "Revenue"),
    # Earnings.
    M("net_income", "us-gaap", "NetIncomeLoss", 10, "monetary", "Net income"),
    M("net_income", "us-gaap", "ProfitLoss", 20, "monetary", "Net income"),
    M("operating_income", "us-gaap", "OperatingIncomeLoss", 10, "monetary", "Operating income"),
    M("gross_profit", "us-gaap", "GrossProfit", 10, "monetary", "Gross profit"),
    # Balance sheet.
    M("assets_total", "us-gaap", "Assets", 10, "monetary", "Total assets"),
    M("liabilities_total", "us-gaap", "Liabilities", 10, "monetary", "Total liabilities"),
    M("equity_total", "us-gaap", "StockholdersEquity", 10, "monetary", "Stockholders equity"),
    M(
        "equity_total",
        "us-gaap",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
        20,
        "monetary",
        "Stockholders equity",
    ),
    M("cash_and_equivalents", "us-gaap", "CashAndCashEquivalentsAtCarryingValue", 10, "monetary", "Cash and equivalents"),
    # Per-share.
    M("eps_basic", "us-gaap", "EarningsPerShareBasic", 10, "per_share", "EPS basic"),
    M("eps_diluted", "us-gaap", "EarningsPerShareDiluted", 10, "per_share", "EPS diluted"),
    M("shares_outstanding", "dei", "EntityCommonStockSharesOutstanding", 10, "shares", "Shares outstanding"),
)

CANONICAL_CONCEPTS: Final[tuple[str, ...]] = tuple(
    dict.fromkeys(m.canonical for m in CONCEPT_MAPPINGS)
)


def mappings_for_tag(taxonomy: str, tag: str) -> ConceptMapping | None:
    """Return the mapping for a source tag, or ``None`` when it is unmapped.

    Unmapped is not an error. ``silver.financial_fact`` keeps the row with a null
    ``concept_canonical`` -- discarding facts we have not mapped yet would make adding
    a concept later require a bronze replay.
    """
    for m in CONCEPT_MAPPINGS:
        if m.taxonomy == taxonomy and m.tag == tag:
            return m
    return None


def tags_to_fetch() -> tuple[tuple[str, str], ...]:
    """(taxonomy, tag) pairs a landing fetcher should request, deduped and ordered."""
    return tuple(dict.fromkeys((m.taxonomy, m.tag) for m in CONCEPT_MAPPINGS))
