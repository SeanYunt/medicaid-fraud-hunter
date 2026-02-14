from dataclasses import dataclass, field
from datetime import date
from enum import Enum


class RedFlagType(Enum):
    VOLUME_IMPOSSIBILITY = "volume_impossibility"
    REVENUE_OUTLIER = "revenue_outlier"
    BILLING_SPIKE = "billing_spike"
    SUSPICIOUS_CONSISTENCY = "suspicious_consistency"


@dataclass
class Provider:
    """A healthcare provider identified by NPI."""
    npi: str
    name: str = ""
    specialty: str = ""
    address: str = ""
    city: str = ""
    state: str = ""
    zip: str = ""
    enumeration_type: str = ""
    billing_npi: str = ""
    servicing_npi: str = ""


@dataclass
class RedFlag:
    """A specific piece of evidence against a provider."""
    flag_type: RedFlagType
    description: str
    severity: float  # 0.0 to 1.0
    evidence: dict = field(default_factory=dict)


@dataclass
class ScanResult:
    """Result of scanning a provider for anomalies."""
    npi: str
    provider_name: str
    overall_score: float  # 0.0 to 1.0, higher = more suspicious
    red_flags: list[RedFlag] = field(default_factory=list)
    total_billed: float = 0.0
    claim_count: int = 0


@dataclass
class Dossier:
    """A complete provider dossier for bounty submission."""
    provider: Provider
    scan_result: ScanResult
    claims_summary: dict = field(default_factory=dict)
    peer_comparison: dict = field(default_factory=dict)
    timeline: list[dict] = field(default_factory=list)
