"""
IMEA Webhook-Server v3.1
FastAPI · Railway.app
Endpoints: lead-qualify, vermittlerprotokoll, gate-check, zulieferer, zins-update
"""

import os
import json
import logging
from datetime import datetime, timezone
from typing import Optional

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, validator

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("imea-webhook")

# ── App ───────────────────────────────────────────────────────────────────────
app = FastAPI(
    title="IMEA Webhook-Server",
    version="3.1.0",
    docs_url=None,   # Disable public docs
    redoc_url=None,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # Restrict to IMEA domains in production if desired
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ── Environment ───────────────────────────────────────────────────────────────
GHL_API_KEY        = os.environ.get("GHL_API_KEY", "")
GHL_LOCATION_ID    = os.environ.get("GHL_LOCATION_ID", "YhHlEER2R1qjYlTnP0mn")
GHL_API_BASE       = "https://services.leadconnectorhq.com"

# GHL Custom Setting field key for Beispielannuität
ANNUITAET_FIELD_KEY = "IMEA Beispiel Annuitaet"

# Fallback default (only used if GHL has never been set)
ANNUITAET_DEFAULT   = 5.7

# ── In-memory cache for annuität (avoids repeated GHL calls within same request) ──
_annuitaet_cache: dict = {
    "value": None,
    "updated_at": None,
}


# ─────────────────────────────────────────────────────────────────────────────
#  GHL API Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _ghl_headers() -> dict:
    return {
        "Authorization": f"Bearer {GHL_API_KEY}",
        "Content-Type": "application/json",
        "Version": "2021-07-28",
    }


async def ghl_get_custom_value(field_key: str) -> Optional[float]:
    """Read a Location-level Custom Value from GHL."""
    url = f"{GHL_API_BASE}/locations/{GHL_LOCATION_ID}/customValues"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url, headers=_ghl_headers())
            resp.raise_for_status()
            data = resp.json()
            # GHL returns { "customValues": [ { "id": ..., "name": ..., "value": ... } ] }
            for cv in data.get("customValues", []):
                if cv.get("name", "").lower() == field_key.lower():
                    try:
                        return float(cv["value"])
                    except (ValueError, TypeError):
                        return None
    except Exception as e:
        logger.warning(f"GHL get_custom_value failed: {e}")
    return None


async def ghl_upsert_custom_value(field_key: str, value: float) -> bool:
    """Create or update a Location-level Custom Value in GHL."""
    # First, list existing values to find the ID if it exists
    url_list = f"{GHL_API_BASE}/locations/{GHL_LOCATION_ID}/customValues"
    existing_id = None

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url_list, headers=_ghl_headers())
            resp.raise_for_status()
            data = resp.json()
            for cv in data.get("customValues", []):
                if cv.get("name", "").lower() == field_key.lower():
                    existing_id = cv.get("id")
                    break

            if existing_id:
                # Update existing
                url_update = f"{GHL_API_BASE}/locations/{GHL_LOCATION_ID}/customValues/{existing_id}"
                patch_resp = await client.put(
                    url_update,
                    headers=_ghl_headers(),
                    json={"name": field_key, "value": str(value)},
                )
                patch_resp.raise_for_status()
                logger.info(f"GHL custom value '{field_key}' updated to {value}")
            else:
                # Create new
                create_resp = await client.post(
                    url_list,
                    headers=_ghl_headers(),
                    json={"name": field_key, "value": str(value)},
                )
                create_resp.raise_for_status()
                logger.info(f"GHL custom value '{field_key}' created with value {value}")

            return True

    except Exception as e:
        logger.error(f"GHL upsert_custom_value failed: {e}")
        return False


async def get_current_annuitaet() -> float:
    """Return current Beispielannuität — from GHL, cache, or default."""
    # Try GHL first
    ghl_val = await ghl_get_custom_value(ANNUITAET_FIELD_KEY)
    if ghl_val is not None:
        _annuitaet_cache["value"] = ghl_val
        return ghl_val

    # Fall back to in-memory cache
    if _annuitaet_cache["value"] is not None:
        return _annuitaet_cache["value"]

    # Hard fallback (first start only)
    logger.warning(f"Using hardcoded default annuität: {ANNUITAET_DEFAULT}")
    return ANNUITAET_DEFAULT


# ─────────────────────────────────────────────────────────────────────────────
#  Pydantic Models
# ─────────────────────────────────────────────────────────────────────────────

class ZinsUpdateRequest(BaseModel):
    annuitaet: float = Field(..., gt=0, le=20, description="Durchschnitts-Annuität in Prozent, z.B. 5.7")

    @validator("annuitaet")
    def round_to_two(cls, v):
        return round(v, 2)


class ZinsUpdateResponse(BaseModel):
    success: bool
    value: float
    updated_at: str


class ZinsCurrentResponse(BaseModel):
    value: Optional[float]
    updated_at: Optional[str]


# ─────────────────────────────────────────────────────────────────────────────
#  /webhook/zins-update  (NEW — Feature 1 Backend)
# ─────────────────────────────────────────────────────────────────────────────

@app.post(
    "/webhook/zins-update",
    response_model=ZinsUpdateResponse,
    summary="Annuität aktualisieren",
    tags=["Zins"],
)
async def zins_update(payload: ZinsUpdateRequest):
    """
    Eric Kretschmer trägt die aktuelle Durchschnitts-Annuität ein.
    Speichert den Wert als GHL Location Custom Value 'beispielannuitaet'.
    Nur neue Lead-Scorings verwenden den neuen Wert — keine Rückwirkung.
    """
    value = payload.annuitaet
    now_iso = datetime.now(timezone.utc).isoformat()

    # Persist to GHL
    success = await ghl_upsert_custom_value(ANNUITAET_FIELD_KEY, value)

    if not success:
        raise HTTPException(
            status_code=502,
            detail="GHL-Aktualisierung fehlgeschlagen. Bitte erneut versuchen.",
        )

    # Update in-memory cache
    _annuitaet_cache["value"] = value
    _annuitaet_cache["updated_at"] = now_iso

    logger.info(f"Annuität aktualisiert: {value}% — {now_iso}")

    return ZinsUpdateResponse(
        success=True,
        value=value,
        updated_at=now_iso,
    )


# ─────────────────────────────────────────────────────────────────────────────
#  /zins/current  (GET — für die Zins-Seite: letzten Wert anzeigen)
# ─────────────────────────────────────────────────────────────────────────────

@app.get(
    "/zins/current",
    response_model=ZinsCurrentResponse,
    summary="Aktuellen Annuität-Wert lesen",
    tags=["Zins"],
)
async def zins_current():
    """Gibt den aktuell in GHL gespeicherten Annuität-Wert zurück."""
    value = await ghl_get_custom_value(ANNUITAET_FIELD_KEY)
    updated_at = _annuitaet_cache.get("updated_at")
    return ZinsCurrentResponse(value=value, updated_at=updated_at)


# ─────────────────────────────────────────────────────────────────────────────
#  /zins  (GET — Serve the HTML form page)
# ─────────────────────────────────────────────────────────────────────────────

@app.get(
    "/zins",
    response_class=HTMLResponse,
    summary="Zins-Update Formular",
    tags=["Zins"],
    include_in_schema=False,
)
async def zins_page():
    """Serve the Zins-Update HTML form."""
    import pathlib
    html_path = pathlib.Path(__file__).parent / "static" / "zins.html"
    if html_path.exists():
        return HTMLResponse(content=html_path.read_text(encoding="utf-8"))
    return HTMLResponse(content="<h1>Zins-Update Seite nicht gefunden</h1>", status_code=404)


# ─────────────────────────────────────────────────────────────────────────────
#  /webhook/lead-qualify  (UPDATED — reads beispielannuitaet dynamically)
# ─────────────────────────────────────────────────────────────────────────────

class LeadQualifyRequest(BaseModel):
    # Core income fields
    frei_verfuegbar: Optional[float] = Field(None, description="Frei verfügbares Einkommen in EUR/Monat")
    netto_einkommen: Optional[float] = Field(None, description="Nettoeinkommen in EUR/Monat")
    eigenkapital: Optional[float] = Field(None, description="Eigenkapital in EUR")

    # Contact info (optional — for GHL update)
    contact_id: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None

    # Additional scoring fields
    beschaeftigung: Optional[str] = None   # "angestellt" | "selbstaendig" | "beamter"
    schufa_ok: Optional[bool] = None
    alter: Optional[int] = None
    unterhalt: Optional[bool] = None
    bestehende_kredite: Optional[float] = Field(None, description="Monatliche Kreditbelastung in EUR")

    # Raw text for Claude analysis (optional)
    raw_text: Optional[str] = None


@app.post(
    "/webhook/lead-qualify",
    summary="Lead-Scoring mit dynamischer Annuität",
    tags=["Lead"],
)
async def lead_qualify(payload: LeadQualifyRequest):
    """
    Claude-basiertes Lead-Scoring (1–10).
    Liest beispielannuitaet IMMER aktuell aus GHL — kein hardcodierter Fallback
    außer beim allerersten Start (5.7).

    Formel:
      Max. Rate = frei_verfuegbar × 0.80
      Geschätztes Volumen = Max. Rate ÷ (annuitaet / 100 / 12)
    """
    # ── 1. Dynamisch aktuelle Annuität aus GHL lesen ──
    annuitaet = await get_current_annuitaet()
    logger.info(f"Lead-Qualify: Annuität aus GHL = {annuitaet}%")

    # ── 2. Volumenberechnung ──
    volumen_100 = None
    volumen_110 = None
    max_rate = None

    if payload.frei_verfuegbar and payload.frei_verfuegbar > 0:
        max_rate = payload.frei_verfuegbar * 0.80
        monthly_factor = annuitaet / 100 / 12
        if monthly_factor > 0:
            volumen_100 = round(max_rate / monthly_factor)
            volumen_110 = round(volumen_100 / 1.10)  # inkl. 10% Nebenkosten

    # ── 3. Basis-Scoring-Logik ──
    score = 5  # Startwert
    disqualified = False
    disqualification_reason = None

    # Hard Disqualifiers
    if payload.schufa_ok is False:
        disqualified = True
        disqualification_reason = "Schufa-Negativmerkmal"
        score = 1

    elif payload.netto_einkommen and payload.netto_einkommen < 2800:
        disqualified = True
        disqualification_reason = f"Nettoeinkommen unter Minimum (2.800 €): {payload.netto_einkommen:.0f} €"
        score = 2

    else:
        # Positive scoring factors
        if payload.netto_einkommen:
            if payload.netto_einkommen >= 5000:
                score += 2
            elif payload.netto_einkommen >= 3500:
                score += 1

        if payload.eigenkapital:
            if payload.eigenkapital >= 30000:
                score += 2
            elif payload.eigenkapital >= 15000:
                score += 1

        if payload.beschaeftigung in ("angestellt", "beamter"):
            score += 1

        if payload.alter and 25 <= payload.alter <= 45:
            score += 1

        # Negative factors
        if payload.unterhalt:
            score -= 1

        if payload.bestehende_kredite and payload.bestehende_kredite > 500:
            score -= 1

        score = max(1, min(10, score))

    # ── 4. Ergebnis ──
    result = {
        "score": score,
        "disqualified": disqualified,
        "disqualification_reason": disqualification_reason,
        "annuitaet_used": annuitaet,
        "max_rate": round(max_rate, 2) if max_rate else None,
        "estimated_volume_100pct": volumen_100,
        "estimated_volume_110pct_nk": volumen_110,
        "calculated_at": datetime.now(timezone.utc).isoformat(),
    }

    logger.info(f"Lead-Qualify result: score={score}, volumen={volumen_100}, annuität={annuitaet}%")

    # ── 5. Optional: GHL Contact Update ──
    if payload.contact_id and GHL_API_KEY:
        await _update_ghl_contact_score(payload.contact_id, result)

    return result


async def _update_ghl_contact_score(contact_id: str, result: dict):
    """Push lead score and volume back to GHL contact custom fields."""
    url = f"{GHL_API_BASE}/contacts/{contact_id}"
    payload = {
        "customFields": [
            {"key": "lead_score", "field_value": str(result["score"])},
            {"key": "estimated_volume", "field_value": str(result.get("estimated_volume_100pct", ""))},
            {"key": "annuitaet_used", "field_value": str(result["annuitaet_used"])},
        ]
    }
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            resp = await client.put(url, headers=_ghl_headers(), json=payload)
            resp.raise_for_status()
    except Exception as e:
        logger.warning(f"GHL contact score update failed for {contact_id}: {e}")


# ─────────────────────────────────────────────────────────────────────────────
#  /webhook/gate-check  (existing — unchanged stub)
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/webhook/gate-check", tags=["Pipeline"])
async def gate_check(request: Request):
    """Pipeline Gate-Logik. Custom Field: Tct2XbQgJhJUyOMmsod5"""
    body = await request.json()
    logger.info(f"gate-check called: {json.dumps(body)[:200]}")
    return {"status": "ok", "message": "Gate check received"}


# ─────────────────────────────────────────────────────────────────────────────
#  /webhook/vermittlerprotokoll  (existing — unchanged stub)
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/webhook/vermittlerprotokoll", tags=["Dokumente"])
async def vermittlerprotokoll(request: Request):
    """Auto-PDF-Generierung + Dropbox-Upload für Vermittlerprotokoll V2."""
    body = await request.json()
    logger.info(f"vermittlerprotokoll called: {json.dumps(body)[:200]}")
    return {"status": "ok", "message": "Vermittlerprotokoll received"}


# ─────────────────────────────────────────────────────────────────────────────
#  /webhook/zulieferer  (existing — unchanged stub)
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/webhook/zulieferer", tags=["Objekte"])
async def zulieferer(request: Request):
    """Objektdaten-Extraktion → GHL Deal."""
    body = await request.json()
    logger.info(f"zulieferer called: {json.dumps(body)[:200]}")
    return {"status": "ok", "message": "Zulieferer data received"}


# ─────────────────────────────────────────────────────────────────────────────
#  /api/ghl-proxy  (GHL Proxy für Check-In Tool)
# ─────────────────────────────────────────────────────────────────────────────

@app.api_route("/api/ghl-proxy/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def ghl_proxy(path: str, request: Request):
    """Transparent proxy to GHL API for the Check-In Tool."""
    target_url = f"{GHL_API_BASE}/{path}"
    body = await request.body()
    headers = dict(request.headers)
    headers.pop("host", None)
    if GHL_API_KEY:
        headers["Authorization"] = f"Bearer {GHL_API_KEY}"

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.request(
                method=request.method,
                url=target_url,
                headers=headers,
                content=body,
                params=dict(request.query_params),
            )
            return JSONResponse(
                content=resp.json() if resp.content else {},
                status_code=resp.status_code,
            )
    except Exception as e:
        logger.error(f"GHL proxy error: {e}")
        raise HTTPException(status_code=502, detail="GHL proxy error")


# ─────────────────────────────────────────────────────────────────────────────
#  Health check
# ─────────────────────────────────────────────────────────────────────────────

@app.get("/health", include_in_schema=False)
async def health():
    return {
        "status": "ok",
        "version": "3.1.0",
        "ghl_location": GHL_LOCATION_ID,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/", include_in_schema=False)
async def root():
    return {"service": "IMEA Webhook-Server", "version": "3.1.0", "status": "running"}
