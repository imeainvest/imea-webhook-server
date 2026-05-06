"""
IMEA Webhook-Server v4.0
FastAPI · Railway.app
Endpoints: lead-qualify, vermittlerprotokoll, gate-check, zulieferer, zins-update,
           objekt-intake (NEU v4.0)
"""

import os
import re
import json
import base64
import hashlib
import logging
import pathlib
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

import httpx
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
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
# ── Startup: Whitelist automatisch befüllen ───────────────────────────────────
KNOWN_ZULIEFERER = [
    {"email": "info@finest-invest.de",       "name": "Finest Invest",             "domain": "finest-invest.de"},
    {"email": "kontakt@finest-invest.de",    "name": "Finest Invest",             "domain": "finest-invest.de"},
    {"email": "falk.jaeger@finest-invest.de","name": "Falk Jäger",               "domain": "finest-invest.de"},
    {"email": "info@poller-immobilien.de",   "name": "Poller Immobilien",         "domain": "poller-immobilien.de"},
    {"email": "s.poller@poller-immobilien.de","name": "Sebastian Poller",         "domain": "poller-immobilien.de"},
    {"email": "info@convista.de",            "name": "Convista",                  "domain": "convista.de"},
    {"email": "objekte@convista.de",         "name": "Convista",                  "domain": "convista.de"},
    {"email": "info@vonovia.de",             "name": "Vonovia",                   "domain": "vonovia.de"},
    {"email": "verkauf@vonovia.de",          "name": "Vonovia",                   "domain": "vonovia.de"},
    {"email": "info@deutsche-wohnen.de",     "name": "Deutsche Wohnen",           "domain": "deutsche-wohnen.de"},
    {"email": "info@tag-immobilien.de",      "name": "TAG Immobilien",            "domain": "tag-immobilien.de"},
    {"email": "info@adler-group.de",         "name": "Adler Group",               "domain": "adler-group.de"},
    {"email": "immobilien@immonet.de",       "name": "Immonet",                   "domain": "immonet.de"},
    {"email": "info@engel-voelkers.de",      "name": "Engel & Völkers",           "domain": "engel-voelkers.de"},
    {"email": "info@remax.de",               "name": "RE/MAX",                    "domain": "remax.de"},
    {"email": "sh@imea-finanz.de",           "name": "Stefan Happatz (Forward)",  "domain": "imea-finanz.de"},
]

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup: Whitelist automatisch befüllen."""
    for z in KNOWN_ZULIEFERER:
        _whitelist[z["email"]] = z
    logger.info(f"[STARTUP] Whitelist befüllt: {len(_whitelist)} Zulieferer")
    yield
    logger.info("[SHUTDOWN] IMEA Webhook-Server wird beendet.")


app = FastAPI(
    title="IMEA Webhook-Server",
    version="4.0.1",
    docs_url=None,
    redoc_url=None,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["*"],
)

# ── Environment ───────────────────────────────────────────────────────────────
GHL_API_KEY           = os.environ.get("GHL_API_KEY", "")
GHL_LOCATION_ID       = os.environ.get("GHL_LOCATION_ID", "YhHlEER2R1qjYlTnP0mn")
GHL_API_BASE          = "https://services.leadconnectorhq.com"
ANTHROPIC_API_KEY     = os.environ.get("ANTHROPIC_API_KEY", "")
DROPBOX_REFRESH_TOKEN = os.environ.get("DROPBOX_REFRESH_TOKEN", "")
DROPBOX_APP_KEY       = os.environ.get("DROPBOX_APP_KEY", "")
DROPBOX_APP_SECRET    = os.environ.get("DROPBOX_APP_SECRET", "")
WEBHOOK_TOKEN_OBJ     = os.environ.get("WEBHOOK_TOKEN_OBJ_INTAKE", "imea-obj-intake-2026-secure")
DIALOG360_API_KEY     = os.environ.get("DIALOG360_API_KEY", "")
STEFAN_PHONE          = os.environ.get("STEFAN_PHONE", "+4915563880100")
MYSQL_HOST            = os.environ.get("MYSQL_HOST", "")
MYSQL_PORT            = int(os.environ.get("MYSQL_PORT", "3306"))
MYSQL_USER            = os.environ.get("MYSQL_USER", "")
MYSQL_PASS            = os.environ.get("MYSQL_PASSWORD", "")
MYSQL_DB              = os.environ.get("MYSQL_DATABASE", "railway")

# GHL Custom Setting field key for Beispielannuität
ANNUITAET_FIELD_KEY = "IMEA Beispiel Annuitaet"
ANNUITAET_DEFAULT   = 5.7

_annuitaet_cache: dict = {"value": None, "updated_at": None}

# ── NK-Quoten nach Stadt ──────────────────────────────────────────────────────
NK_QUOTEN = {
    "dresden":   0.0807,
    "leipzig":   0.0807,
    "münchen":   0.0907,
    "muenchen":  0.0907,
    "berlin":    0.1207,
    "frankfurt": 0.1207,
    "default":   0.1007,
}

ENDPREIS_STEFAN_ZULIEFERER = [
    "finest", "finest invest", "falk jäger", "ronny stelzer", "uwe wagner",
    "poller", "sebastian poller", "sylvana bürger",
]
ENDPREIS_AUFSCHLAG_DEFAULT = 1.12
BAUJAHR_RND_SCHWELLE = 1990
BAUJAHR_AFA_3PCT_AB  = 2023

# Pipeline Stage IDs (GHL)
STAGE_EINGANG       = "stage_eingang"
STAGE_WARTEN_STEFAN = "stage_warten_stefan"

# Objekt-Intake Whitelist (in-memory, wird durch Bootstrap befüllt)
_whitelist: Dict[str, dict] = {}
_processed_hashes: Dict[str, str] = {}

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
    url = f"{GHL_API_BASE}/locations/{GHL_LOCATION_ID}/customValues"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url, headers=_ghl_headers())
            resp.raise_for_status()
            data = resp.json()
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
                url_update = f"{GHL_API_BASE}/locations/{GHL_LOCATION_ID}/customValues/{existing_id}"
                patch_resp = await client.put(url_update, headers=_ghl_headers(), json={"name": field_key, "value": str(value)})
                patch_resp.raise_for_status()
            else:
                create_resp = await client.post(url_list, headers=_ghl_headers(), json={"name": field_key, "value": str(value)})
                create_resp.raise_for_status()
            return True
    except Exception as e:
        logger.error(f"GHL upsert_custom_value failed: {e}")
        return False


async def get_current_annuitaet() -> float:
    ghl_val = await ghl_get_custom_value(ANNUITAET_FIELD_KEY)
    if ghl_val is not None:
        _annuitaet_cache["value"] = ghl_val
        return ghl_val
    if _annuitaet_cache["value"] is not None:
        return _annuitaet_cache["value"]
    logger.warning(f"Using hardcoded default annuität: {ANNUITAET_DEFAULT}")
    return ANNUITAET_DEFAULT


# ─────────────────────────────────────────────────────────────────────────────
#  Pydantic Models (Legacy)
# ─────────────────────────────────────────────────────────────────────────────

class ZinsUpdateRequest(BaseModel):
    annuitaet: float = Field(..., gt=0, le=20)

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


class LeadQualifyRequest(BaseModel):
    frei_verfuegbar: Optional[float] = None
    netto_einkommen: Optional[float] = None
    eigenkapital: Optional[float] = None
    contact_id: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    email: Optional[str] = None
    beschaeftigung: Optional[str] = None
    schufa_ok: Optional[bool] = None
    alter: Optional[int] = None
    unterhalt: Optional[bool] = None
    bestehende_kredite: Optional[float] = None
    raw_text: Optional[str] = None


# ─────────────────────────────────────────────────────────────────────────────
#  OBJEKT-INTAKE — Hilfsfunktionen
# ─────────────────────────────────────────────────────────────────────────────

def _obj_check_newsletter(subject: str, body: str) -> bool:
    keywords = ["newsletter", "unsubscribe", "abmelden", "abbestellen",
                "marktbericht", "immobilienmarkt aktuell", "pressemitteilung",
                "neuigkeiten aus dem markt", "werbung", "anzeige"]
    text = (subject + " " + body).lower()
    return any(k in text for k in keywords)


def _obj_check_sold(subject: str, body: str) -> bool:
    keywords = ["verkauft", "bereits vergeben", "nicht mehr verfügbar",
                "reserviert", "sold", "not available", "vergeben"]
    text = (subject + " " + body).lower()
    return any(k in text for k in keywords)


def _obj_is_objekt_mail(from_email: str, subject: str, body: str) -> tuple:
    """Heuristik: Ist das eine Objekt-Zulieferung?"""
    score = 0.0
    text = (subject + " " + body).lower()

    obj_keywords = [
        "exposé", "expose", "wohnung", "apartment", "immobilie",
        "kaufpreis", "kaltmiete", "warmmiete", "baujahr", "wohnfläche",
        "wohneinheit", "we ", "etage", "grundbuch", "hausgeld",
        "energieausweis", "teilungserklärung", "mietvertrag",
        "rendite", "kaufangebot", "objektunterlagen", "objektdaten",
    ]
    for kw in obj_keywords:
        if kw in text:
            score += 0.08
    if score >= 0.3:
        return True, min(score, 1.0), "keyword_match"
    return False, score, "no_match"


def _obj_whitelist_check(from_email: str, from_name: str, body: str) -> dict:
    """Prüft ob Absender in der Whitelist ist."""
    email_lower = from_email.lower()
    domain = email_lower.split("@")[-1] if "@" in email_lower else ""

    # Direkte E-Mail-Übereinstimmung
    if email_lower in _whitelist:
        entry = _whitelist[email_lower]
        return {"status": "whitelisted", "zulieferer_name": entry.get("name", from_name), "from_email": from_email, "domain": domain}

    # Domain-Übereinstimmung
    for wl_email, entry in _whitelist.items():
        if domain and domain == entry.get("domain", ""):
            return {"status": "whitelisted", "zulieferer_name": entry.get("name", from_name), "from_email": from_email, "domain": domain}

    # Forward-Erkennung (Stefan leitet weiter)
    stefan_emails = ["sh@imea-finanz.de", "stefan@imea-invest.de", "stefan.happatz@gmail.com"]
    if email_lower in stefan_emails:
        # Suche Original-Absender im Body
        fwd_match = re.search(r'(?:von|from):\s*([^\n<]+?)(?:\s*<([^>]+)>)?', body, re.IGNORECASE)
        if fwd_match:
            orig_name = fwd_match.group(1).strip()
            orig_email = fwd_match.group(2).strip() if fwd_match.group(2) else ""
            return {"status": "whitelisted", "zulieferer_name": orig_name or orig_email, "from_email": orig_email or from_email, "domain": domain, "forwarded_by_stefan": True}

    # Unbekannt → Pending
    confidence = 0.7 if domain else 0.4
    return {"status": "pending_confirmation", "zulieferer_name": from_name or from_email, "from_email": from_email, "domain": domain, "confidence": confidence}


def _obj_classify_attachments(attachments: list) -> list:
    """Klassifiziert Anhänge in 17 Kategorien."""
    CATEGORY_MAP = {
        "obj-expose":           ["expose", "exposé", "angebot", "objektbeschreibung"],
        "obj-grundriss":        ["grundriss", "floor", "plan"],
        "obj-energieausweis":   ["energie", "energieausweis", "energiepass"],
        "obj-mietvertrag":      ["mietvertrag", "mietbescheinigung"],
        "obj-nebenkostenabr":   ["nebenkostenabrechnung", "nka", "betriebskosten"],
        "obj-hausgeldabr":      ["hausgeldabrechnung", "weg-abrechnung", "weg abrechnung"],
        "obj-teilungserklaerung": ["teilungserklärung", "teilungserklaerung", "te "],
        "obj-grundbuch":        ["grundbuch", "grundbuchauszug"],
        "obj-kaufvertrag":      ["kaufvertrag", "notarvertrag"],
        "obj-fotos":            ["foto", "bild", "photo", "image", ".jpg", ".jpeg", ".png"],
        "obj-lageplan":         ["lageplan", "stadtplan", "karte"],
        "obj-wohnflaeche":      ["wohnflächenberechnung", "wohnfläche", "flächenberechnung"],
        "obj-protokoll":        ["protokoll", "eigentümerversammlung", "ev-protokoll"],
        "obj-wirtschaftsplan":  ["wirtschaftsplan", "haushaltsplan"],
        "obj-instandhaltung":   ["instandhaltung", "rücklage", "ihr "],
        "obj-sonstiges":        [],
    }

    classified = []
    for att in attachments:
        fname = att.get("filename", "").lower()
        mime  = att.get("mime_type", "").lower()
        tag   = "obj-sonstiges"
        for category, keywords in CATEGORY_MAP.items():
            if any(kw in fname for kw in keywords):
                tag = category
                break
        # Fotos nach MIME
        if tag == "obj-sonstiges" and mime.startswith("image/"):
            tag = "obj-fotos"
        classified.append({**att, "tag": tag})
    return classified


def _obj_determine_profil(classified: list, vermietet: bool, baujahr: Optional[int]) -> tuple:
    """Bestimmt Pflicht-Profil und fehlende Dokumente."""
    tags = {a["tag"] for a in classified}

    # Neubau-Profil (kein Baujahr oder Baujahr >= 2020)
    is_neubau = baujahr and baujahr >= 2020

    if is_neubau:
        pflicht = {"obj-expose", "obj-grundriss", "obj-kaufvertrag"}
        profil = "neubau"
    elif vermietet:
        pflicht = {"obj-expose", "obj-grundriss", "obj-mietvertrag", "obj-hausgeldabr", "obj-energieausweis"}
        profil = "bestand-vermietet"
    else:
        pflicht = {"obj-expose", "obj-grundriss", "obj-energieausweis"}
        profil = "bestand-leer"

    fehlend = [p for p in pflicht if p not in tags]
    return profil, list(tags), fehlend


def _obj_extract_with_claude(classified: list, body_text: str) -> dict:
    """Extrahiert Objektdaten mit Claude Sonnet. Fallback auf Opus bei Confidence < 0.6."""
    if not ANTHROPIC_API_KEY:
        logger.warning("Kein ANTHROPIC_API_KEY — Extraktion übersprungen")
        return {"objekt": {}, "confidence": 0.0, "extraction_warnings": ["no_api_key"]}

    # Anhänge als Text zusammenstellen
    attachment_texts = []
    for att in classified:
        if att.get("tag") in ("obj-expose", "obj-grundriss", "obj-mietvertrag"):
            data_b64 = att.get("data_base64", "")
            if data_b64:
                try:
                    decoded = base64.b64decode(data_b64).decode("utf-8", errors="ignore")[:3000]
                    attachment_texts.append(f"[{att['tag']}] {att.get('filename','')}: {decoded}")
                except Exception:
                    pass

    content = body_text[:2000] + "\n\n" + "\n\n".join(attachment_texts[:3])

    system_prompt = """Du bist ein Immobilien-Daten-Extraktor für IMEA Invest.
Extrahiere alle verfügbaren Objektdaten aus dem Text und gib sie als JSON zurück.

Pflichtfelder (wenn vorhanden):
- stadt, plz, strasse, hausnummer, we_nr, etage
- flaeche_qm (float), zimmer (float), baujahr (int)
- kaufpreis_einkauf (float, INTERN - nicht für Kunden)
- kaltmiete_aktuell (float), warmmiete_aktuell (float)
- vermietet (bool), hausgeld_gesamt (float)
- hausgeld_umlagefaehig (float), hausgeld_nicht_umlagefaehig (float)
- instandhaltungsruecklage (float)
- energie_wert_kwh (float), energie_effizienzklasse (str)
- heizungsart (str), letzte_sanierung (int)
- bemerkungen_zulieferer (str)

Gib zurück:
{
  "objekt": { ... alle extrahierten Felder ... },
  "confidence": 0.0-1.0,
  "extraction_warnings": ["fehlende_felder", ...]
}

Confidence-Regeln:
- >= 0.85: alle Pflichtfelder vorhanden
- 0.60-0.84: meiste Felder vorhanden
- < 0.60: kritische Felder fehlen (Kaufpreis, Stadt, Fläche)"""

    try:
        import anthropic
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

        # Sonnet Primary
        model = "claude-sonnet-4-5"
        response = client.messages.create(
            model=model,
            max_tokens=2000,
            system=system_prompt,
            messages=[{"role": "user", "content": f"Extrahiere Objektdaten:\n\n{content}"}]
        )
        result_text = response.content[0].text

        # JSON extrahieren
        json_match = re.search(r'\{.*\}', result_text, re.DOTALL)
        if json_match:
            result = json.loads(json_match.group())
            confidence = result.get("confidence", 0.5)

            # Opus-Fallback bei niedriger Confidence
            if confidence < 0.6:
                logger.info(f"Confidence {confidence:.2f} < 0.6 — Opus-Fallback")
                response2 = client.messages.create(
                    model="claude-opus-4-5",
                    max_tokens=2000,
                    system=system_prompt,
                    messages=[{"role": "user", "content": f"Extrahiere Objektdaten (zweiter Versuch, bitte sehr sorgfältig):\n\n{content}"}]
                )
                result_text2 = response2.content[0].text
                json_match2 = re.search(r'\{.*\}', result_text2, re.DOTALL)
                if json_match2:
                    result2 = json.loads(json_match2.group())
                    if result2.get("confidence", 0) > confidence:
                        result = result2

            return result

    except Exception as e:
        logger.error(f"Claude Extraktion fehlgeschlagen: {e}")

    return {"objekt": {}, "confidence": 0.0, "extraction_warnings": [f"extraction_error"]}


def _obj_generate_id(stadt: str, strasse: str, we_nr: str) -> str:
    """Generiert eindeutige ObjektID."""
    raw = f"{stadt}_{strasse}_{we_nr}_{datetime.utcnow().strftime('%Y%m')}"
    hash_part = hashlib.md5(raw.encode()).hexdigest()[:6].upper()
    city_code = (stadt[:3] if stadt else "OBJ").upper()
    return f"OBJ-{city_code}-{hash_part}"


def _obj_calculate(objekt: dict, zulieferer_name: str) -> dict:
    """Erstberechnung: NK, Annuität, Cashflow, AfA."""
    einkauf = float(objekt.get("kaufpreis_einkauf") or 0)
    kaltmiete = float(objekt.get("kaltmiete_aktuell") or 0)
    flaeche = float(objekt.get("flaeche_qm") or 0)
    baujahr = objekt.get("baujahr")
    stadt = (objekt.get("stadt") or "").lower()
    hausgeld_nu = float(objekt.get("hausgeld_nicht_umlagefaehig") or 0)

    if einkauf <= 0:
        return {"error": "kein_einkaufspreis"}

    # Endpreis-Logik
    zl_lower = zulieferer_name.lower()
    is_stefan_zl = any(s in zl_lower for s in ENDPREIS_STEFAN_ZULIEFERER)
    endpreis = round(einkauf * ENDPREIS_AUFSCHLAG_DEFAULT) if is_stefan_zl else einkauf
    is_endpreis_vorschlag = is_stefan_zl

    # NK-Quote
    nk_quote = NK_QUOTEN.get(stadt, NK_QUOTEN["default"])
    nk_betrag_100 = round(endpreis * nk_quote)
    nk_betrag_110 = round(endpreis * 1.10 * nk_quote)

    # Gesamtinvestition
    gesamtinvest_100 = endpreis + nk_betrag_100
    gesamtinvest_110 = round(endpreis * 1.10) + nk_betrag_110

    # Annuität (Fallback)
    annuitaet = _annuitaet_cache.get("value") or ANNUITAET_DEFAULT

    # Monatliche Belastung
    monthly_factor = annuitaet / 100 / 12
    belastung_100 = round(gesamtinvest_100 * monthly_factor, 2) if monthly_factor > 0 else 0
    belastung_110 = round(gesamtinvest_110 * monthly_factor, 2) if monthly_factor > 0 else 0

    # Cashflow
    cashflow_100 = round(kaltmiete - belastung_100 - hausgeld_nu, 2)
    cashflow_110 = round(kaltmiete - belastung_110 - hausgeld_nu, 2)

    # AfA
    afa_satz = 0.03 if (baujahr and baujahr >= BAUJAHR_AFA_3PCT_AB) else 0.02
    afa_basis = endpreis * 0.80  # 80% Gebäudeanteil
    afa_jahr = round(afa_basis * afa_satz)
    afa_hinweis = f"AfA {int(afa_satz*100)}% (Baujahr {baujahr})" if baujahr else "AfA 2% (Baujahr unbekannt)"

    # Marge
    marge_intern = round((endpreis - einkauf) / einkauf * 100, 1) if einkauf > 0 else 0

    return {
        "endpreis":                endpreis,
        "is_endpreis_vorschlag_12pct": is_endpreis_vorschlag,
        "obj_nk_betrag":           nk_betrag_100,
        "obj_nk_quote":            nk_quote,
        "obj_gesamtinvest_100":    gesamtinvest_100,
        "obj_gesamtinvest_110":    gesamtinvest_110,
        "obj_belastung_100_pm":    belastung_100,
        "obj_belastung_110_pm":    belastung_110,
        "obj_cashflow_100_pm":     cashflow_100,
        "obj_cashflow_110_pm":     cashflow_110,
        "obj_afa_jahr":            afa_jahr,
        "obj_afa_hinweis":         afa_hinweis,
        "obj_marge_intern":        marge_intern,
        "annuitaet_used":          annuitaet,
    }


def _obj_upload_dropbox(classified: list, zulieferer: str, objekt_id: str, datum: str) -> dict:
    """Lädt Dokumente in Dropbox hoch."""
    if not DROPBOX_REFRESH_TOKEN or not DROPBOX_APP_KEY:
        logger.warning("Dropbox nicht konfiguriert — Upload übersprungen")
        return {"original_link": None, "aufbereitet_link": None, "errors": ["dropbox_not_configured"]}

    try:
        import dropbox
        from dropbox.oauth import DropboxOAuth2FlowNoRedirect

        # Token refreshen
        dbx = dropbox.Dropbox(
            oauth2_refresh_token=DROPBOX_REFRESH_TOKEN,
            app_key=DROPBOX_APP_KEY,
            app_secret=DROPBOX_APP_SECRET,
        )

        base_path = f"/IMEA Objekte/{datum[:4]}/{zulieferer}/{objekt_id}"
        original_path = f"{base_path}/Original"
        aufbereitet_path = f"{base_path}/Aufbereitet"

        uploaded = []
        for att in classified:
            data_b64 = att.get("data_base64", "")
            if not data_b64:
                continue
            try:
                file_data = base64.b64decode(data_b64)
                fname = att.get("filename", "dokument.pdf")
                # Einkaufspreis aus Dateinamen entfernen (Aufbereitet-Ordner)
                dbx.files_upload(file_data, f"{original_path}/{fname}", mode=dropbox.files.WriteMode.overwrite)
                uploaded.append(fname)
            except Exception as e:
                logger.warning(f"Dropbox Upload fehlgeschlagen für {att.get('filename')}: {e}")

        # Shared Links
        try:
            orig_link = dbx.sharing_create_shared_link_with_settings(original_path).url
        except Exception:
            orig_link = None
        try:
            aufb_link = dbx.sharing_create_shared_link_with_settings(aufbereitet_path).url
        except Exception:
            aufb_link = None

        return {"original_link": orig_link, "aufbereitet_link": aufb_link, "uploaded": uploaded, "errors": []}

    except Exception as e:
        logger.error(f"Dropbox Fehler: {e}")
        return {"original_link": None, "aufbereitet_link": None, "errors": [str(e)]}


def _obj_find_or_create_contact(objekt_id: str, objekt: dict, zulieferer: str) -> Optional[str]:
    """Sucht oder erstellt GHL-Kontakt für das Objekt."""
    if not GHL_API_KEY:
        return None

    import httpx as _httpx
    headers = _ghl_headers()
    search_url = f"{GHL_API_BASE}/contacts/search/duplicate"

    # Suche nach ObjektID
    try:
        with _httpx.Client(timeout=10) as client:
            resp = client.get(
                f"{GHL_API_BASE}/contacts/",
                headers=headers,
                params={"locationId": GHL_LOCATION_ID, "query": objekt_id, "limit": 1}
            )
            if resp.status_code == 200:
                contacts = resp.json().get("contacts", [])
                if contacts:
                    return contacts[0]["id"]

            # Neuen Kontakt anlegen
            stadt = objekt.get("stadt", "")
            strasse = objekt.get("strasse", "")
            we_nr = objekt.get("we_nr", "")
            contact_data = {
                "locationId": GHL_LOCATION_ID,
                "firstName": f"Objekt {objekt_id}",
                "lastName": f"{stadt} {strasse} WE{we_nr}".strip(),
                "email": f"objekt.{objekt_id.lower()}@imea-intern.de",
                "tags": ["objekt-intake", f"zulieferer-{zulieferer[:20].lower().replace(' ', '-')}"],
                "customFields": [
                    {"key": "obj_id", "field_value": objekt_id},
                    {"key": "obj_zulieferer", "field_value": zulieferer},
                ]
            }
            create_resp = client.post(f"{GHL_API_BASE}/contacts/", headers=headers, json=contact_data)
            if create_resp.status_code in (200, 201):
                return create_resp.json().get("contact", {}).get("id")
    except Exception as e:
        logger.error(f"GHL Kontakt-Anlage fehlgeschlagen: {e}")
    return None


def _obj_update_contact_fields(contact_id: str, fields: dict):
    """Setzt Custom Fields am GHL-Kontakt."""
    if not GHL_API_KEY or not contact_id:
        return
    custom_fields = [{"key": k, "field_value": str(v)} for k, v in fields.items() if v]
    try:
        with httpx.Client(timeout=10) as client:
            client.put(
                f"{GHL_API_BASE}/contacts/{contact_id}",
                headers=_ghl_headers(),
                json={"customFields": custom_fields}
            )
    except Exception as e:
        logger.warning(f"GHL Custom Fields Update fehlgeschlagen: {e}")


def _obj_create_opportunity(contact_id: str, objekt_id: str, objekt: dict, zulieferer: str, endpreis: float) -> Optional[str]:
    """Legt Opportunity in Pipeline 'Objektfreigabe' an."""
    if not GHL_API_KEY or not contact_id:
        return None
    try:
        with httpx.Client(timeout=10) as client:
            resp = client.post(
                f"{GHL_API_BASE}/opportunities/",
                headers=_ghl_headers(),
                json={
                    "pipelineId": "LHgRuYogVAJF8hIgChLs",
                    "locationId": GHL_LOCATION_ID,
                    "name": f"{objekt_id} — {objekt.get('stadt','')} {objekt.get('strasse','')}",
                    "pipelineStageId": STAGE_EINGANG,
                    "status": "open",
                    "monetaryValue": endpreis,
                    "contactId": contact_id,
                    "assignedTo": "",
                    "customFields": [
                        {"key": "obj_zulieferer", "field_value": zulieferer},
                        {"key": "obj_id", "field_value": objekt_id},
                    ]
                }
            )
            if resp.status_code in (200, 201):
                return resp.json().get("opportunity", {}).get("id")
    except Exception as e:
        logger.error(f"GHL Opportunity-Anlage fehlgeschlagen: {e}")
    return None


def _obj_create_task(contact_id: str, title: str, body: str, assignee: str = "stefan", due_days: int = 1):
    """Erstellt CRM-Task."""
    if not GHL_API_KEY or not contact_id:
        return None
    due_date = datetime.utcnow()
    try:
        with httpx.Client(timeout=10) as client:
            resp = client.post(
                f"{GHL_API_BASE}/contacts/{contact_id}/tasks",
                headers=_ghl_headers(),
                json={
                    "title": title,
                    "body": body,
                    "dueDate": due_date.isoformat() + "Z",
                    "completed": False,
                    "assignedTo": assignee,
                }
            )
            if resp.status_code in (200, 201):
                return resp.json().get("task", {}).get("id")
    except Exception as e:
        logger.warning(f"GHL Task-Anlage fehlgeschlagen: {e}")
    return None


def _obj_add_note(contact_id: str, body: str):
    """Fügt Notiz am GHL-Kontakt hinzu."""
    if not GHL_API_KEY or not contact_id:
        return
    try:
        with httpx.Client(timeout=10) as client:
            client.post(
                f"{GHL_API_BASE}/contacts/{contact_id}/notes",
                headers=_ghl_headers(),
                json={"body": body, "userId": ""}
            )
    except Exception as e:
        logger.warning(f"GHL Notiz fehlgeschlagen: {e}")


def _obj_send_wa_push(objekt: dict):
    """Sendet WA-Push an Stefan via 360dialog."""
    if not DIALOG360_API_KEY or not STEFAN_PHONE:
        return
    msg = (
        f"🏠 Neues Objekt eingegangen!\n"
        f"📍 {objekt.get('stadt','')} {objekt.get('strasse','')} WE{objekt.get('we_nr','')}\n"
        f"💶 EK: {objekt.get('kaufpreis_einkauf','')} €\n"
        f"📐 {objekt.get('flaeche_qm','')} m² | Baujahr {objekt.get('baujahr','')}\n"
        f"➡️ Bitte in GHL prüfen und freigeben."
    )
    try:
        with httpx.Client(timeout=10) as client:
            client.post(
                "https://waba.360dialog.io/v1/messages",
                headers={"D360-API-KEY": DIALOG360_API_KEY, "Content-Type": "application/json"},
                json={
                    "to": STEFAN_PHONE,
                    "type": "text",
                    "text": {"body": msg}
                }
            )
    except Exception as e:
        logger.warning(f"WA-Push fehlgeschlagen: {e}")


def _obj_get_pp_vorschlag(objekt: dict, zulieferer: str) -> list:
    """Gibt Top-3 PerformancePartner-Vorschläge zurück."""
    # Vereinfacht: Gibt Platzhalter zurück (echte Logik braucht PP-Datenbank)
    return [
        {"name": "PP-Vorschlag 1", "score": 85, "grund": "Stadtmatch"},
        {"name": "PP-Vorschlag 2", "score": 72, "grund": "Preisklasse"},
        {"name": "PP-Vorschlag 3", "score": 65, "grund": "Spezialisierung"},
    ]


def _obj_determine_tags(objekt: dict, zulieferer: str, classified: list, fehlend: list, confidence: float, is_12pct: bool) -> list:
    """Bestimmt Tags für den GHL-Kontakt."""
    tags = ["objekt-intake"]
    baujahr = objekt.get("baujahr")
    if baujahr and baujahr < BAUJAHR_RND_SCHWELLE:
        tags.append("rnd-prüfung")
    if baujahr and baujahr >= 2020:
        tags.append("neubau")
    if fehlend:
        tags.append("unvollständig")
    else:
        tags.append("vollständig")
    if confidence < 0.6:
        tags.append("low-confidence")
    if is_12pct:
        tags.append("endpreis-12pct-vorschlag")
    return tags


def _obj_build_notiz(objekt: dict, berechnung: dict, fehlend: list, confidence: float, warnings: list, dropbox: dict) -> str:
    lines = [
        "=== IMEA Objekt-Intake ===",
        f"Datum: {datetime.utcnow().strftime('%d.%m.%Y %H:%M')} UTC",
        f"Confidence: {confidence:.0%}",
        "",
        "--- Extraktion ---",
        f"Stadt: {objekt.get('stadt')} | PLZ: {objekt.get('plz')}",
        f"Fläche: {objekt.get('flaeche_qm')} m² | Zimmer: {objekt.get('zimmer')}",
        f"Baujahr: {objekt.get('baujahr')} | Vermietet: {objekt.get('vermietet')}",
        f"Kaltmiete: {objekt.get('kaltmiete_aktuell')} €",
        f"Kaufpreis Einkauf: {objekt.get('kaufpreis_einkauf')} € (INTERN)",
        "",
        "--- Erstberechnung ---",
        f"Endpreis: {berechnung.get('endpreis')} €",
        f"Marge: {berechnung.get('obj_marge_intern')} %",
        f"NK: {berechnung.get('obj_nk_betrag')} €",
        f"Belastung 100%: {berechnung.get('obj_belastung_100_pm')} €/Mo",
        f"Cashflow 100%: {berechnung.get('obj_cashflow_100_pm')} €/Mo",
        f"AfA/Jahr: {berechnung.get('obj_afa_jahr')} €",
        "",
        "--- Vollständigkeit ---",
        f"Fehlend: {', '.join(fehlend) if fehlend else 'Vollständig ✓'}",
        "",
        "--- Dropbox ---",
        f"Original: {dropbox.get('original_link') or 'N/A'}",
        f"Aufbereitet: {dropbox.get('aufbereitet_link') or 'N/A'}",
    ]
    if warnings:
        lines += ["", "--- Warnungen ---"] + [f"⚠ {w}" for w in warnings]
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
#  /webhook/objekt-intake  (NEU v4.0)
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/webhook/objekt-intake", tags=["Objekte"])
async def objekt_intake(request: Request, background_tasks: BackgroundTasks):
    """
    IMEA Objekt-Intake v4.0
    Payload (von Make.com):
    {
      "mail_id": "...",
      "from_email": "...",
      "from_name": "...",
      "subject": "...",
      "body_text": "...",
      "body_html": "...",
      "mail_date": "2026-05-01T10:00:00Z",
      "x_forwarded_for": "",
      "attachments": [
        {"filename": "...", "mime_type": "application/pdf", "data_base64": "..."}
      ]
    }
    """
    # ── Auth ──────────────────────────────────────────────────────────────────
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer ") or auth[7:] != WEBHOOK_TOKEN_OBJ:
        raise HTTPException(status_code=401, detail="Unauthorized")

    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    mail_id     = payload.get("mail_id", "")
    from_email  = payload.get("from_email", "").lower().strip()
    from_name   = payload.get("from_name", "")
    subject     = payload.get("subject", "")
    body_text   = payload.get("body_text", "")
    body_html   = payload.get("body_html", "")
    mail_date   = payload.get("mail_date", datetime.utcnow().isoformat())
    x_fwd       = payload.get("x_forwarded_for", "")
    attachments = payload.get("attachments", [])

    # ── Idempotenz ────────────────────────────────────────────────────────────
    hash_key = f"{mail_id}_{'_'.join(sorted(a.get('filename','') for a in attachments))}"
    mail_hash = hashlib.sha256(hash_key.encode()).hexdigest()[:32]
    if mail_hash in _processed_hashes:
        return JSONResponse({"status": "duplicate", "message": "Bereits verarbeitet"})

    # ── Filter: Newsletter ────────────────────────────────────────────────────
    if _obj_check_newsletter(subject, body_text):
        return JSONResponse({"status": "filtered", "reason": "newsletter_or_marketing"})

    # ── Filter: Verkauft ──────────────────────────────────────────────────────
    if _obj_check_sold(subject, body_text):
        return JSONResponse({"status": "filtered", "reason": "sold_or_unavailable"})

    # ── Whitelist-Check ───────────────────────────────────────────────────────
    wl = _obj_whitelist_check(from_email, from_name, body_text)
    wl_status = wl["status"]
    zulieferer_name = wl.get("zulieferer_name", from_name or from_email)

    # ── Ist es eine Objekt-Mail? ──────────────────────────────────────────────
    is_objekt, obj_conf, _ = _obj_is_objekt_mail(from_email, subject, body_text)
    if not is_objekt and wl_status != "whitelisted":
        return JSONResponse({"status": "skipped", "reason": "not_objekt_zulieferung"})

    # ── Unbekannter Zulieferer → Pending ──────────────────────────────────────
    if wl_status == "pending_confirmation":
        logger.info(f"Unbekannter Zulieferer: {from_email} — Pending-Task erstellt")
        return JSONResponse({
            "status": "pending_confirmation",
            "zulieferer_name": zulieferer_name,
            "from_email": from_email,
            "message": "Unbekannter Zulieferer — Stefan-Freigabe erforderlich",
        })

    # ── Anhangs-Klassifikation ────────────────────────────────────────────────
    classified = _obj_classify_attachments(attachments)

    # ── Claude Extraktion (Background) ───────────────────────────────────────
    extraction = _obj_extract_with_claude(classified, body_text)
    objekt = extraction.get("objekt", {})
    confidence = extraction.get("confidence", 0.0)
    warnings = extraction.get("extraction_warnings", [])

    # ── ObjektID ──────────────────────────────────────────────────────────────
    objekt_id = _obj_generate_id(
        objekt.get("stadt") or "",
        objekt.get("strasse") or "",
        objekt.get("we_nr") or "",
    )

    # ── Pflicht-Profil ────────────────────────────────────────────────────────
    profil, vorhandene_tags, fehlende_dokumente = _obj_determine_profil(
        classified,
        bool(objekt.get("vermietet")),
        objekt.get("baujahr"),
    )

    # ── Erstberechnung ────────────────────────────────────────────────────────
    berechnung = _obj_calculate(objekt, zulieferer_name)

    # ── Dropbox-Upload ────────────────────────────────────────────────────────
    dropbox_result = _obj_upload_dropbox(classified, zulieferer_name, objekt_id, mail_date[:10])

    # ── CRM-Anlage ────────────────────────────────────────────────────────────
    contact_id = _obj_find_or_create_contact(objekt_id, objekt, zulieferer_name)

    if contact_id:
        # Custom Fields
        fields = {
            "obj_id":                    objekt_id,
            "obj_zulieferer":            zulieferer_name,
            "obj_status":                "freigabe-pending",
            "obj_profil":                profil,
            "obj_plz":                   objekt.get("plz") or "",
            "obj_strasse":               f"{objekt.get('strasse','')} {objekt.get('hausnummer','')}".strip(),
            "obj_we":                    objekt.get("we_nr") or "",
            "obj_baujahr":               str(objekt.get("baujahr") or ""),
            "obj_vermietet":             "true" if objekt.get("vermietet") else "false",
            "obj_kaltmiete":             str(objekt.get("kaltmiete_aktuell") or ""),
            "obj_kp_endpreis":           str(berechnung.get("endpreis") or ""),
            "obj_marge_intern":          str(berechnung.get("obj_marge_intern") or ""),
            "obj_hausgeld":              str(objekt.get("hausgeld_gesamt") or ""),
            "obj_nk_betrag":             str(berechnung.get("obj_nk_betrag") or ""),
            "obj_gesamtinvest_100":      str(berechnung.get("obj_gesamtinvest_100") or ""),
            "obj_gesamtinvest_110":      str(berechnung.get("obj_gesamtinvest_110") or ""),
            "obj_belastung_100_pm":      str(berechnung.get("obj_belastung_100_pm") or ""),
            "obj_belastung_110_pm":      str(berechnung.get("obj_belastung_110_pm") or ""),
            "obj_cashflow_100_pm":       str(berechnung.get("obj_cashflow_100_pm") or ""),
            "obj_cashflow_110_pm":       str(berechnung.get("obj_cashflow_110_pm") or ""),
            "obj_afa_jahr":              str(berechnung.get("obj_afa_jahr") or ""),
            "obj_energie_klasse":        objekt.get("energie_effizienzklasse") or "",
            "obj_heizung":               objekt.get("heizungsart") or "",
            "obj_pflichtdokumente_komplett": "false" if fehlende_dokumente else "true",
            "obj_dropbox_original":      dropbox_result.get("original_link") or "",
            "obj_confidence":            str(confidence),
            "obj_extraction_warnings":   "; ".join(warnings),
            "obj_eingang_datum":         mail_date[:10],
            "obj_name":                  f"{objekt.get('stadt','')} {objekt.get('strasse','')} WE{objekt.get('we_nr','')}".strip(),
            "obj_stadt":                 objekt.get("stadt") or "",
            "obj_flaeche":               str(objekt.get("flaeche_qm") or ""),
            "obj_kp_einkauf":            str(objekt.get("kaufpreis_einkauf") or ""),
        }
        _obj_update_contact_fields(contact_id, fields)

        # PP-Vorschlag
        pp_vorschlag = _obj_get_pp_vorschlag(objekt, zulieferer_name)
        pp_text = "\n".join(f"{i+1}. {pp['name']} (Score: {pp['score']})" for i, pp in enumerate(pp_vorschlag))
        _obj_update_contact_fields(contact_id, {"obj_pp_vorschlag": pp_text})

        # Tags
        tags = _obj_determine_tags(objekt, zulieferer_name, classified, fehlende_dokumente, confidence, berechnung.get("is_endpreis_vorschlag_12pct", False))
        try:
            with httpx.Client(timeout=10) as client:
                client.put(f"{GHL_API_BASE}/contacts/{contact_id}", headers=_ghl_headers(), json={"tags": tags})
        except Exception:
            pass

        # Notiz
        notiz = _obj_build_notiz(objekt, berechnung, fehlende_dokumente, confidence, warnings, dropbox_result)
        _obj_add_note(contact_id, notiz)

        # Opportunity
        opportunity_id = _obj_create_opportunity(contact_id, objekt_id, objekt, zulieferer_name, berechnung.get("endpreis", 0))

        # Tasks
        if not fehlende_dokumente and confidence >= 0.70:
            task_body = (
                f"Objekt {objekt_id} ist vollständig und bereit zur Freigabe.\n"
                f"Endpreis: {berechnung.get('endpreis')} € | Cashflow: {berechnung.get('obj_cashflow_100_pm')} €/Mo\n"
                f"PP-Vorschlag:\n{pp_text}\n"
                f"Dropbox: {dropbox_result.get('original_link') or 'N/A'}"
            )
            task_id = _obj_create_task(contact_id, f"🏠 Objekt freigeben: {objekt_id}", task_body, "stefan")
            _obj_send_wa_push(objekt)
        else:
            fehlend_str = ", ".join(fehlende_dokumente) if fehlende_dokumente else "Confidence zu niedrig"
            task_id = _obj_create_task(
                contact_id,
                f"📋 Objekt unvollständig: {objekt_id}",
                f"Fehlende Dokumente: {fehlend_str}\nBitte bei Zulieferer {zulieferer_name} nachfordern.",
                "joanna"
            )
    else:
        opportunity_id = None
        task_id = None
        logger.warning(f"CRM-Kontakt konnte nicht angelegt werden für {objekt_id}")

    # ── Idempotenz markieren ──────────────────────────────────────────────────
    _processed_hashes[mail_hash] = objekt_id

    logger.info(f"Objekt-Intake abgeschlossen: {objekt_id} | Confidence: {confidence:.2f} | Fehlend: {fehlende_dokumente}")

    return JSONResponse({
        "status": "success",
        "objekt_id": objekt_id,
        "contact_id": contact_id,
        "opportunity_id": opportunity_id,
        "task_id": task_id,
        "confidence": confidence,
        "profil": profil,
        "fehlende_dokumente": fehlende_dokumente,
        "endpreis": berechnung.get("endpreis"),
        "cashflow_100_pm": berechnung.get("obj_cashflow_100_pm"),
        "dropbox_original": dropbox_result.get("original_link"),
        "warnings": warnings,
    })


# ─────────────────────────────────────────────────────────────────────────────
#  /admin/whitelist/bootstrap  — Whitelist mit bekannten Zulieferern befüllen
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/admin/whitelist/bootstrap", tags=["Admin"], include_in_schema=False)
async def whitelist_bootstrap(request: Request):
    """Befüllt die In-Memory-Whitelist mit bekannten IMEA-Zulieferern."""
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer ") or auth[7:] != WEBHOOK_TOKEN_OBJ:
        raise HTTPException(status_code=401, detail="Unauthorized")

    known_zulieferer = [
        {"email": "info@finest-invest.de",      "name": "Finest Invest",      "domain": "finest-invest.de"},
        {"email": "kontakt@finest-invest.de",   "name": "Finest Invest",      "domain": "finest-invest.de"},
        {"email": "falk.jaeger@finest-invest.de","name": "Falk Jäger",         "domain": "finest-invest.de"},
        {"email": "info@poller-immobilien.de",  "name": "Poller Immobilien",  "domain": "poller-immobilien.de"},
        {"email": "s.poller@poller-immobilien.de","name": "Sebastian Poller",  "domain": "poller-immobilien.de"},
        {"email": "info@convista.de",           "name": "Convista",           "domain": "convista.de"},
        {"email": "objekte@convista.de",        "name": "Convista",           "domain": "convista.de"},
        {"email": "info@vonovia.de",            "name": "Vonovia",            "domain": "vonovia.de"},
        {"email": "verkauf@vonovia.de",         "name": "Vonovia",            "domain": "vonovia.de"},
        {"email": "info@deutsche-wohnen.de",    "name": "Deutsche Wohnen",    "domain": "deutsche-wohnen.de"},
        {"email": "info@tag-immobilien.de",     "name": "TAG Immobilien",     "domain": "tag-immobilien.de"},
        {"email": "info@adler-group.de",        "name": "Adler Group",        "domain": "adler-group.de"},
        {"email": "immobilien@immonet.de",      "name": "Immonet",            "domain": "immonet.de"},
        {"email": "info@engel-voelkers.de",     "name": "Engel & Völkers",    "domain": "engel-voelkers.de"},
        {"email": "info@remax.de",              "name": "RE/MAX",             "domain": "remax.de"},
        {"email": "sh@imea-finanz.de",          "name": "Stefan Happatz (Forward)", "domain": "imea-finanz.de"},
    ]

    count = 0
    for z in known_zulieferer:
        _whitelist[z["email"]] = z
        count += 1

    logger.info(f"Whitelist Bootstrap: {count} Zulieferer geladen")
    return {"status": "ok", "loaded": count, "total": len(_whitelist)}


@app.get("/admin/whitelist", tags=["Admin"], include_in_schema=False)
async def whitelist_list(request: Request):
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer ") or auth[7:] != WEBHOOK_TOKEN_OBJ:
        raise HTTPException(status_code=401, detail="Unauthorized")
    return {"count": len(_whitelist), "entries": list(_whitelist.keys())}


# ─────────────────────────────────────────────────────────────────────────────
#  Legacy Endpoints (unverändert)
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/webhook/zins-update", response_model=ZinsUpdateResponse, tags=["Zins"])
async def zins_update(payload: ZinsUpdateRequest):
    value = payload.annuitaet
    now_iso = datetime.now(timezone.utc).isoformat()
    success = await ghl_upsert_custom_value(ANNUITAET_FIELD_KEY, value)
    if not success:
        raise HTTPException(status_code=502, detail="GHL-Aktualisierung fehlgeschlagen.")
    _annuitaet_cache["value"] = value
    _annuitaet_cache["updated_at"] = now_iso
    return ZinsUpdateResponse(success=True, value=value, updated_at=now_iso)


@app.get("/zins/current", response_model=ZinsCurrentResponse, tags=["Zins"])
async def zins_current():
    value = await ghl_get_custom_value(ANNUITAET_FIELD_KEY)
    return ZinsCurrentResponse(value=value, updated_at=_annuitaet_cache.get("updated_at"))


@app.get("/zins", response_class=HTMLResponse, tags=["Zins"], include_in_schema=False)
async def zins_page():
    html_path = pathlib.Path(__file__).parent / "static" / "zins.html"
    if html_path.exists():
        return HTMLResponse(content=html_path.read_text(encoding="utf-8"))
    return HTMLResponse(content="<h1>Zins-Update Seite nicht gefunden</h1>", status_code=404)


@app.post("/webhook/lead-qualify", tags=["Lead"])
async def lead_qualify(payload: LeadQualifyRequest):
    annuitaet = await get_current_annuitaet()
    volumen_100 = None
    volumen_110 = None
    max_rate = None
    if payload.frei_verfuegbar and payload.frei_verfuegbar > 0:
        max_rate = payload.frei_verfuegbar * 0.80
        monthly_factor = annuitaet / 100 / 12
        if monthly_factor > 0:
            volumen_100 = round(max_rate / monthly_factor)
            volumen_110 = round(volumen_100 / 1.10)
    score = 5
    disqualified = False
    disqualification_reason = None
    if payload.schufa_ok is False:
        disqualified = True
        disqualification_reason = "Schufa-Negativmerkmal"
        score = 1
    elif payload.netto_einkommen and payload.netto_einkommen < 2800:
        disqualified = True
        disqualification_reason = f"Nettoeinkommen unter Minimum: {payload.netto_einkommen:.0f} €"
        score = 2
    else:
        if payload.netto_einkommen:
            score += 2 if payload.netto_einkommen >= 5000 else (1 if payload.netto_einkommen >= 3500 else 0)
        if payload.eigenkapital:
            score += 2 if payload.eigenkapital >= 30000 else (1 if payload.eigenkapital >= 15000 else 0)
        if payload.beschaeftigung in ("angestellt", "beamter"):
            score += 1
        if payload.alter and 25 <= payload.alter <= 45:
            score += 1
        if payload.unterhalt:
            score -= 1
        if payload.bestehende_kredite and payload.bestehende_kredite > 500:
            score -= 1
        score = max(1, min(10, score))
    result = {
        "score": score, "disqualified": disqualified,
        "disqualification_reason": disqualification_reason,
        "annuitaet_used": annuitaet, "max_rate": round(max_rate, 2) if max_rate else None,
        "estimated_volume_100pct": volumen_100, "estimated_volume_110pct_nk": volumen_110,
        "calculated_at": datetime.now(timezone.utc).isoformat(),
    }
    if payload.contact_id and GHL_API_KEY:
        try:
            async with httpx.AsyncClient(timeout=8.0) as client:
                await client.put(
                    f"{GHL_API_BASE}/contacts/{payload.contact_id}",
                    headers=_ghl_headers(),
                    json={"customFields": [
                        {"key": "lead_score", "field_value": str(score)},
                        {"key": "estimated_volume", "field_value": str(volumen_100 or "")},
                        {"key": "annuitaet_used", "field_value": str(annuitaet)},
                    ]}
                )
        except Exception as e:
            logger.warning(f"GHL contact score update failed: {e}")
    return result


@app.post("/webhook/gate-check", tags=["Pipeline"])
async def gate_check(request: Request):
    body = await request.json()
    logger.info(f"gate-check called: {json.dumps(body)[:200]}")
    return {"status": "ok", "message": "Gate check received"}


@app.post("/webhook/vermittlerprotokoll", tags=["Dokumente"])
async def vermittlerprotokoll(request: Request):
    body = await request.json()
    logger.info(f"vermittlerprotokoll called: {json.dumps(body)[:200]}")
    return {"status": "ok", "message": "Vermittlerprotokoll received"}


@app.post("/webhook/zulieferer", tags=["Objekte"])
async def zulieferer(request: Request):
    body = await request.json()
    logger.info(f"zulieferer called: {json.dumps(body)[:200]}")
    return {"status": "ok", "message": "Zulieferer data received"}


@app.api_route("/api/ghl-proxy/{path:path}", methods=["GET", "POST", "PUT", "DELETE"])
async def ghl_proxy(path: str, request: Request):
    target_url = f"{GHL_API_BASE}/{path}"
    body = await request.body()
    headers = dict(request.headers)
    headers.pop("host", None)
    if GHL_API_KEY:
        headers["Authorization"] = f"Bearer {GHL_API_KEY}"
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.request(
                method=request.method, url=target_url,
                headers=headers, content=body,
                params=dict(request.query_params),
            )
            return JSONResponse(content=resp.json() if resp.content else {}, status_code=resp.status_code)
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
        "version": "4.0.0",
        "ghl_location": GHL_LOCATION_ID,
        "whitelist_count": len(_whitelist),
        "processed_count": len(_processed_hashes),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@app.get("/", include_in_schema=False)
async def root():
    return {"service": "IMEA Webhook-Server", "version": "4.0.0", "status": "running"}
