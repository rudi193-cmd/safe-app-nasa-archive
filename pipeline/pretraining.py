"""
NASA Archive — SAFE OS Pre-Training Pipeline
============================================
Processes PUBLIC RECORD sources only.

PRIVACY HARD RULE:
  This pipeline writes source_type='public_record' ONLY.
  Private content (oral history sessions, private comms) = HARD STOP.
  Oral history agent handles oral_history_consented separately.

Public sources:
  1. scoot.net rally data (data/rallies/{slug}/meta.json)
  2. Vespa Motorsport Podcast (MP3 → Whisper → entity extraction)
  3. Club/rally web archives (Modern Vespa, club sites, obituaries)

Usage:
  python pipeline/pretraining.py --source scootnet
  python pipeline/pretraining.py --source podcast --path /path/to/ep4.mp3
  python pipeline/pretraining.py --source web --url https://modernvespa.com/...
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any

import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client

# Willow free fleet
sys.path.insert(0, str(Path(__file__).parent.parent.parent / "Willow" / "core"))
import llm_router

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
log = logging.getLogger("pretraining")

# ─── NASA Hook Triggers ────────────────────────────────────────────────────────
# Applied to text chunks. If any trigger matches, extract entities from that chunk.

HOOKS: dict[str, list[str]] = {
    "importance":   ["very well known", "legendary", "iconic", "founder of", "started the"],
    "person":       ["wade parker", "matthew noli", "colin shattuck", "phil lombardo",
                     "missi kroge", "chris hyder", "chopper john", "gabe"],
    "shop":         ["scooters", "motorsport", "cycles", "garage", "shop"],
    "rally":        ["tng", "mile high mayhem", "king tutt putt", "amerivespa",
                     "rally", "run", "scootfest", "scootarama"],
    "media":        ["scoot! magazine", "vespa motorsport podcast", "podcast episode"],
    "venue":        ["tower bar", "calabasas campsite", "the crypt", "congress bar",
                     "abbey tavern", "pub scouts"],
    "incident":     ["died", "shooting", "accident", "passed away", "in memoriam"],
    "manufacturer": ["vespa", "lambretta", "honda", "stella", "bajaj"],
    "geo":          ["san diego scene", "denver scene", "tucson scene", "phoenix scene",
                     "chicago scene", "new england scene", "atlanta scene"],
    "club":         ["pharaohs", "secret servix", "ace", "bottle rockets", "jedi knights",
                     "sqream", "pub scouts", "blue smoke", "jett sett", "hard pack"],
}

EXTRACT_PROMPT = """\
You are an archivist for the North America Scooter Archive (NASA).
Extract structured entities from the following text.

Return ONLY a JSON array. Each item must follow this schema:
{
  "entity_type": "rally" | "club" | "person" | "shop" | "venue" | "event",
  "name": "<canonical name>",
  "year": <integer or null>,
  "city": "<city or null>",
  "state": "<state abbreviation or null>",
  "description": "<1-2 sentence summary or null>",
  "confidence": "high" | "medium" | "low"
}

Rules:
- Only extract entities explicitly named in the text.
- Do not invent names, dates, or locations.
- If a detail is not stated, use null.
- "high" confidence = name + at least one corroborating fact stated.
- "medium" = name present, supporting details vague.
- "low" = inferred from context.

Text:
{text}

Hook that triggered extraction: {hook}

JSON array:"""


class PreTrainingPipeline:
    """
    Processes public-record sources and seeds the oral_* Supabase tables.
    source_type is always 'public_record' — no exceptions.
    """

    DATA_DIR = Path(__file__).parent.parent / "data" / "rallies"

    def __init__(self) -> None:
        url = os.environ["SUPABASE_URL"]
        key = os.environ["SUPABASE_SERVICE_ROLE_KEY"]  # service role needed for inserts
        self.db: Client = create_client(url, key)
        llm_router.load_keys_from_json()

    # ─── Source: scoot.net rally metadata ─────────────────────────────────────

    def process_rally_data(self) -> int:
        """
        Walk data/rallies/{slug}/meta.json and upsert each rally into oral_events.
        Returns number of records upserted.
        """
        count = 0
        for meta_path in sorted(self.DATA_DIR.glob("*/meta.json")):
            with open(meta_path, encoding="utf-8") as f:
                meta = json.load(f)

            # Directory name is the Astro route slug — matches getStaticPaths()
            dir_slug  = meta_path.parent.name
            meta_slug = meta.get("slug") or dir_slug   # for source URL only
            title     = meta.get("title") or dir_slug
            year      = meta.get("year")

            record = {
                "name":        title,
                "event_year":  year,
                "archive_slug": dir_slug,              # must match Astro directory name
                "source_type": "public_record",
                "confidence":  "high",
                "sources": json.dumps([{
                    "type":       "web_archive",
                    "url":        meta.get("url") or f"http://scoot.net/gallery/{meta_slug}/",
                    "timestamp":  None,
                    "confidence": "high",
                }]),
            }

            self._upsert("oral_events", record, conflict_col="archive_slug")
            count += 1
            if count % 100 == 0:
                log.info("Processed %d rallies…", count)

        log.info("Rally data: %d records upserted", count)
        return count

    # ─── Source: Podcast audio ─────────────────────────────────────────────────

    def process_podcast(self, mp3_path: str | Path, episode_url: str = "") -> list[dict]:
        """
        Transcribe a podcast MP3 with Whisper, then extract entities.
        Requires: pip install openai-whisper
        """
        try:
            import whisper  # type: ignore
        except ImportError:
            log.error("openai-whisper not installed. Run: pip install openai-whisper")
            return []

        log.info("Transcribing %s …", mp3_path)
        model = whisper.load_model("base")
        result = model.transcribe(str(mp3_path))
        text: str = result["text"]

        source = {
            "type":       "podcast",
            "url":        episode_url or str(mp3_path),
            "timestamp":  None,
            "confidence": "high",
        }
        entities = self.extract_entities_from_text(text, source)
        self._write_entities(entities)
        return entities

    # ─── Source: Web archive ───────────────────────────────────────────────────

    def process_web_page(self, url: str) -> list[dict]:
        """
        Scrape a public page (Modern Vespa, club site, obituary) and extract entities.
        """
        try:
            resp = requests.get(url, timeout=15, headers={"User-Agent": "NASAArchive/1.0"})
            resp.raise_for_status()
        except requests.RequestException as e:
            log.error("Fetch failed %s: %s", url, e)
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        # Remove nav/footer noise
        for tag in soup(["nav", "footer", "script", "style"]):
            tag.decompose()
        text = soup.get_text(separator=" ", strip=True)

        source = {"type": "web_archive", "url": url, "timestamp": None, "confidence": "medium"}
        entities = self.extract_entities_from_text(text, source)
        self._write_entities(entities)
        return entities

    # ─── Entity extraction via hooks + fleet ──────────────────────────────────

    def extract_entities_from_text(
        self, text: str, source: dict, chunk_size: int = 1500
    ) -> list[dict]:
        """
        Apply all 10 NASA hooks to text chunks.
        For each matching chunk, call the free fleet to extract structured entities.
        Returns list of entity dicts ready for Supabase.
        """
        entities: list[dict] = []
        # Split into overlapping chunks so we don't miss entities at boundaries
        words = text.split()
        step = chunk_size - 100  # 100-word overlap
        for i in range(0, len(words), step):
            chunk = " ".join(words[i : i + chunk_size])
            chunk_lower = chunk.lower()

            triggered_hooks = [
                hook for hook, triggers in HOOKS.items()
                if any(t in chunk_lower for t in triggers)
            ]
            if not triggered_hooks:
                continue

            hook_label = ", ".join(triggered_hooks)
            prompt = EXTRACT_PROMPT.format(text=chunk, hook=hook_label)

            raw = self._call_fleet(prompt)
            if not raw:
                continue

            parsed = self._parse_json_array(raw)
            for item in parsed:
                item["sources"] = [source]
                item["source_type"] = "public_record"
            entities.extend(parsed)

        # Deduplicate by name (case-insensitive)
        seen: set[str] = set()
        unique: list[dict] = []
        for e in entities:
            key = (e.get("entity_type", ""), e.get("name", "").lower())
            if key not in seen:
                seen.add(key)
                unique.append(e)

        return unique

    # ─── Write to Supabase ─────────────────────────────────────────────────────

    def _write_entities(self, entities: list[dict]) -> None:
        """Route each extracted entity to the correct oral_* table."""
        for entity in entities:
            etype = entity.get("entity_type", "")
            if etype == "rally":
                self._upsert("oral_events", {
                    "name":        entity.get("name"),
                    "event_year":  entity.get("year"),
                    "description": entity.get("description"),
                    "source_type": "public_record",
                    "confidence":  entity.get("confidence", "medium"),
                    "sources":     json.dumps(entity.get("sources", [])),
                }, conflict_col="name")
            elif etype == "club":
                self._upsert("oral_clubs", {
                    "name":         entity.get("name"),
                    "city":         entity.get("city"),
                    "state":        entity.get("state"),
                    "notes":        entity.get("description"),
                    "source_type":  "public_record",
                    "confidence":   entity.get("confidence", "medium"),
                    "sources":      json.dumps(entity.get("sources", [])),
                }, conflict_col="name")
            elif etype == "person":
                self._upsert("oral_persons", {
                    "club_name":   entity.get("name"),
                    "home_city":   entity.get("city"),
                    "home_state":  entity.get("state"),
                    "bio":         entity.get("description"),
                    "source_type": "public_record",
                    "confidence":  entity.get("confidence", "medium"),
                    "sources":     json.dumps(entity.get("sources", [])),
                }, conflict_col="club_name")
            elif etype in ("shop", "venue"):
                self._upsert("oral_locations", {
                    "name":          entity.get("name"),
                    "city":          entity.get("city"),
                    "state":         entity.get("state"),
                    "location_type": etype,
                    "notes":         entity.get("description"),
                    "source_type":   "public_record",
                    "confidence":    entity.get("confidence", "medium"),
                    "sources":       json.dumps(entity.get("sources", [])),
                }, conflict_col="name")
            else:
                log.debug("Skipping entity_type=%s name=%s", etype, entity.get("name"))

    def _upsert(self, table: str, record: dict, conflict_col: str = "name") -> None:
        """Idempotent insert. Existing records with same conflict_col are ignored."""
        # Remove None values — Supabase rejects null for non-nullable cols
        clean = {k: v for k, v in record.items() if v is not None}
        try:
            (
                self.db.table(table)
                .upsert(clean, on_conflict=conflict_col, ignore_duplicates=True)
                .execute()
            )
        except Exception as e:
            log.warning("Upsert failed %s %s: %s", table, clean.get("name", "?"), e)

    # ─── Fleet helpers ─────────────────────────────────────────────────────────

    def _call_fleet(self, prompt: str, retries: int = 3) -> str | None:
        """Call the Willow free fleet with exponential backoff."""
        for attempt in range(retries):
            try:
                resp = llm_router.ask(prompt, preferred_tier="free")
                if resp:
                    return resp.content
            except Exception as e:
                log.warning("Fleet attempt %d failed: %s", attempt + 1, e)
            time.sleep(2 ** attempt)
        return None

    @staticmethod
    def _parse_json_array(text: str) -> list[dict]:
        """Extract the first JSON array from LLM output. Returns [] on parse failure."""
        start = text.find("[")
        end   = text.rfind("]") + 1
        if start == -1 or end == 0:
            return []
        try:
            data = json.loads(text[start:end])
            return [item for item in data if isinstance(item, dict) and item.get("name")]
        except json.JSONDecodeError:
            log.debug("JSON parse failed: %s", text[:200])
            return []


# ─── CLI ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="NASA Archive pre-training pipeline")
    parser.add_argument("--source", choices=["scootnet", "podcast", "web"], required=True)
    parser.add_argument("--path",  help="Path to MP3 file (podcast mode)")
    parser.add_argument("--url",   help="URL to scrape (web mode)")
    args = parser.parse_args()

    p = PreTrainingPipeline()

    if args.source == "scootnet":
        n = p.process_rally_data()
        print(f"Done: {n} rally records upserted")

    elif args.source == "podcast":
        if not args.path:
            parser.error("--path required for podcast mode")
        entities = p.process_podcast(args.path)
        print(f"Done: {len(entities)} entities extracted")

    elif args.source == "web":
        if not args.url:
            parser.error("--url required for web mode")
        entities = p.process_web_page(args.url)
        print(f"Done: {len(entities)} entities extracted")
