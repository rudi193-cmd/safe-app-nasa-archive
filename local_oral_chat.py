"""
local_oral_chat.py -- Local proxy for the NASA oral-chat edge function.
Mirrors the Supabase edge function API exactly.

Startup:
  1. Registers 'nasa-oral-chat' with Willow's agent_registry
  2. Gets auto-assigned port from the 84xx range
  3. Writes PUBLIC_ORAL_CHAT_URL to site/.env.local
  4. Starts serving

Run: python local_oral_chat.py
Then: cd site && npm run dev
"""
import json
import sys
import io
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

# Force UTF-8 stdout/stderr on Windows
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding="utf-8", errors="replace")

# Willow core
WILLOW_CORE = r"C:\Users\Sean\Documents\GitHub\Willow\core"
sys.path.insert(0, WILLOW_CORE)

import llm_router
import agent_registry

llm_router.load_keys_from_json()

USERNAME = "Sweet-Pea-Rudi19"
AGENT_NAME = "nasa-oral-chat"
SITE_ENV_LOCAL = Path(__file__).parent / "site" / ".env.local"

SYSTEM_PROMPT = """You are the NASA oral historian -- the voice of the North America Scootering Archive.

Your job: help community members share their memories of scooter rallies. You listen, ask follow-up questions, and help people tell their stories in their own words.

Cultural principles you embody:
- "Names Given Not Chosen" -- people go by their club names, not legal names. Always use what they give you.
- "Someone Always Stops" -- rescues on the road are fundamental community stories. Ask about them.
- "Grief Makes Space" -- if someone mentions someone who has passed, receive it gently.
- "Corrections Not Erasure" -- if someone says the date was wrong, or the bike was different, that's valuable. Record it.
- "Recognition Not Instruction" -- you're here to witness, not to teach.

Your approach:
1. Ask about specific moments, not general impressions
2. Follow up on names that come up naturally
3. Ask about bikes -- make, model, what broke, garden art status
4. Ask about rescues -- who saved them, who they saved
5. Ask about how people got their names (especially if it was drunk)
6. Keep it conversational -- this is a bar story, not a deposition

Keep replies short (2-4 sentences). Ask one follow-up question at a time."""


def _register_and_get_port() -> int:
    """Register with Willow agent_registry and get auto-assigned port."""
    agent_registry.register_agent(
        username=USERNAME,
        name=AGENT_NAME,
        display_name="NASA Oral Chat",
        trust_level="WORKER",
        agent_type="service",
        purpose="Local proxy for NASA oral history chat (Groq/fleet LLM)",
    )
    port = agent_registry.assign_port(USERNAME, AGENT_NAME, server_type="oral-chat")
    return port


def _write_env_local(port: int):
    """Write PUBLIC_ORAL_CHAT_URL to site/.env.local so Astro picks it up."""
    SITE_ENV_LOCAL.write_text(
        f"# Auto-written by local_oral_chat.py on startup\n"
        f"PUBLIC_ORAL_CHAT_URL=http://localhost:{port}\n",
        encoding="utf-8",
    )


def _call_fleet(prompt: str) -> str:
    r = llm_router.ask(prompt, preferred_tier="free")
    if r and r.content:
        return r.content.strip()
    raise RuntimeError("All fleet providers failed")


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        print(f"oral-chat: {fmt % args}", flush=True)

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")

    def do_OPTIONS(self):
        self.send_response(200)
        self._cors()
        self.end_headers()

    def do_POST(self):
        if self.path != "/functions/v1/oral-chat":
            self.send_response(404)
            self.end_headers()
            return

        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length))

        message = body.get("message", "").strip()
        slug = body.get("slug", "").strip()
        history = body.get("history", [])

        if not message or not slug:
            self._json(400, {"error": "message and slug required"})
            return

        system_content = SYSTEM_PROMPT + f'\n\nThe user is sharing memories about the rally: "{slug}"'
        history_text = "\n".join(
            f"{'User' if m['role'] == 'user' else 'Historian'}: {m['content']}"
            for m in history[-10:]
        )
        prompt = f"{system_content}\n\n{history_text}\nUser: {message}\nHistorian:"

        try:
            reply = _call_fleet(prompt)
            self._json(200, {"reply": reply})
        except Exception as e:
            print(f"Fleet error: {type(e).__name__}: {e}", flush=True)
            self._json(503, {"error": "LLM unavailable"})

    def _json(self, status: int, data: dict):
        payload = json.dumps(data).encode()
        self.send_response(status)
        self._cors()
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


if __name__ == "__main__":
    port = _register_and_get_port()
    _write_env_local(port)

    url = f"http://localhost:{port}/functions/v1/oral-chat"
    print(f"NASA oral-chat proxy: {url}", flush=True)
    print(f"site/.env.local updated with PUBLIC_ORAL_CHAT_URL", flush=True)
    print(f"Now run: cd site && npm run dev", flush=True)

    HTTPServer(("localhost", port), Handler).serve_forever()
