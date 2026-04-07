"""
Test suite covering every known issue. Run before pushing.
Usage: python test_app.py [--url http://localhost:8001]
"""

import sys
import json
import time
import argparse
import requests

BASE = "http://localhost:8001"

RESET  = "\033[0m"
GREEN  = "\033[92m"
RED    = "\033[91m"
YELLOW = "\033[93m"
BOLD   = "\033[1m"

passed = 0
failed = 0

def ask(question, history=None, company="Amadeo"):
    payload = {"company_id": company, "question": question, "history": history or []}
    r = requests.post(f"{BASE}/query", json=payload, timeout=60)
    r.raise_for_status()
    return r.json()

def check(name, answer, *rules):
    global passed, failed
    errors = []
    for rule_fn, description in rules:
        if not rule_fn(answer):
            errors.append(description)
    if errors:
        failed += 1
        print(f"  {RED}FAIL{RESET} {name}")
        for e in errors:
            print(f"       ✗ {e}")
        print(f"       Answer: {answer[:200]}")
    else:
        passed += 1
        print(f"  {GREEN}PASS{RESET} {name}")

def contains(*words):
    return lambda a: all(w.lower() in a.lower() for w in words), f"Expected answer to contain: {words}"

def not_contains(*words):
    return lambda a: not any(w.lower() in a.lower() for w in words), f"Answer should NOT contain: {words}"

def not_empty():
    return lambda a: len(a) > 5 and "could not answer" not in a.lower(), "Answer should not be empty or failure"

def has_numbers():
    return lambda a: any(c.isdigit() for c in a), "Answer should contain at least one number"


print(f"\n{BOLD}=== mai test suite ==={RESET}\n")

# ── 1. Health check ──────────────────────────────────────────────────────────
print(f"{BOLD}[Health]{RESET}")
try:
    r = requests.get(f"{BASE}/", timeout=5)
    assert r.json()["status"] == "healthy"
    passed += 1
    print(f"  {GREEN}PASS{RESET} Server is healthy")
except Exception as e:
    failed += 1
    print(f"  {RED}FAIL{RESET} Server health check — {e}")
    print(f"\n{RED}Server not reachable at {BASE}. Start with: uvicorn app.main:app --port 8001{RESET}\n")
    sys.exit(1)

# ── 2. Basic queries ─────────────────────────────────────────────────────────
print(f"\n{BOLD}[Basic queries]{RESET}")

d = ask("What is total revenue?")
check("Total revenue (English)",
    d["answer"],
    not_empty(),
    has_numbers(),
    contains("245"))

d = ask("What is total expenses?")
check("Total expenses (English)",
    d["answer"],
    not_empty(),
    has_numbers(),
    contains("127"))

# ── 3. Georgian basic ────────────────────────────────────────────────────────
print(f"\n{BOLD}[Georgian language]{RESET}")

d = ask("რა იყო ჯამური შემოსავალი?")
check("Total revenue (Georgian)",
    d["answer"],
    not_empty(),
    has_numbers(),
    contains("245"))

d = ask("რა იყო ჯამური ხარჯები?")
check("Total expenses (Georgian)",
    d["answer"],
    not_empty(),
    has_numbers(),
    contains("127"))

# ── 4. Semantic question — no false clarification ────────────────────────────
print(f"\n{BOLD}[No false clarification on semantic questions]{RESET}")

d = ask("რა იყო მთავარი ხარჯი")
check("Main expense Georgian — no false clarification",
    d["answer"],
    not_empty(),
    not_contains("I couldn't find 'მთავარი ხარჯი'", "couldn't find 'main expense'"),
    has_numbers())

d = ask("what was the biggest expense category")
check("Biggest expense (English) — no false clarification",
    d["answer"],
    not_empty(),
    not_contains("I couldn't find 'biggest'", "I couldn't find 'expense category'"),
    has_numbers())

# ── 5. Typo / synonym handling ───────────────────────────────────────────────
print(f"\n{BOLD}[Typo and synonym handling]{RESET}")

d = ask("how much was my xpnses")
check("Typo: xpnses → expense",
    d["answer"],
    not_empty(),
    has_numbers(),
    contains("127"),
    contains("xpnses", "expense"))

d = ask("what was my total incm")
check("Typo: incm → income/revenue",
    d["answer"],
    not_empty(),
    has_numbers())

# ── 6. Multi-part question ───────────────────────────────────────────────────
print(f"\n{BOLD}[Multi-part questions]{RESET}")

d = ask("what was expenses and what was the source of main expense")
check("Expenses + main source (English)",
    d["answer"],
    not_empty(),
    has_numbers(),
    contains("127"))

d = ask("რა იყო ხარჯები და რა იყო მთავარი ხარჯის წყარო")
check("Expenses + main source (Georgian)",
    d["answer"],
    not_empty(),
    has_numbers())

# ── 7. Breakdown / aggregation ───────────────────────────────────────────────
print(f"\n{BOLD}[Aggregation and breakdown]{RESET}")

d = ask("how much was my revenues, break it down by subcategory")
check("Revenue breakdown by subcategory",
    d["answer"],
    not_empty(),
    has_numbers(),
    contains("245"))

# ── 8. Weekly grouping ───────────────────────────────────────────────────────
print(f"\n{BOLD}[Time-based grouping]{RESET}")

d = ask("show total expenses by week")
check("Expenses by week (English) — must have multiple weeks",
    d["answer"],
    not_empty(),
    has_numbers(),
    (lambda a: d["rows"] > 1, f"Expected multiple weeks, got {d['rows']} rows"),
    (lambda a: "2025" in a, "Expected dates in answer"))

d = ask("დამითვალე ჯამური ხარჯები კვირის ჭრილში")
check("Expenses by week (Georgian)",
    d["answer"],
    not_empty(),
    has_numbers(),
    (lambda a: d["rows"] > 1, f"Expected multiple weeks, got {d['rows']} rows"))

# ── 9. Conversation history — HISTORY path ───────────────────────────────────
print(f"\n{BOLD}[Conversation history]{RESET}")

history = [
    {"role": "user",      "content": "how much was my expenses?"},
    {"role": "assistant", "content": "Your expenses were $127,000."},
    {"role": "user",      "content": "and revenue?"},
    {"role": "assistant", "content": "Your revenue was $245,000."},
]

d = ask("compare them", history=history)
check("Compare from history (no DB call)",
    d["answer"],
    not_empty(),
    has_numbers(),
    (lambda a: d["sql"] == "", f"Should use HISTORY path (no SQL), got: {d['sql']}"))

# ── 10. Deduction from history ───────────────────────────────────────────────
history2 = [
    {"role": "user",      "content": "break down my revenues"},
    {"role": "assistant", "content": "Product Sales $115,000, Service Fees $48,000, Subscriptions $82,000. Total $245,000."},
]

d = ask("calculate it without subscriptions", history=history2)
check("Deduct subcategory from history",
    d["answer"],
    not_empty(),
    has_numbers(),
    contains("163"))

# ── 11. Company isolation ────────────────────────────────────────────────────
print(f"\n{BOLD}[Company isolation]{RESET}")

companies = requests.get(f"{BASE}/companies").json()["companies"]
if len(companies) > 1:
    other = [c for c in companies if c != "Amadeo"][0]
    d1 = ask("what is total revenue", company="Amadeo")
    d2 = ask("what is total revenue", company=other)
    check(f"Amadeo vs {other} return different revenue",
        "ok",
        (lambda _: d1["answer"] != d2["answer"], f"Both companies returned same answer: {d1['answer']}"))
else:
    print(f"  {YELLOW}SKIP{RESET} Only one company in DB — isolation test skipped")

# ── Summary ──────────────────────────────────────────────────────────────────
total = passed + failed
print(f"\n{BOLD}{'═'*40}{RESET}")
if failed == 0:
    print(f"{GREEN}{BOLD}All {total} tests passed. Safe to push.{RESET}")
else:
    print(f"{RED}{BOLD}{failed}/{total} tests FAILED. Fix before pushing.{RESET}")
print()
sys.exit(0 if failed == 0 else 1)
