# ii-deed-fetch

A scheduled GitHub Actions job that downloads Luxembourg Business Register (RESA)
publication PDFs — public documents the register serves to any visitor — and posts
them to a private server for archiving and extraction.

**This repository holds no data.** No PDF, no extracted text, no artifact is ever
stored here; the job logs print publication numbers, HTTP statuses and timings only.
The server address and token are repository secrets set by the owner
(`PIPELINE_SERVER_URL`, `PIPELINE_INGEST_TOKEN`); there is no pull-request trigger,
so no third-party code can run with them.

How it behaves towards the register: one job at a time (a concurrency group), at most
10 publications per job, a server-enforced ceiling of 300 attempts per day, every
download made through the register's own page control after its captcha proof-of-work
has been solved by the browser itself. Nothing is disguised, no captcha service, no
proxy.

Files:
- `.github/workflows/deed-fetch.yml` — the schedule (every 30 min) and the job.
- `.github/workflows/deed-probe.yml` — a manual probe that only reports timings.
- `deed_fetch.py` — the fetch script (claim → journal page → download → report).

The canonical copy of these files lives in the owner's private repository
(`helper/ii-deed-fetch/`); this repository is a mirror pushed by
`scripts/sync_deed_fetch_repo.py`. Source commit: see the last line of this file.

Note: GitHub disables scheduled workflows on public repositories after 60 days without
a commit; the monthly sync keeps the schedule alive.

Source commit: 1b62f589 (investment-intelligence, helper/ii-deed-fetch/)
