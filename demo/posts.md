# Posts

Three drafts, three audiences. Paste as-is.

---

## LinkedIn

Companies revise their financial results after publishing them. A revenue figure filed in
February can be restated in August, and both numbers were officially filed with the SEC.

Most data pipelines handle this by overwriting the old number. That works until someone
asks what you believed back in March, when you made the decision — and the answer no
longer exists anywhere.

I spent the last stretch building a lakehouse over SEC EDGAR filings that treats a
restatement as a new assertion rather than a correction. The old figure stays, marked
superseded. You can ask "what is true now" and "what did we believe then" and get a
correct answer to both. In the last verified run it preserved 329 restatement events that
an overwriting pipeline would have reported as zero.

The part I did not expect to be the hard part: getting four separate repositories to agree
on what a column means. My first attempt gave each one its own copy of the schema
definitions. They drifted for weeks without anyone noticing, because the check meant to
catch drift only compared part of the definition. The fix was to delete every copy and
publish one installable package that the others depend on at a pinned version. They cannot
drift now because they cannot disagree.

It runs on a free tier, on a deliberately small slice of the data, and two of the six
repositories are still just design documents. I would rather say that than imply otherwise.

If you have built on EDGAR — how did you handle restatements? I have not found a
satisfying answer to point-in-time queries that does not cost storage.

#dataengineering #dataquality #lakehouse #python

---

## Reddit — r/dataengineering

**What I got wrong building a medallion lakehouse over SEC filings: the schema copies**

Writing this up because the mistake took weeks to surface and the lesson generalises past
my project.

**Setup.** Six repos: shared contracts, Terraform infra, an ingest container, the
medallion pipeline, and two consumers that are still design-only. Data is SEC EDGAR
filings. Small on purpose — eight companies, free-tier Databricks.

**The actual problem worth solving.** Companies restate figures. Overwriting the old value
destroys your ability to answer "what did we believe on date X", which is the question that
matters when someone audits a decision. So facts are versioned as assertions: a restatement
inserts a new row, flags the old one superseded, and nothing is lost. Last run kept 329
restatement events that an UPSERT-style pipeline reports as zero.

**What I got wrong.** The ingest repo and the pipeline repo each kept their own copy of the
schema definitions. Obvious in hindsight. What made it bad was the guard rail: I had a CI
check comparing the two, so I believed I was covered. It only compared column *names*, not
types, not the file-path helpers, not the envelope shape. The copies drifted in every
dimension it did not look at, and the check stayed green the whole time.

Every round of debugging surfaced a new drift class — envelope fields, then all 13 tables,
then parameter names, then spec types. I kept fixing instances instead of the cause.

**The fix.** Delete both copies. One repo owns the definitions and publishes a wheel; the
others declare a pinned dependency on it. They cannot drift because there is nothing to
drift from. The comparison check now derives its expectations from the published package
rather than restating them.

**Other things that bit me, briefly:**

- ECR with immutable tags means `:latest` belongs permanently to the first image that
  claimed it. I "fixed" a pull failure by pushing `:latest`, which worked exactly once and
  has silently no-opped every build since. It is now the stalest image in the registry.
- Both schedules were set to 06:00. Enabling them would have started the pipeline while
  the collector was still writing, and the run would have processed a partial day and
  reported success. Caught it reading the cron expressions side by side, not from a test.
- A stale image digest left in a gitignored local tfvars produced a completely ordinary
  looking Terraform plan that would have pinned an image that no longer exists. Terraform
  does not check that a digest resolves.

**What I would do differently:** make the contract a package on day one, and never write a
consistency check that restates what it is checking. Derive it from the source of truth or
it will pass while lying to you.

Happy to go into the assertion-versioning model if useful — that part I am fairly happy
with.

---

## Discord

Built a lakehouse over SEC EDGAR filings — the interesting bit is it never overwrites a
restated figure, so you can still ask "what did we believe in March" after a company
revises its numbers. Last run preserved 329 restatements an overwriting pipeline would
report as zero.

Biggest lesson was unrelated to finance: I had two repos keeping their own copies of the
shared schema, plus a CI check to compare them. The check only compared column names, so
they drifted in every other dimension for weeks while staying green.

Diagram + write-up: <repo link>
