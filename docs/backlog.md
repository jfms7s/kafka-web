# Backlog — deferred review findings

Minor findings from the per-task adversarial reviews and the final whole-project review, triaged **DEFER** (polish, performance, rare edge cases). Everything triaged FIX NOW was fixed before this file was written.

- **Task 1:** password-leak test never reaches an exception-text path; no tests for empty/URL-safe b64; uv.lock/.python-version committed (fine)
- **Task 1:** PEM mixing CERTIFICATE + TRUSTED CERTIFICATE fails whole; "no certificates" message for TRUSTED-only file slightly misleading; RC2 PKCS12 path only verified via monkeypatch
- **Task 2:** update() not rollback-safe on partial failure (PEM/secret replaced before YAML write fails); hand-edited truststore path not confined to config dir (read only, echoed path in reason)
- **Task 2:** chmod 0700 on a non-owned KAFKA_WEB_CONFIG_DIR raises raw PermissionError; extra may set ssl.endpoint.identification.algorithm / enable.ssl.certificate.verification (weakens TLS, spec silent)
- **Task 3:** 500 handler doesn't suppress uvicorn traceback logging (report claim inaccurate, no leak today)
- **Task 3:** foreign Host 400 is plain text not {code,message}; leader re-raise keeps attempt in a ref cycle until GC
- **Task 4:** (1) create/update/test mutation variables (incl. sasl_password, truststore_base64/password) retained in React Query MutationCache for gcTime — set gcTime 0 / reset; (2) ClusterForm ignores has_sasl_password — "leave blank to keep" shown when no secret saved, blank save leaves cluster unusable silently; (3) ClusterForm seeds useState from possibly stale cached cluster → PUT can overwrite newer settings; (4) 422 for an unrendered field shows nothing; (5) FileReader race choosing two files; (6) SPA fallback returns index.html for /missing.js, /favicon.ico, /API/x; (7) start.sh: unknown mode silently dev, `wait` not `wait -n`, kill 0 scope; (8) index.html no Cache-Control, stale "Connection OK", ConfirmDialog focus trap, connect.variables tracking; (9) coverage gaps for edit-mode unusable/protocol switches
- **Task 5:** non-default highlight counts STATIC_BROKER_CONFIG as non-default; humanize 1048575 → "1024.00 KiB" and Long.MAX ms sentinels render as huge durations; encoding tests vacuous (names needing no encoding); Tabs aria-controls to missing ids; leader -1 shown raw, partition error ignored; RF computed twice
- **Task 6:** JSON integers > 2^53 shown rounded in pretty view; MessagesTab errors inline instead of toasts and 422 field not attached to input; fetch not aborted on unmount
- **Task 6:** _can_serialise doubles JSON serialization CPU for big snapshots
- **Task 7:** stream frames not byte-capped (~291 MB frame costs ~1 s on loop; raw queue can hold ~1.4 GB); stale _ALL_BROKERS_DOWN from setup phase could end a healthy stream on first poll (unreproduced)
- **Task 8:** memory scales with CSV cell count (10k columns × 1k rows → ~893 MB peak); consider capping columns/headers per row; extra can override allow.auto.create.topics
- **Task 9:** group ids containing "/" (or exactly "."/"..") unreachable (404 not_found); timestamp 10**30 → 502 instead of 422 (add le=2**63-1); successful reset reported as failed if the trailing list_offsets fails; coordinator retry can take ~2× timeout per call; one broker error fails whole group list; group id whitespace inconsistent (API accepts "   ", UI trims)
- **Task 10:** stream probe (2 s list_topics) ignores stop and equals JOIN_TIMEOUT_S 2 s → "did not stop in time" during outage; dev broker.pem (unencrypted dev key) 0644; gen-certs.sh no trap cleanup on failure; probe timeout not asserted in tests
- **Final:** in-flight connection test can set "Connection OK" after an edit; DecodedValue files use a different quote style
- **Final:** deleting a group briefly flashes "Could not load the group" before navigating away (remove the detail query on delete success).
- **Final:** on read-only clusters the "Create group" button is hidden rather than disabled with an explanation (spec §8).
