.PHONY: fmt fmt-check qualification-test consumer-manifest-check clippy test check

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

qualification-test:
	PYTHONPATH=scripts python3 -m unittest scripts/test_scope_closure_qualification.py
	PYTHONPATH=scripts python3 -m unittest scripts/test_supabase_consumer_manifest.py

consumer-manifest-check:
	python3 scripts/check_supabase_consumer_manifest.py

clippy:
	cargo clippy --workspace --all-targets --all-features -- -D warnings

test:
	cargo test --workspace --all-features

check: fmt-check qualification-test consumer-manifest-check clippy test
