.PHONY: fmt fmt-check qualification-test clippy test check

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

qualification-test:
	PYTHONPATH=scripts python3 -m unittest scripts/test_scope_closure_qualification.py
	python3 scripts/qualify_snapshot_private_cutover.py --static-only

clippy:
	cargo clippy --workspace --all-targets --all-features -- -D warnings

test:
	cargo test --workspace --all-features

check: fmt-check qualification-test clippy test
