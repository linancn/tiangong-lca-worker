.PHONY: fmt fmt-check qualification-test clippy test check

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all -- --check

qualification-test:
	PYTHONPATH=scripts python3 -m unittest scripts/test_scope_closure_qualification.py scripts/test_worker_control_plane_api_cutover.py scripts/test_worker_control_plane_db_harness.py
	python3 scripts/check_worker_control_plane_api_cutover.py

clippy:
	cargo clippy --workspace --all-targets --all-features -- -D warnings

test:
	cargo test --workspace --all-features

check: fmt-check qualification-test clippy test
