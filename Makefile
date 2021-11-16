all: build

build: build_release

build_with_tests: test build_release

build_release:
	cargo build --release

run_debug:
	target/debug/kprf --config=config_example.yaml

clean:
	cargo clean

test:
	cargo test --workspace && cargo test --package ratelimit --lib tests
