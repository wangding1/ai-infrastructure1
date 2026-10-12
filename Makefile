DS ?= all

.PHONY: docker-recreate build dev debug course course-push test-benchmark

docker-recreate:
	docker compose up -d --force-recreate

build:
	uv run langgraph build -t langgraph-python-agent:$(TAG)

dev:
	uv run langgraph dev --host 0.0.0.0 --port 80

debug:
	uv run langgraph dev --host 0.0.0.0 --port 80 --debug-port 5678 --wait-for-client
course:
	uv run python scripts/course.py

test-benchmark:
	BENCHMARK_DATASET="$(DS)" uv run pytest -q -s --tb=no tests/benchmarks/test_benchmark.py

course-push:
	@[ -n "$(SHA)" ] || (echo "请提供提交 hash: make course-push SHA=<hash>" && exit 1)
	uv run python scripts/course-push.py $(SHA)
