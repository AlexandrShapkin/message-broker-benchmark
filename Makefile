export GOCACHE := $(CURDIR)/.cache/go-build
PYTHON ?= python3
.PHONY: bootstrap build test lint validate smoke recovery analyze plots clean
bootstrap:
	go mod download
	$(PYTHON) -m venv .venv
	.venv/bin/pip install -r analysis/requirements.lock
	.venv/bin/pip install --no-deps -e 'analysis[dev]'
build:
	go build -o bin/bench ./cmd/bench
test:
	go test ./...
	.venv/bin/python -m pytest analysis/tests
lint:
	test -z "$$(gofmt -l cmd internal drivers schemas tests)"
	go vet ./...
	.venv/bin/ruff check analysis
	.venv/bin/mypy analysis/src
validate: build
	@for f in scenarios/smoke/*.yaml scenarios/research/*.yaml; do [ ! -f "$$f" ] || bin/bench validate "$$f" || exit 1; done
smoke: build
	bin/bench smoke --all
recovery:
	BENCH_RECOVERY=1 BENCH_TEST_RESULTS="$(CURDIR)/results/contracts" go test ./tests/integration -run TestBrokerProfilesAndRecovery -count=1 -v -timeout 30m
analyze:
	.venv/bin/python -m broker_analysis --results results --output analysis/output
plots: analyze
clean:
	go clean
