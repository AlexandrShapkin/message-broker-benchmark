.PHONY: bootstrap test lint smoke analyze clean

bootstrap:
	go mod download
	python -m venv .venv
	.venv/bin/pip install -e "analysis[dev]"

test:
	go test ./...
	.venv/bin/python -m pytest analysis/tests

lint:
	gofmt -w .
	go vet ./...

smoke:
	go run ./cmd/bench smoke --all

analyze:
	.venv/bin/python -m broker_analysis

clean:
	go clean
	rm -rf analysis/output
