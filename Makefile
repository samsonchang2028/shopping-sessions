IMAGE ?= shopping-sessions:latest
CPUS  ?= 8
MEM   ?= 24g
# The scorer's platform. PLATFORM=linux/arm64 builds and runs natively on an Apple-silicon Mac, for fast local loops.
PLATFORM ?= linux/amd64
DEV   ?= dev
# In the monorepo the fixtures live two levels up; the published starter has its own copy.
FIXTURES ?= $(if $(wildcard fixtures),fixtures,../../fixtures)
export IMAGE CPUS MEM PLATFORM

.PHONY: build fixtures dev run-dev check-dev

## Build the image for the scorer's platform, linux/amd64 (emulated on Apple silicon)
build:
	docker buildx build --platform $(PLATFORM) --load -t $(IMAGE) .

## Run the image on all 36 edge-case fixtures and diff against the expected tables
fixtures: build
	python3 scripts/run_fixtures.py $(FIXTURES) -- scripts/docker_run.sh

## Make the 1M-event dev set with the published generator (needs `generate` on PATH)
dev:
	generate --seed 1 --events 1000000 --clicks 250000 --output $(DEV)

## Run on the dev set with scorer flags and time it
run-dev: build
	rm -rf out && time scripts/docker_run.sh $(DEV) out

## Compare your dev output with the published dev answer (dev-answer.zip, unzipped): make check-dev ANSWER=dev-answer
ANSWER ?= dev-answer
check-dev:
	python3 scripts/compare.py out $(ANSWER)

## ---- local harness (no Docker): unit tests, fixtures, fuzz, dev check
PY ?= .venv/bin/python
.PHONY: unit fixtures-local fixtures-ref fuzz check-local test-all
unit:
	$(PY) -m unittest discover -s tests
fixtures-local:
	CHECK=1 $(PY) scripts/run_fixtures.py $(FIXTURES) -- $(PY) pipeline/pipeline.py
fixtures-ref:
	$(PY) scripts/run_fixtures.py $(FIXTURES) -- $(PY) ref/reference.py
fuzz:
	$(PY) scripts/fuzz.py
check-local:
	rm -rf out && CHECK=1 DATA_DIR=$(DEV) OUTPUT_DIR=out $(PY) pipeline/pipeline.py && $(PY) scripts/compare.py out $(ANSWER)
test-all: unit fixtures-local fixtures-ref check-local fuzz
