.PHONY: all base builder worker bigcouch

all: base builder worker bigcouch

base:
	docker build -t onedata/base:v3 -f base .

builder: base builder
	docker build -t onedata/builder:v4 -f builder .

worker: base worker
	docker build -t onedata/worker:v3 -f worker .

bigcouch: bigcouch
	docker build -t onedata/bigcouch:v1 -f bigcouch .

