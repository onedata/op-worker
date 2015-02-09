.PHONY: all base builder worker bigcouch

all: base builder worker bigcouch

base: base/Dockerfile
	docker build -t onedata/base base/

builder: base builder/Dockerfile
	docker build -t onedata/builder builder/

worker: base worker/Dockerfile
	docker build -t onedata/worker worker/

bigcouch: bigcouch/Dockerfile
	docker build -t onedata/bigcouch bigcouch/
