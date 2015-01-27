.PHONY: all builder worker

all: builder worker

builder: builder/Dockerfile
	docker build -t onedata/builder builder/

worker: worker/Dockerfile
	docker build -t onedata/worker worker/
