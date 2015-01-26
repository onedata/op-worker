all: builder worker master

builder: builder/Dockerfile
	docker build -t onedata/builder builder/

worker: worker/Dockerfile
	docker build -t onedata/worker worker/

master: master/Dockerfile
	docker build -t onedata/master master/
