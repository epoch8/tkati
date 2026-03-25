IMAGE ?= my-registry/pipelines
TAG   ?= latest

REPO_ROOT := $(shell git -C $(dir $(abspath $(lastword $(MAKEFILE_LIST)))) rev-parse --show-toplevel)

.PHONY: build push

build:
	docker build -f $(REPO_ROOT)/examples/clicks/Dockerfile \
		-t $(IMAGE):$(TAG) \
		$(REPO_ROOT)

push: build
	docker push $(IMAGE):$(TAG)
