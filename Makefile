IMAGE_NAME ?= localhost/redis-logger
IMAGE_TAG ?= latest
IMAGE ?= $(IMAGE_NAME):$(IMAGE_TAG)

CONTAINER_ENGINE := $(shell command -v podman 2> /dev/null | echo docker)
CONTAINER_BUILD_EXTRA_FLAGS =

.PHONE: images push push-only magic

images:
	$(CONTAINER_ENGINE) build -t $(IMAGE)  $(CONTAINER_BUILD_EXTRA_FLAGS) -f Containerfile .

push-only:
	$(CONTAINER_ENGINE) push --quiet $(IMAGE)

push: images push-only

magic: images
	$(CONTAINER_ENGINE) run -it --rm --name redis-logger ${IMAGE}
