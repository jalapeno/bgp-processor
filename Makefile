REGISTRY_NAME?=docker.io/iejalapeno
IMAGE_VERSION?=latest

.PHONY: all bgp-processor container push clean test

ifdef V
TESTARGS = -v -args -alsologtostderr -v 5
else
TESTARGS =
endif

all: bgp-processor

bgp-processor:
	mkdir -p bin
	$(MAKE) -C ./cmd compile-bgp-processor

bgp-processor-container: bgp-processor
	docker build -t $(REGISTRY_NAME)/bgp-processor:$(IMAGE_VERSION) -f ./build/Dockerfile.bgp-processor .

push: bgp-processor-container
	docker push $(REGISTRY_NAME)/bgp-processor:$(IMAGE_VERSION)

clean:
	rm -rf bin

test:
	GO111MODULE=on go test `go list ./... | grep -v 'vendor'` $(TESTARGS)
	GO111MODULE=on go vet `go list ./... | grep -v vendor`
