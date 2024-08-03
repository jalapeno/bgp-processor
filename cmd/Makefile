compile-bgp-processor:
	CGO_ENABLED=0 GOOS=linux GO111MODULE=on go build -a -ldflags '-extldflags "-static"' -o ../bin/bgp-processor ./bgp-processor.go
