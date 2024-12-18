all: resource-usage-api

SOURCES = $(wildcard *.go) $(wildcard */*.go)

resource-usage-api: ${SOURCES}
	go build --buildvcs=false .

clean:
	rm -rf resource-usage-api

.PHONY: all clean
