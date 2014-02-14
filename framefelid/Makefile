framefelid: install

install: build check
	go install

build: deps
	go build

deps:
	go get

clean:
	rm -f framefelid

check:
	go test
