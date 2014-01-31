bletchley_framegen: install

install: build check
	go install

build: deps
	go build

deps:
	go get

clean:
	rm -f framegen

check:
	go test
