bletchley_framegen: install

install: build check
	go install

build: deps
	go build

deps:
	go get

clean:
	rm -f bletchley_framegen

check:
	go test
