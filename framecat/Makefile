bletchley_framecat: install

install: build check
	go install

build: deps
	go build

deps:
	go get

clean:
	rm -f bletchley_framecat

check:
	go test
