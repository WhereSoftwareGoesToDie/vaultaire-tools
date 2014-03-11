default: build-all

build: build-all

install: install-all

clean: clean-all

build-all:
	make -C src

install-all:
	make -C telemetry install
	make -C src install

clean-all:
	make -C src clean
