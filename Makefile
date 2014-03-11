default: build-all

build: build-all

install: install-all

clean: clean-all

build-all:
	$(MAKE) $(MFLAGS) -C src

install-all:
	$(MAKE) $(MFLAGS) -C telemetry install
	$(MAKE) $(MFLAGS) -C src install

clean-all:
	$(MAKE) $(MFLAGS) -C src clean
