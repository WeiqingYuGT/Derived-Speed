BASE_NAME = derived_speed

BASE_CONF = $(BASE_NAME).conf

XPKG = xpkg

# Release version
VERSION := $(shell gawk '/^Release/ {print $$2; exit}' ../CHANGELOG.md)

# Doc & Install scripts
CHANGES = ../CHANGELOG.md

#----------
# Targets
#----------
default: rp

# Release package
rp: clean rp1
	$(XPKG) $(BASE_CONF)

rp1:
	$(XPKG) $(BASE_CONF)

clean:
	rm -f *.rpm
	rm -rf releases
