
PWD    = $(shell pwd)


ifeq ($(OSNAME),RedHat)
DISTRO = $(shell head -1 /etc/redhat-release| awk \
           '{if ($$0 ~ /CentOS release 5/) { print ".el5."} else { print "." }} ')
INITSCRIPT=rhel.init.script
endif
ifeq ($(OSNAME),SLES)
DISTRO = .SLES$(shell grep VERSION /etc/SuSE-release | cut -d ' ' -f 3).
INITSCRIPT=suse.init.script
endif

default: init.script
	mkdir -p packages

	mock --root $(DISTRIBUTION) --buildsrpm --spec package.spec --sources $(PWD) --resultdir $(PWD)/packages
	mock --root $(DISTRIBUTION) --rebuild packages/*.src.rpm --resultdir $(PWD)/packages
	cd packages && \
		for rpmfile in *.rpm; do \
			sha256sum $${rpmfile} > $${rpmfile}.sha \
		; done

init.script:
	cp $(INITSCRIPT) init.script
