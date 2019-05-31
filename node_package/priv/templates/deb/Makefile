
default:
	ln -sf $(PKG_ID).tar.gz ../{{package_name}}_$(PKG_VERSION).orig.tar.gz
	export DEBFULLNAME="{{vendor_contact_name}}"; \
	export DEBEMAIL="{{vendor_contact_email}}"; \
	dch  --create --package {{package_name}} -v "$(PKG_VERSION)-$(PKG_BUILD)" \
             "Build from $(PKG_VERSION)";\
	debuild --prepend-path=$(ERLANG_BIN) \
		-e REVISION="$(PKG_VERSION)" \
		-e RELEASE="$(PKG_BUILD)" \
		-e REBAR="$(REBAR)" \
		{{debuild_extra_options}} \
		-uc -us -S -d
	mkdir -p ../packages
	cd .. && sg sbuild -c "sbuild -sd $(DISTRIBUTION) -j$$(nproc) *$(PKG_VERSION)-$(PKG_BUILD).dsc"
	cd .. && mv *$(PKG_VERSION).tar.gz packages
	cd .. && mv *$(PKG_VERSION)-$(PKG_BUILD).dsc packages
	cd .. && mv *$(PKG_VERSION)-$(PKG_BUILD).diff.gz packages
	cd .. && mv *$(PKG_VERSION)-$(PKG_BUILD)_amd64.changes packages
	cd .. && mv *$(PKG_VERSION)-$(PKG_BUILD)_*.deb packages
	cd ../packages && \
		for debfile in *.deb; do \
			sha256sum $${debfile} > $${debfile}.sha \
		; done
