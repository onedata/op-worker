
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
	cd .. && sg sbuild -c "sbuild -sd $(DISTRIBUTION) -j$$(nproc) {{package_name}}_$(PKG_VERSION)-$(PKG_BUILD)~$(DISTRIBUTION).dsc"
	cd .. && mv *$(PKG_VERSION).tar.gz packages
	cd .. && mv *$(PKG_VERSION)-$(PKG_BUILD)~$(DISTRIBUTION).dsc packages
	cd .. && mv *$(PKG_VERSION)-$(PKG_BUILD)~$(DISTRIBUTION).diff.gz packages
	cd .. && mv *$(PKG_VERSION)-$(PKG_BUILD)~$(DISTRIBUTION)_source.changes packages
	cd .. && mv *$(PKG_VERSION)-$(PKG_BUILD)~$(DISTRIBUTION)_*.deb packages
	cd ../packages && \
		for debfile in *.deb; do \
			sha256sum $${debfile} > $${debfile}.sha \
		; done
