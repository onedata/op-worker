ONECLIENT=../oneclient
ONEPROVIDER=../oneprovider
MAIN=..
ROOT=../..

REST_PORT=$(shell sed -n '/.*name:[ ]*rest_port.*/ {N;N; s/.*value:[ ]*\([0-9]*\).*/\1/p}' < ${ROOT}/config/default.yml)
REST_PORT_PLACEHOLDER=\$${rest_port}

all: main oneclient oneprovider

main:
	@printf "\r[%3d%%]\tMain" 0
	@python main.py $(MAIN)
	@printf "\r[%3d%%]\tMain\n" 100

oneclient:
	@printf "\r[%3d%%]\toneclient" 0
	@make -C $(ROOT)/oneclient docs
	@printf "\r[%3d%%]\toneclient" 33
	@mkdir -p $(ONECLIENT)
	@pandoc --from=markdown --to=rst --output=$(ONECLIENT)/about.rst $(ROOT)/oneclient/README.md
	@printf "\r[%3d%%]\toneclient" 67
	@python oneclient.py $(ROOT) $(ONECLIENT)
	@printf "\r[%3d%%]\toneclient\n" 100

oneprovider:
	@sed -i -e "s/${REST_PORT_PLACEHOLDER}/${REST_PORT}/" $(ONEPROVIDER)/REST_API/*
	@printf "\r[%3d%%]\toneprovider" 0
	@mkdir -p $(ONEPROVIDER)/c_modules/gpv_nif
	@mkdir -p $(ONEPROVIDER)/c_modules/helpers_nif
	@pandoc --from=markdown --to=rst --output=$(ONEPROVIDER)/about.rst $(ROOT)/README.md
	@pandoc --from=markdown --to=rst --output=$(ONEPROVIDER)/development.rst $(ROOT)/README-DEVELOPER.md
	@printf "\r[%3d%%]\toneprovider" 50
	@python oneprovider.py $(ROOT) $(ONEPROVIDER)
	@printf "\r[%3d%%]\toneprovider\n" 100

clean:
	rm -f $(MAIN)/*.rst

	rm -rf $(ONECLIENT)
	rm -rf $(ROOT)/oneclient/doc

	rm -f $(ONEPROVIDER)/*.rst
	rm -rf $(ONEPROVIDER)/erlang_modules
	rm -rf $(ONEPROVIDER)/c_modules
