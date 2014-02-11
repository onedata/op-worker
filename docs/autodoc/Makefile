VEILCLIENT=../veilClient
VEILCLUSTER=../veilCluster
MAIN=..
ROOT=../..

all: main veilclient veilcluster

main:
	@printf "\r[%3d%%]\tMain" 0
	@python main.py $(MAIN)
	@printf "\r[%3d%%]\tMain\n" 100

veilclient:
	@printf "\r[%3d%%]\tVeilClient" 0
	@doxygen $(ROOT)/veilclient/Doxyfile
	@printf "\r[%3d%%]\tVeilClient" 33
	@mkdir -p $(VEILCLIENT)
	@pandoc --from=markdown --to=rst --output=$(VEILCLIENT)/about.rst $(ROOT)/veilclient/README.md
	@printf "\r[%3d%%]\tVeilClient" 67
	@python veilclient.py $(ROOT) $(VEILCLIENT)
	@printf "\r[%3d%%]\tVeilClient\n" 100

veilcluster:
	@printf "\r[%3d%%]\tVeilCluster" 0
	@mkdir -p $(VEILCLUSTER)/c_modules/gpv_nif
	@mkdir -p $(VEILCLUSTER)/c_modules/veilhelpers_nif
	@pandoc --from=markdown --to=rst --output=$(VEILCLUSTER)/about.rst $(ROOT)/README.md
	@pandoc --from=markdown --to=rst --output=$(VEILCLUSTER)/development.rst $(ROOT)/README-DEVELOPER.md
	@printf "\r[%3d%%]\tVeilCluster" 50
	@python veilcluster.py $(ROOT) $(VEILCLUSTER)
	@printf "\r[%3d%%]\tVeilCluster\n" 100

clean:
	rm -f $(MAIN)/*.rst

	rm -rf $(VEILCLIENT)

	rm -f $(VEILCLUSTER)/*.rst
	rm -rf $(VEILCLUSTER)/erlang_modules
	rm -rf $(VEILCLUSTER)/c_modules
