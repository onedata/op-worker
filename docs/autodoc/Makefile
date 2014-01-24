VEILCLIENT=../veilClient
VEILCLUSTER=../veilCluster
MAIN=..
ROOT=../..

all: main veilclient veilcluster

main:
	@printf "\rMain\t\t(%3d%%)" 0
	@python main.py $(MAIN)
	@printf "\rMain\t\t(%3d%%)\n" 100

veilclient:
	@printf "\rVeilClient\t(%3d%%)" 0
	@pandoc --from=markdown --to=rst --output=$(VEILCLIENT)/about.rst $(MAIN)/data/veilclient/README.md
	@printf "\rVeilClient\t(%3d%%)" 50
	@python veilclient.py $(MAIN) $(VEILCLIENT)
	@printf "\rVeilClient\t(%3d%%)\n" 100

veilcluster:
	@printf "\rVeilCluster\t(%3d%%)" 0
	@pandoc --from=markdown --to=rst --output=$(VEILCLUSTER)/about.rst $(ROOT)/README.md
	@printf "\rVeilCluster\t(%3d%%)" 33
	@pandoc --from=markdown --to=rst --output=$(VEILCLUSTER)/development.rst $(ROOT)/README-DEVELOPER.md
	@printf "\rVeilCluster\t(%3d%%)" 67
	@python veilcluster.py $(ROOT) $(VEILCLUSTER)
	@printf "\rVeilCluster\t(%3d%%)\n" 100

clean:
	rm -f $(VEILCLIENT)/classes/*.rst
	rm -f $(VEILCLIENT)/structs/*.rst
	rm -f $(VEILCLIENT)/namespaces/*.rst
	rm -f $(VEILCLIENT)/files/*.rst
	rm -f $(VEILCLIENT)/*.rst

	rm -f $(VEILCLUSTER)/*.rst
	rm -f $(VEILCLUSTER)/REST_APT/index.rst
	rm -rf $(VEILCLUSTER)/erlang_modules
	rm -rf $(VEILCLUSTER)/c_modules

	rm -f $(MAIN)/*.rst

