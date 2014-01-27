VEILCLIENT=../veilClient
VEILCLUSTER=../veilCluster
MAIN=..
ROOT=../..

## TODO: uncomment commented lines and delete temporary copies when pandoc is installed on Bamboo machines

all: main veilclient veilcluster

main:
	@printf "\rMain\t\t(%3d%%)" 0
	@python main.py $(MAIN)
	@printf "\rMain\t\t(%3d%%)\n" 100

veilclient:
	@printf "\rVeilClient\t(%3d%%)" 0
	@mkdir -p $(VEILCLIENT)
	@#pandoc --from=markdown --to=rst --output=$(VEILCLIENT)/about.rst $(MAIN)/data/veilclient/README.md
	@cp $(MAIN)/data/veilclient/README.txt $(VEILCLIENT)/about.rst
	@printf "\rVeilClient\t(%3d%%)" 50
	@python veilclient.py $(MAIN) $(VEILCLIENT)
	@printf "\rVeilClient\t(%3d%%)\n" 100

veilcluster:
	@printf "\rVeilCluster\t(%3d%%)" 0
	@mkdir -p $(VEILCLUSTER)
	@mkdir -p $(VEILCLUSTER)/c_modules/gpv_nif
	@mkdir -p $(VEILCLUSTER)/c_modules/veilhelpers_nif
	@#pandoc --from=markdown --to=rst --output=$(VEILCLUSTER)/about.rst $(ROOT)/README.md
	@cp $(MAIN)/data/veilcluster/README.txt $(VEILCLUSTER)/about.rst
	@printf "\rVeilCluster\t(%3d%%)" 33
	@#pandoc --from=markdown --to=rst --output=$(VEILCLUSTER)/development.rst $(ROOT)/README-DEVELOPER.md
	@cp $(MAIN)/data/veilcluster/README-DEVELOPER.txt $(VEILCLUSTER)/development.rst
	@cp $(MAIN)/data/veilcluster/gpv_nif/README.txt $(VEILCLUSTER)/c_modules/gpv_nif/README.rst
	@cp $(MAIN)/data/veilcluster/veilhelpers_nif/README.txt $(VEILCLUSTER)/c_modules/veilhelpers_nif/README.rst
	@printf "\rVeilCluster\t(%3d%%)" 67
	@python veilcluster.py $(ROOT) $(VEILCLUSTER)
	@printf "\rVeilCluster\t(%3d%%)\n" 100

clean:
	rm -rf $(VEILCLIENT)

	rm -f $(VEILCLUSTER)/*.rst
	rm -f $(VEILCLUSTER)/REST_API/index.rst
	rm -rf $(VEILCLUSTER)/erlang_modules
	rm -rf $(VEILCLUSTER)/c_modules

	rm -f $(MAIN)/*.rst
