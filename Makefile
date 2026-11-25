.ONESHELL:

# OS / python / SQLITE_VERSION
OS_TARGET=rocky-8
PYTHON_VERSION=3.10.20  # updated from 3.9.25 (drop EOL Python 3.9)
SQLITE_YEAR=2025
SQLITE_VERSION=3490100

# conda
CONDA_ACTIVATE = source $$(conda info --base)/etc/profile.d/conda.sh ; conda activate ; conda activate ;

# docker run command
#DOCKER_RUN:=docker -D run
DOCKER_RUN:=docker run

# docker run opts
#DOCKER_RUN_OPTS:=--rm -it
DOCKER_RUN_OPTS:=-it

# docker build commands, with the options included (unlike the approach to docker run above)
#DOCKER_BUILD:=docker build --pull=false -f 
#DOCKER_BUILD:=docker -D build --pull=false -f
DOCKER_BUILD:=docker build -f 
#DOCKER_BUILD:=docker -D build -f
#DOCKER_BUILD:=docker build --no-cache -f
#DOCKER_BUILD:=docker -D build --no-cache -f

# minimal-metrics epmt project url. contains epmt, epmt-dash, and papiex srcs
MM_SRC_URL_BASE=https://gitlab.com/minimal-metrics-llc/epmt
# NOAA-GFDL github url. contains epmt, epmt-dash, and papiex srcs
NOAAGFDL_SRC_URL_BASE=https://github.com/noaa-gfdl

# papiex details
PAPIEX_VERSION?=2.3.16
PAPIEX_SRC?=papiex
PAPIEX_SRC_BRANCH=main
PAPIEX_SRC_TARBALL=$(PAPIEX_SRC_BRANCH).tar.gz
PAPIEX_SRC_URL=$(NOAAGFDL_SRC_URL_BASE)/papiex/archive/$(PAPIEX_SRC_TARBALL)
PAPIEX_RELEASE=papiex-epmt-$(PAPIEX_VERSION)-$(OS_TARGET).tgz
CONFIG_PAPIEX_PAPI?=y
CONFIG_PAPIEX_DEBUG?=y

# epmt details
EPMT_VERSION=$(shell python3 -c "exec(open('src/epmt/epmtlib.py').read().split('def ')[0]); from packaging.version import Version; print(Version(__version__))")
EPMT_PYTHON_FULL_RELEASE=epmt-$(EPMT_VERSION).tar.gz

# <root>/src/epmt/ui submodule details
EPMT_DASH_SRC_BRANCH=main
EPMT_DASH_SRC_TARBALL=epmt-dash-$(EPMT_DASH_SRC_BRANCH).tar.gz
EPMT_DASH_SRC_URL=$(NOAAGFDL_SRC_URL_BASE)/epmt-dash/archive/$(EPMT_DASH_SRC_BRANCH).tar.gz
EPMT_DASH_SRC=src/epmt/ui

# shell
SHELL=/bin/bash
PWD=$(shell pwd)

.PHONY: default \\
	epmt-build compile build lint \\
	install-py3-conda install-py3-pyenv install-deps \\
	python-dist \\
	epmt-dash \\
	papiex-dist \\
	clean-extra clean-all clean distclean dashclean papiexclean \\
	check check-epmt-check check-integration-tests check-unittests

# general
epmt-build compile build:
	@echo "(epmt-build compile build) whoami: $(shell whoami)"
	cd src/epmt
	python3 -O -bb -m py_compile *.py orm/*.py orm/*/*.py test/*.py

lint:
	@echo "(lint) whoami: $(shell whoami)"
	cd src
	python3 -m pylint --fail-under 8.2 --max-line-length 120 --max-args 6 -ry src/epmt

# virtual environment options
install-py3-conda:
	@echo "(install-py3-conda) whoami: $(shell whoami)"
	set -e ; \
	echo "Installing Python $(PYTHON_VERSION) using conda" ; \
	conda create -n $(EPMT_VERSION)_py$(PYTHON_VERSION) python=$(PYTHON_VERSION) -y ; \
	$(CONDA_ACTIVATE) $(EPMT_VERSION)_py$(PYTHON_VERSION) ; \
	$(MAKE) install-deps ; \
	echo ; \
	echo "Your virtual python environment is epmt-$(EPMT_VERSION)_py$(PYTHON_VERSION)." ;

install-py3-pyenv:
	@echo "(install-py3-pyenv) whoami: $(shell whoami)"
	set -e ; \
	echo "Installing Python $(PYTHON_VERSION) using pyenv" ; \
	PYTHON_CONFIGURE_OPTS="--enable-shared" pyenv install -s $(PYTHON_VERSION) ; \
	pyenv virtualenv $(PYTHON_VERSION) epmt-$(EPMT_VERSION)_py$(PYTHON_VERSION) ; \
	pyenv local epmt-$(EPMT_VERSION)_py$(PYTHON_VERSION) ; \
	$(MAKE) install-deps ; \
	echo ; \
	echo "Your virtual python environment is epmt-$(EPMT_VERSION)_py$(PYTHON_VERSION)." ;

install-deps:
	@echo "(install-deps) whoami: $(shell whoami)"
	set -e ; \
	pip3 install --upgrade pip ; \
	pip3 install -r requirements.txt.py3 ; \
	pip3 install -r $(EPMT_DASH_SRC)/requirements-ui.txt.py3 ;

# Build the wheel from the sdist without native/UI staging prerequisites.
python-dist:
	set -eu
	python3 -m build
	python3 utils/check_python_artifacts.py
	python3 -m twine check --strict dist/*

.PHONY: python-dist-test
python-dist-test: python-dist
	python3 utils/check_python_artifacts.py --install

# ----------- EPMT_DASH THINGS ---------- #
epmt-dash: $(EPMT_DASH_SRC)
	@echo "(epmt-dash) whoami: $(shell whoami)"

$(EPMT_DASH_SRC): $(EPMT_DASH_SRC_TARBALL)
	@echo "(EPMT_DASH_SRC) whoami: $(shell whoami)"
	@echo " ------ GRAB EPMT-DASH SUBMODULE (UI) ------- "
	@echo   "EPMT_DASH_SRC_TARBALL = ${EPMT_DASH_SRC_TARBALL}"
	@echo   "EPMT_DASH_SRC_BRANCH  = ${EPMT_DASH_SRC_BRANCH}"
	@echo   "EPMT_DASH_SRC_URL     = ${EPMT_DASH_SRC_URL}"
	@echo
	@echo
	echo "untarring ${EPMT_DASH_SRC_TARBALL}" ; \
	tar zxf $(EPMT_DASH_SRC_TARBALL) ; \
	mv `tar ztf ${EPMT_DASH_SRC_TARBALL} | head -1` $(EPMT_DASH_SRC) ; \
	echo "top-level dir contents of EPMT_DASH_SRC=${EPMT_DASH_SRC}..." ; \
	ls $(EPMT_DASH_SRC) ; \
	echo "making symbolic link to epmt/ui/docs/index.md" ; \
	cd epmtdocs/docs && \
	ln -s ../../$(EPMT_DASH_SRC)/docs/index.md index.md || \
	echo "symbolic link creation failed. but it's OK." ; \
	cd -

$(EPMT_DASH_SRC_TARBALL):
	@echo "(EPMT_DASH_SRC_TARBALL) whoami: $(shell whoami)"
	echo "grabbing epmt-dash via curl" ; \
	curl -L --fail --retry 3 --retry-delay 5 -o $(EPMT_DASH_SRC_TARBALL) $(EPMT_DASH_SRC_URL) ; \
	ls $(EPMT_DASH_SRC_TARBALL) ; \
# ----------- \end EPMT_DASH THINGS ---------- #


# ----------- PAPIEX THINGS ---------- #
papiex-dist: $(PAPIEX_RELEASE)
	@echo "(papiex-dist) whoami: $(shell whoami)"

$(PAPIEX_RELEASE): $(PAPIEX_SRC)/$(PAPIEX_RELEASE)
	@echo "(PAPIEX_RELEASE) whoami: $(shell whoami)"
	cp $< $@

$(PAPIEX_SRC)/$(PAPIEX_RELEASE): $(PAPIEX_SRC)
	@echo "(PAPIEX_SRC/PAPIEX_RELEASE) whoami: $(shell whoami)"
	@echo " ------ CREATE PAPIEX TARBALL : papiex-dist ------- "
	- @echo "PAPIEX_VERSION     = ${PAPIEX_VERSION}"
	- @echo "PAPIEX_SRC         = ${PAPIEX_SRC}"
	@echo   "PAPIEX_SRC_TARBALL = ${PAPIEX_SRC_TARBALL}"
	@echo   "PAPIEX_SRC_BRANCH  = ${PAPIEX_SRC_BRANCH}"
	@echo   "PAPIEX_SRC_URL     = ${PAPIEX_SRC_URL}"
	- @echo   "PAPIEX_RELEASE     = ${PAPIEX_RELEASE}"
	@echo
	@echo
	@echo "################### BEGIN MAKE PAPIEX TARBALL : papiex-dist ########################################"
	if [ -n "${OUTSIDE_DOCKER}" ]; then \
	echo "within docker. make and make check within PAPIEX_SRC/PAPIEX_RELEASE target" ; \
	make -C $(PAPIEX_SRC) CONFIG_PAPIEX_PAPI=$(CONFIG_PAPIEX_PAPI) CONFIG_PAPIEX_DEBUG=$(CONFIG_PAPIEX_DEBUG) OS_TARGET=$(OS_TARGET) distclean install dist ; \
	make -C $(PAPIEX_SRC) CONFIG_PAPIEX_PAPI=$(CONFIG_PAPIEX_PAPI) CONFIG_PAPIEX_DEBUG=$(CONFIG_PAPIEX_DEBUG) OS_TARGET=$(OS_TARGET) dist-test ; \
	make -C $(PAPIEX_SRC) CONFIG_PAPIEX_PAPI=$(CONFIG_PAPIEX_PAPI) CONFIG_PAPIEX_DEBUG=$(CONFIG_PAPIEX_DEBUG) OS_TARGET=$(OS_TARGET) check ; \
	else \
	echo "outside docker. making docker-dist within PAPIEX_SRC/PAPIEX_RELEASE target" ; \
	make -C $(PAPIEX_SRC) CONFIG_PAPIEX_PAPI=$(CONFIG_PAPIEX_PAPI) CONFIG_PAPIEX_DEBUG=$(CONFIG_PAPIEX_DEBUG) OS_TARGET=$(OS_TARGET) DOCKER_RUN_OPTS="$(DOCKER_RUN_OPTS)" docker-dist ; \
	fi

$(PAPIEX_SRC): $(PAPIEX_SRC_TARBALL)
	@echo "(PAPIEX_SRC) whoami: $(shell whoami)"
	ls $(PAPIEX_SRC_TARBALL) ; \
	echo "tar zxf ${PAPIEX_SRC_TARBALL}" ; \
	tar zxf $(PAPIEX_SRC_TARBALL) ; \
	mv `tar ztf ${PAPIEX_SRC_TARBALL} | head -1` $(PAPIEX_SRC) ; \
	echo "top-level dir contents of PAPIEX_SRC=${PAPIEX_SRC}..." ; \
	ls $(PAPIEX_SRC) ; \

$(PAPIEX_SRC_TARBALL):
	@echo "(PAPIEX_SRC_TARBALL) whoami: $(shell whoami)"
	curl -L --fail --retry 3 --retry-delay 5 -O $(PAPIEX_SRC_URL) ; \
	ls $(PAPIEX_SRC_TARBALL)
# ----------- \end PAPIEX THINGS ---------- #


# ----------- CLEANING ---------- #
clean-extra: clean-all papiexclean dashclean
	@echo "(clean-extra) whoami: $(shell whoami)"

clean-all: clean distclean
	@echo "(clean-all) whoami: $(shell whoami)"

clean:
	@echo "(clean) whoami: $(shell whoami)"
	- find . -type f \( -name "core" -or -name "*~" -or -name "*.pyc" -or -name "epmt.log" \) -exec rm -f {} \;
	- rm -rf $(EPMT_DASH_SRC)/__pycache__ __pycache__ build .venv374

distclean:
	@echo "(distclean) whoami: $(shell whoami)"
	- rm -f settings.py dist/*

dashclean:
	@echo "(dashclean) whoami: $(shell whoami)"
	- rm -rf $(EPMT_DASH_SRC)
	- rm -f $(EPMT_DASH_SRC_TARBALL)
	- rm -f epmtdocs/docs/index.md

papiexclean:
	@echo "(papiexclean) whoami: $(shell whoami)"
	- rm -fr $(PAPIEX_SRC) 
	- rm -f $(PAPIEX_SRC_TARBALL) $(PAPIEX_RELEASE)
# ----------- \end CLEANING ---------- #


# ----------- CHECKING/TESTING ---------- #
check: check-epmt-check check-integration-tests check-unittests
	@echo "(check) whoami: $(shell whoami)"

check-epmt-check:
	@echo "(check-epmt-check) whoami: $(shell whoami)"
	- @env -i TERM=ansi PATH=${PWD}:${PATH} epmt -v -v check

check-integration-tests:
	@echo "(check-integration-tests) whoami: $(shell whoami)"
	- @env -i TERM=ansi PATH=${PWD}:${PATH} TZ=UTC pytest -x -vv src/epmt/test/integration/test_integration_*.py

check-unittests:
	@echo "(check-unittests) whoami: $(shell whoami)"
	- @env -i TERM=ansi PATH=${PWD}:${PATH} epmt -v -v unittest
#	@env -i TERM=ansi PATH=${PWD}:${PATH} python3 -m unittest -v -f \
#   epmt.test.test_lib epmt.test.test_stat epmt.test.test_settings \
#	epmt.test.test_anysh epmt.test.test_submit epmt.test.test_run \
#	epmt.test.test_cmds epmt.test.test_query epmt.test.test_explore \
#	epmt.test.test_outliers epmt.test.test_db_schema epmt.test.test_db_migration
# ----------- \end CHECKING (not used?) ---------- #

# Optional legacy distribution; Python packaging does not depend on these targets.
-include packaging/frozen/targets.mk
