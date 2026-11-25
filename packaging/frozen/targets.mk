# Legacy PyInstaller/bundled distribution. Included from the repository root.
EPMT_RELEASE=epmt-$(EPMT_VERSION)-$(OS_TARGET).tgz
EPMT_FULL_RELEASE=EPMT-release-$(EPMT_VERSION)-$(OS_TARGET).tgz
EPMT_INSTALL_PATH=/opt/minimalmetrics
EPMT_INSTALL_PREFIX=$(EPMT_INSTALL_PATH)/epmt-$(EPMT_VERSION)/epmt-install

.PHONY: dist dist-test docker-dist docker-dist-test epmt-full-release build-check-release check-release release dockerclean \
	frozen-dist frozen-dist-test frozen-docker-dist frozen-docker-dist-test frozen-full-release frozen-build-check-release frozen-check-release frozen-release frozen-dockerclean frozen-clean

# Compatibility entry points live with the implementation they expose.
dist: frozen-dist
dist-test: frozen-dist-test
docker-dist: frozen-docker-dist
docker-dist-test: frozen-docker-dist-test
epmt-full-release: frozen-full-release
build-check-release: frozen-build-check-release
check-release: frozen-check-release
release: frozen-release
dockerclean: frozen-dockerclean
clean-all: frozen-clean frozen-dockerclean

# This target runs pyinstaller, outputs a tarball with epmt + all dependencies included
$(EPMT_RELEASE) frozen-dist:
	@echo "(EPMT_RELEASE dist) whoami: $(shell whoami)"
	@echo "WARNING removing directories: rm -rf epmt-install build"
	rm -rf epmt-install build && mkdir -p epmt-install/epmt/epmtdocs
	@echo "DONE removing epmt-install/ build/ and recreating epmt-install/epmt/epmt-docs"
	@echo
	@echo
	@echo "**********************************************************"
	@echo "*************** calling pyinstaller **********************"
	@echo "**********************************************************"
	pyinstaller --version
#	@echo "pyinstaller --clean --noconfirm --distpath=epmt-install packaging/frozen/epmt.spec"
#	pyinstaller --clean --noconfirm --distpath=epmt-install packaging/frozen/epmt.spec
	@echo "WARNING NOT CLEAN: pyinstaller --noconfirm --distpath=epmt-install packaging/frozen/epmt.spec"
	pyinstaller --noconfirm --distpath=epmt-install packaging/frozen/epmt.spec
	@echo
	@echo
	@echo "**********************************************************"
	@echo "****************** calling mkdocs ************************"
	@echo "**********************************************************"
#   add --dirty to the build call to utilize the cache when building docs
	@echo "WARNING DIRTY: mkdocs build --dirty -f epmtdocs/mkdocs.yml"
	mkdocs build --dirty -f epmtdocs/mkdocs.yml
#	@echo "mkdocs build -f epmtdocs/mkdocs.yml"
#	mkdocs build -f epmtdocs/mkdocs.yml
	@echo
	@echo
	cp -Rp preset_settings epmt-install
	cp -Rp notebooks epmt-install
	cp -Rp src/epmt/epmt_migrations epmt-install/migrations
	cp -pr src/epmt epmt-install
#	examples
	mkdir epmt-install/examples
	cp src/epmt/test/shell/epmt-example.*sh epmt-install/examples
#	slurm
	mkdir epmt-install/slurm
	cp utils/SLURM/slurm_task_*log_epmt.sh epmt-install/slurm
#	docs
	cp -Rp epmtdocs/site epmt-install/epmt/epmtdocs
	@echo
	@echo
	@echo "**********************************************************"
	@echo "************* creating ${EPMT_RELEASE} *******************"
	@echo "**********************************************************"
	tar -czf $(EPMT_RELEASE) epmt-install

# creates a tarball containing test directories. If the test directory exists, clobber and re-create
test-$(EPMT_RELEASE) frozen-dist-test:
	@echo "(test-EPMT_RELEASE dist-test) whoami: $(shell whoami)"
	@echo
	@echo "WARNING recreating directories: epmt-install-tests"
	rm -rf epmt-install-tests && mkdir epmt-install-tests
	@echo "DONE recreating directories: rm -rf epmt-install-tests"
	@echo
	cp -Rp src/epmt/test epmt-install-tests
	@echo "creating test-tarball in final location: test-${EPMT_RELEASE}"
	tar -czf test-$(EPMT_RELEASE) epmt-install-tests
	@echo
	@echo "WARNING removing directory to clean up: rm -rf epmt-install-tests"
	rm -rf epmt-install-tests

# this target 1) builds an image with an environment in which we'd like to build our application
# 2) builds that application within a running container of that image
# NOTE: bind mounts to current working directory, usually the repository directory
frozen-docker-dist:
	@echo "(docker-dist) whoami: $(shell whoami)"
	@echo " ------ CREATE EPMT TARBALL: docker-dist ------- "
	@echo "       build command = ${DOCKER_BUILD}"
	@echo "       run   command = ${DOCKER_RUN}"
	@echo "       run   options = ${DOCKER_RUN_OPTS}"
	@echo
	@echo
	@echo " - docker build <STUFF> Dockerfiles/Dockerfile.$(OS_TARGET)-epmt-build"
	@echo "       we are creating a container environment in which to build the python distribution"
	$(DOCKER_BUILD) Dockerfiles/Dockerfile.$(OS_TARGET)-epmt-build -t $(OS_TARGET)-epmt-build:$(EPMT_VERSION) \
	--build-arg sqlite_version=$(SQLITE_VERSION) \
	--build-arg sqlite_year=$(SQLITE_YEAR) \
	--build-arg python_version=$(PYTHON_VERSION) .
	@echo
	@echo
	@echo " - docker run <STUFF> <use image built from Dockerfiles/Dockerfile.$(OS_TARGET)-epmt-build>"
	@echo "       within a running container of the image we just built, now build the python application."
	@echo "       i.e. running make frozen-dist python-dist frozen-dist-test inside $(OS_TARGET)-epmt-build"
	$(DOCKER_RUN) $(DOCKER_RUN_OPTS) --privileged \
	--volume=$(PWD):$(PWD) \
	-w $(PWD) $(OS_TARGET)-epmt-build:$(EPMT_VERSION) \
	make --debug OS_TARGET=$(OS_TARGET) frozen-dist python-dist frozen-dist-test
#   wait.... is this one of those ... "pip install it twice deals? commenting out for now..."
#	make --debug OS_TARGET=$(OS_TARGET) dist python-dist $(EPMT_RELEASE) dist-test


# tests distribution within a container
frozen-docker-dist-test:
	@echo "(docker-dist-test) whoami: $(shell whoami)"
	$(DOCKER_RUN) $(DOCKER_RUN_OPTS) --privileged \
	--volume=$(PWD):$(PWD) \
	-w $(PWD) $(OS_TARGET)-epmt-build:$(EPMT_VERSION) \
	make --debug OS_TARGET=$(OS_TARGET) dist-test


# ----------- EPMT_FULL_RELEASE THINGS ---------- #
frozen-full-release: $(EPMT_FULL_RELEASE)
	@echo "(epmt-full-release) whoami: $(shell whoami)"

$(EPMT_FULL_RELEASE): $(EPMT_RELEASE) test-$(EPMT_RELEASE) $(PAPIEX_RELEASE)
	@echo "(EPMT_FULL_RELEASE) whoami: $(shell whoami)"
	@echo "Making EPMT $(EPMT_VERSION) for $(OS_TARGET): $^"
	tar -cvzf $(EPMT_FULL_RELEASE) $(notdir $^)
	@echo
	@echo "$(EPMT_FULL_RELEASE) build complete!"
	@echo

frozen-build-check-release: $(EPMT_FULL_RELEASE)
	@echo "(build-check-release) whoami: $(shell whoami)"
	@echo "creating an all-included testing environment / delivery tarball"
	$(DOCKER_BUILD) Dockerfiles/Dockerfile.$(OS_TARGET)-epmt-test-release \
	-t $(OS_TARGET)-epmt-test-release:$(EPMT_VERSION) \
	--build-arg epmt_version=$(EPMT_VERSION) \
	--build-arg install_path=$(EPMT_INSTALL_PATH) \
	--build-arg install_prefix=$(EPMT_INSTALL_PREFIX) \
	--build-arg epmt_full_release=$(EPMT_FULL_RELEASE) \
	--build-arg epmt_python_full_release=$(EPMT_PYTHON_FULL_RELEASE) \
	--build-arg papiex_release=$(PAPIEX_RELEASE) \
	--build-arg sqlite_version=$(SQLITE_VERSION) \
	--build-arg sqlite_year=$(SQLITE_YEAR) \
	--build-arg python_version=$(PYTHON_VERSION) .
	@echo

frozen-check-release:
	@echo "(check-release) whoami: $(shell whoami)"
	@echo "looking for postgres-test container"
	if docker ps | grep postgres-test > /dev/null ; \
	then docker stop postgres-test; fi
	@echo
	@echo "looking for epmt-test-net docker networks"
	if docker network ls | grep epmt-test-net > /dev/null ; \
	then docker network rm -f epmt-test-net; fi
	@echo
	@echo
	@echo "creating epmt-test-net docker network"
	docker network create epmt-test-net
	@echo
	@echo "running postgres-test container"
	$(DOCKER_RUN) -d --rm --name postgres-test --network epmt-test-net --privileged \
	-e POSTGRES_USER=postgres \
	-e POSTGRES_PASSWORD=example \
	-e POSTGRES_DB=EPMT-TEST postgres:latest
	@echo
	@echo
	@echo "looking for prev ran epmt-test-release container to remove..."
	if docker container ls -a | grep epmt-$(EPMT_VERSION)-test-release > /dev/null ; \
	then docker container rm $(OS_TARGET)-epmt-$(EPMT_VERSION)-test-release; sleep 2; fi
	@echo
	@echo "running epmt-test-release container"
	$(DOCKER_RUN) --name $(OS_TARGET)-epmt-$(EPMT_VERSION)-test-release \
	--network epmt-test-net \
	--privileged $(DOCKER_RUN_OPTS) \
	-h slurmctl $(OS_TARGET)-epmt-test-release:$(EPMT_VERSION) \
	bash -c 'echo 2 > /proc/sys/kernel/perf_event_paranoid; epmt -vv -V ; \
	echo "" ; \
	echo "" ; \
	echo "" && echo "------ epmt -vv check ------" && epmt -vv check \
	|| echo "epmt -vv check failure guard, keep going" ; \
	echo "" ; \
	echo "" ; \
	echo "" && echo "------ epmt -vv unittest ------" && epmt -vv unittest \
	|| echo "epmt -vv unittest failure guard, keep going" ; \
	echo "" ; \
	echo "" ; \
	echo "" && echo "------ pytest integration ------" && TZ=UTC pytest -x -vv src/epmt/test/integration/test_integration_*.py \
	|| echo "pytest integration failure guard, keep going" ; \
	echo ""'
	@echo
	@echo
# ----------- \end EPMT_FULL_RELEASE THINGS ---------- #


# ----------- release targets ---------- #
frozen-release:
	@echo "(release) whoami: $(shell whoami)"
	@echo
	@echo
	@echo " ------ MAKE : clean-all / CLEAN ------- "
	$(MAKE) clean-all
	@echo
	@echo
	@echo " ------ MAKE : epmt-dash / DASH ------- "
	$(MAKE) epmt-dash
	@echo
	@echo
	@echo " ------ MAKE : papiex-dist / PAPIEX ------- "
	$(MAKE) papiex-dist
	@echo
	@echo
	@echo " ------ MAKE : docker-dist / DIST ------- "
	$(MAKE) frozen-docker-dist
	@echo
	@echo
	@echo " ------ MAKE : epmt-full-release / FULL-RELEASE ------- "
	$(MAKE) frozen-full-release
	@echo
	@echo
	@echo " ------ MAKE : build-check-release / CHECK-RELEASE ------- "
	$(MAKE) frozen-build-check-release
	@echo
	@echo
	@echo " ------ MAKE : check-release / CHECK-RELEASE ------- "
	$(MAKE) frozen-check-release
	@echo
	@echo
	@echo "done building epmt"
# ----------- \end release targets ---------- #


frozen-dockerclean:
	@echo "(dockerclean) whoami: $(shell whoami)"
	- docker image rm --force \
	$(OS_TARGET)-epmt-test-release:$(EPMT_VERSION) $(OS_TARGET)-epmt-build:$(EPMT_VERSION)


frozen-clean:
	- rm -rf epmt-install epmt-install-tests epmtdocs/site
	- rm -f $(EPMT_RELEASE) test-$(EPMT_RELEASE) $(EPMT_FULL_RELEASE)
