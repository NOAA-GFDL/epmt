To publish new release, cease merging new PRs to `main`, and carefully follow the below procedure:



## first, create a new tag for release (IN DEVELOPMENT)

- [ ] create a new branch off `main`, *give it a name different than the exact tag you are creating*
- [ ] edit the version number in `src/epmt/epmtlib.py` to the desired version tag of format `X.Y.Z` (remove the `.post` suffix)
- [ ] from a clean checkout, install `build` and `twine`, then run `make python-dist-test`.
      Check the sdist and wheel in `dist/`; do not stage papiex libraries or epmt-dash into Python artifacts.
- [ ] open a PR to `main` in this repository after making the version change
- [ ] if checks pass, create the tag from the branch locally in your terminal, with `git tag X.Y.Z;`



## second, publish release to PyPI, then github, in that order (IN DEVELOPMENT)

- [ ] obtain explicit human approval before pushing the tag; there is no additional manual environment gate.
- [ ] push your locally created tag with `git checkout X.Y.Z; git push origin HEAD:refs/tags/X.Y.Z`
- [ ] pushing the new tag `X.Y.Z` triggers the `pip_publish` pipeline.
      Source/tag/artifact version checks and clean-install tests on Python 3.10/3.11 must pass before publishing.
      Publication uses the same validated artifacts, without rebuilding.
- [ ] wait for the pipeline to finish and find the release on PyPI.
- [ ] on PyPI, download the built package `tar.gz` file for version `X.Y.Z`

WARNING: *any problems or mistakes after the next step are irreversible due to package immutability so make sure things are working before continuing*

- [ ] compare the PyPI sdist SHA256 with the validated workflow artifact before uploading it elsewhere.
- [ ] with human approval, create a GitHub release *including the tarball you downloaded in the previous step*,
      generate contribution notes, and save the release
- [ ] check that the release looks right: it needs the PyPI `tar.gz` file with the `X.Y.Z` tag, and contribution notes.



## third, publish release to `conda-forge` via `epmt-feedstock` fork (IN DEVELOPMENT)

- [ ] use (create if needed) an `epmt-feedstock` fork to create a new branch called `epmtX.Y.Z`
- [ ] adjust the version to `X.Y.Z` and update the `sha256` to what it says on PyPI in `recipe.yaml`
- [ ] open a PR to `conda-forge/epmt-feedstock`
- [ ] once checks pass, a reviewer with access to `conda-forge/epmt-feedstock` can approve and merge, kicking off the rest of the publishing pipeline to `conda-forge`



## wrap-up (IN DEVELOPMENT)

- [ ] back to the `epmt` PR we opened initially.
- [ ] edit the version number in `src/epmt/epmtlib.py` to `X.Y.Z.post` (bump to next version with `.post` suffix), let the checks pass
- [ ] merge the PR branch you used for creating the release to `main`
