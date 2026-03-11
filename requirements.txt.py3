# CI/Docker convenience file — installs ALL direct dependencies
# (runtime + every optional extra).  Transitive dependencies are
# resolved automatically by pip and are intentionally omitted.
#
# The canonical dependency specification lives in src/pyproject.toml.
# If you only need the runtime package: pip install epmt
# For a specific extra:                 pip install epmt[test]
# For everything:                       pip install epmt[all]

# -- runtime ---------------------------------------------------------------
alembic
Flask
kneed
numpy==1.26.4
pandas==1.5.3
plotly
psycopg2-binary
py-cpuinfo
pyod
python-daemon
pytz
scikit-learn
scipy
six
SQLAlchemy==1.4.54

# -- notebook --------------------------------------------------------------
ipykernel
ipython
ipywidgets
jupyter
jupyter-client
jupyter-console
jupyter-core
notebook<7
qtconsole

# -- ui --------------------------------------------------------------------
dash
dash-bootstrap-components
dash-daq
dash-table

# -- docs ------------------------------------------------------------------
livereload
mkdocs
mkdocs-git-committers-plugin
mkdocs-theme-bootstrap4

# -- dev (linting / static analysis) ---------------------------------------
astroid
isort
lazy-object-proxy
mccabe
wrapt

# -- test ------------------------------------------------------------------
pytest

# -- build -----------------------------------------------------------------
pyinstaller==5.7.0
pyinstaller-hooks-contrib==2023.11
