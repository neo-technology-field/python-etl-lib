# Configuring Sphinx to build documentation, including API

This is based on https://github.com/JamesALeedham/Sphinx-Autosummary-Recursion Thanks for providing this.


## Build the documentation:

   `make html`

NOTE: to debug rst errors, run `sphinx-build -b html . _build/html -n -T -v -E --keep-going`

## Run live http review:

   `make livehtml`

