# Directives
You can specify job parameters using special comments (`directives`) specified in a submitted shell
script. Directives are lines that begin with the `#HQ` prefix. Any text following this prefix will
be interpreted as a command line argument for `hq submit`.

## Example directive file

Suppose that `script.sh` has the following content:
```bash
#!/bin/bash

#HQ --name=Example
#HQ --cpus="2 compact" --pin taskset

./my-program
```

If you execute
```bash
$ hq submit script.sh
```
it will behave as if you have executed
```bash
$ hq submit --name=Example --cpus="2 compact" --pin taskset script.sh
```

## Directives mode
You can select three modes using the `--directives` flag of `hq submit`. The mode will
determine when should HyperQueue attempt to parse directives from the provided command.

* `auto` (default) - Directives will be parsed if the first command passed to `hq submit` has the
  `.sh` extension.
* `file` - Directives will be parsed from the first command passed to `hq submit`.
* `stdin` - Directives will be parsed from stdin (see ``--stdin``) 
* `off` - Directives will not be parsed.

## Notes

* Directives have to be defined at the beginning of the file. Only comments or empty lines are allowed
  to precede the directives.   
* Directives have to be defined in the first 32KiB of the file, the rest of the file is ignored.
* Parameters set via CLI have precedence over parameters set via direectives:
    * Parameters that cannot occur multiple times (like `--name`) **will be overriden** by values set from CLI.
    * Parameters that can occur multiple times (like `--resource`) will be combined from CLI and from directives.
* A script may contain more lines with the `#HQ` prefix, such lines are combined and evaluated as a
continuous list of parameters.
