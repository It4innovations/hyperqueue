## HQ Directives

A submitted script may contain a defined job properties via comments that starts with ``#HQ``.

## HQ directive example

```bash
#!/bin/bash

#HQ --name=Example
#HQ --cpus="2 compact" --pin

./do-something
```

## HQ directive rules

* Directives are parsed when the submitted file has the suffix ``.sh``. For other files it has to be enabled via ``hq submit --directives ...`` 
* Directives have to be defined at the beginning of the file. Only comments or empty lines are allowed to precede the directives.   
* Directives have to be defined in the first 32KiB of the file, the rest of the file is ignored.
* Parameters set via CLI overrides parameters set through directives.
* A script may contain more lines with ``#HQ`` prefix, such lines are merged and evaluated as one.