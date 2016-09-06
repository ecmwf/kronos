#!/bin/sh
#!/bin/sh

export PATH=${bamboo_working_directory}/miniconda/bin:${PATH}

find ${bamboo_working_directory} -type f -name "*.py" -print0 | xargs -0 -n1 pyflakes;
