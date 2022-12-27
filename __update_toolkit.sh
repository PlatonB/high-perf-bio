#!/bin/bash

echo -e "
A script that updates the high-perf-bio itself.

Version: v1.0
Dependencies: -
Author: Platon Bykadorov (platon.work@gmail.com), 2022
License: GNU General Public License version 3
Donate: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Documentation: https://github.com/PlatonB/high-perf-bio/blob/master/README-EN.md
Bug reports, suggestions, talks: https://github.com/PlatonB/high-perf-bio/issues

The script doesn't update dependencies.\n"

cd $(dirname $(realpath $0))
cd ..
wget https://github.com/PlatonB/high-perf-bio/archive/refs/heads/master.zip
unzip -o master.zip; echo
rm -v master.zip; echo
cd high-perf-bio-master
pwd; echo
