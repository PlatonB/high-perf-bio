#!/bin/bash

echo -e "
A script that removes MongoDB (optionally
with data), PyMongo and Streamlit.

Version: 0.6-beta
Dependencies: -
Author: Platon Bykadorov (platon.work@gmail.com), 2022
License: GNU General Public License version 3
Donate: https://www.tinkoff.ru/rm/bykadorov.platon1/7tX2Y99140/
Documentation: https://github.com/PlatonB/high-perf-bio/blob/master/README-EN.md
Bug reports, suggestions, talks: https://github.com/PlatonB/high-perf-bio/issues

Notation:
y - type y to confirm this action;
<enter> - type anything other than y or
type nothing to discard this action.\n"

#Дистро-специфичные команды.
distro_name=$(cat /etc/os-release | grep -Po "(?<=^ID=).+")
ubuntu_based_names=(elementary linuxmint neon)
for ubuntu_based_name in ${ubuntu_based_names[@]}
do
	if [[ $ubuntu_based_name == $distro_name ]]
	then
		distro_name=ubuntu
		break
	fi
done
if [[ $distro_name == ubuntu ]]
then
	sudo service mongod stop; echo
	sudo apt purge -y mongodb-org*; echo
	sudo rm -v /etc/apt/sources.list.d/mongodb-org-6.0.list; echo
	sudo apt autoremove -y; echo
else
	echo -e "Automatic dependencies uninstallation
from $distro_name is not supported yet.\n"
	exit
fi

#Кросс-дистрибутивные команды.
read -p "Delete MongoDB data? (DANGER!) (y/<enter>): " del_mongodb_data; echo
if [[ $del_mongodb_data == y ]]
then
	sudo rm -vr /var/log/mongodb
	sudo rm -vr /var/lib/mongodb; echo
fi
if [[ -d ~/miniconda3/ ]]
then
	conda uninstall -y streamlit; echo
fi
pip3 uninstall -y streamlit pymongo; echo
read -p "Reboot OS now? (recommended) (y/<enter>): " reboot; echo
if [[ $reboot == y ]]
then
	sudo systemctl reboot
fi
