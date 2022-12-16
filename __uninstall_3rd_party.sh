#!/bin/bash

echo -e "
A script that removes MongoDB (optionally
with data), PyMongo and Streamlit.

Version: v1.1
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

#Театр безопасности.
read -p "Are you really want to remove the aforementioned
packages along with their config files? (y/<enter>): " agreement; echo
if [[ $agreement != y ]]; then
	echo -e "Canceled.\n"
	exit
fi

#Определение дистрибутива.
distro_name=$(cat /etc/os-release |
              grep -Po "(?<=^ID=).+" |
              grep -Po "\w+")
ubuntu_family=(elementary linuxmint neon ubuntu)
redhat_family=(almalinux centos fedora rhel rocky)
for name in ${ubuntu_family[@]}
do
	if [[ $name == $distro_name ]]; then
		distro_name=ubuntu
		break
	fi
done
for name in ${redhat_family[@]}
do
	if [[ $name == $distro_name ]]; then
		distro_name=rhel
		break
	fi
done

#Дистро-специфичные команды.
if [[ $distro_name == ubuntu ]]; then
	sudo service mongod stop; echo
	sudo apt purge -y mongodb-org*; echo
	sudo apt autoremove -y; echo
	sudo rm -v /etc/apt/sources.list.d/mongodb-org-*.list; echo
elif [[ $distro_name == rhel ]]; then
	sudo service mongod stop; echo
	sudo yum erase -y $(rpm -qa | grep mongodb-org); echo
	sudo rm -v /etc/yum.repos.d/mongodb-org-*.repo; echo
else
	echo -e "Automatic dependencies uninstallation
from $distro_name is not supported yet.\n"
	exit
fi

#Кросс-дистрибутивные команды.
read -p "Delete MongoDB data? (DANGER!) (y/<enter>): " del_mongodb_data; echo
if [[ $del_mongodb_data == y ]]; then
	sudo rm -vr /var/log/mongodb; echo
	sudo rm -vr /var/lib/mongodb; echo
	sudo rm -vr /var/lib/mongo; echo
fi
if [[ -d ~/miniconda3/ ]]; then
	conda uninstall -y streamlit; echo
fi
pip3 uninstall -y streamlit pymongo; echo
read -p "Reboot OS now? (recommended) (y/<enter>): " reboot; echo
if [[ $reboot == y ]]; then
	sudo systemctl reboot
fi
