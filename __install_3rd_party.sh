#!/bin/bash

echo -e "
A script that installs MongoDB, PyMongo and Streamlit.

Version: 0.7-beta
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
	ubuntu_ver=$(cat /etc/os-release | grep -Po "(?<=^UBUNTU_CODENAME=).+")
	if [[ $ubuntu_ver == jammy ]]
	then
		wget http://archive.ubuntu.com/ubuntu/pool/main/o/openssl/libssl1.1_1.1.1f-1ubuntu2_amd64.deb
		sudo apt install ./libssl1.1_1.1.1f-1ubuntu2_amd64.deb; echo
		rm -v libssl1.1_1.1.1f-1ubuntu2_amd64.deb; echo
	fi
	wget -qO - https://www.mongodb.org/static/pgp/server-6.0.asc |
	sudo apt-key add -; echo
	echo "deb [ arch=amd64,arm64 ] https://repo.mongodb.org/apt/ubuntu $ubuntu_ver/mongodb-org/6.0 multiverse" |
	sudo tee /etc/apt/sources.list.d/mongodb-org-6.0.list; echo
	sudo apt update; echo
	sudo apt install -y mongodb-org; echo
	sudo systemctl enable mongod
	sudo apt install -y python3-pip; echo
else
	echo -e "Automatic dependencies installation
on $distro_name is not supported yet.\n"
	exit
fi

#Кросс-дистрибутивные команды.
if [[ -d ~/miniconda3/ ]]
then
	conda install -y streamlit; echo
else
	pip3 install streamlit; echo
fi
pip3 install pymongo; echo
read -p "Reboot OS now? (recommended) (y/<enter>): " reboot; echo
if [[ $reboot == y ]]
then
	sudo systemctl reboot
fi
