#!/bin/bash

echo -e "
A script that installs MongoDB, PyMongo and Streamlit.

Version: v1.0
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

#Определение дистрибутива.
#Грязный хак для Fedora.
distro_name=$(cat /etc/os-release |
              grep -Po "(?<=^ID=).+" |
              grep -Po "\w+")
ubuntu_family=(elementary linuxmint neon ubuntu)
redhat_family=(almalinux centos fedora rhel rocky)
for name in ${ubuntu_family[@]}
do
	if [[ $name == $distro_name ]]; then
		distro_name=ubuntu
		ubuntu_ver=$(cat /etc/os-release |
		             grep -Po "(?<=^UBUNTU_CODENAME=).+")
		break
	fi
done
for name in ${redhat_family[@]}
do
	if [[ $name == $distro_name ]]; then
		if [[ $distro_name == fedora ]]; then
			rhel_ver=9
		else
			rhel_ver=$(cat /etc/os-release |
			           grep -Po "(?<=^VERSION_ID=).+" |
			           grep -Po "\d+" |
			           head -1)
		fi
		distro_name=rhel
		break
	fi
done

#Дистро-специфичные команды.
if [[ $distro_name == ubuntu ]]; then
	if [[ $ubuntu_ver == jammy ]]; then
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
	sudo apt install -y python3-pip; echo
elif [[ $distro_name == rhel ]]; then
	echo "[mongodb-org-6.0]
name=MongoDB Repository
baseurl=https://repo.mongodb.org/yum/redhat/$rhel_ver/mongodb-org/6.0/x86_64/
gpgcheck=1
enabled=1
gpgkey=https://www.mongodb.org/static/pgp/server-6.0.asc" |
	sudo tee /etc/yum.repos.d/mongodb-org-6.0.repo; echo
	sudo yum install -y mongodb-org; echo
	sudo dnf install -y python3-pip; echo
else
	echo -e "Automatic dependencies installation
on $distro_name is not supported yet.\n"
	exit
fi

#Кросс-дистрибутивные команды.
sudo systemctl enable mongod
if [[ -d ~/miniconda3/ ]]; then
	conda install -y streamlit; echo
else
	pip3 install streamlit; echo
fi
pip3 install pymongo; echo
read -p "Reboot OS now? (recommended) (y/<enter>): " reboot; echo
if [[ $reboot == y ]]; then
	sudo systemctl reboot
fi
