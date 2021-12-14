#!/bin/bash
for i in {1..29}
do
	echo $i
	echo "node"$i
	scp ~/.bashrc "node$i":~/
	scp -r /usr/local/hadoop-2.6.0 "node"$i:~/
	scp ~/mpich-3.2.1.tar.gz "node"$i:~/
	ssh zz_y@"node"$i "tar -xvzf mpich-3.2.1.tar.gz ; cd mpich-3.2.1 ; \
	./configure -prefix=/usr/local/mpich --disable-fortran ; \
	sudo make ; \
	sudo make install ; \
	sudo apt-get update ; \
	sudo apt-get install -y openjdk-8-jdk ; \
	sudo mkdir /local/hdfs ; \
	sudo mv ~/hadoop-2.6.0 /usr/local/ ; \
	sudo chown zz_y /local/hdfs ; \
	sudo chmod 755 /local/hdfs ; \
	sudo chown -R zz_y /usr/local/hadoop-2.6.0 ; \
	source ~/.bashrc
	exit" &
done

