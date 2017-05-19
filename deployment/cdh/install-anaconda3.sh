#!/bin/bash

cd

# Download Anaconda
wget http://repo.continuum.io/archive/Anaconda3-4.1.1-Linux-x86_64.sh
chmod a+x Anaconda3-4.1.1-Linux-x86_64.sh

# Install Anaconda
sudo bash Anaconda3-4.1.1-Linux-x86_64.sh -f -b -p /opt/anaconda3

mkdir -p bin

# Fix Hive/spark
sudo rm -f /etc/spark/conf/hive-site.xml
sudo ln -f -s /etc/hive/conf/hive-site.xml /etc/spark/conf/hive-site.xml


# Create Start script
cat <<EOT > bin/jupyter-spark
#!/bin/bash

export ANACONDA_ROOT=/opt/anaconda3
export PYSPARK_DRIVER_PYTHON=\$ANACONDA_ROOT/bin/jupyter
export PYSPARK_DRIVER_PYTHON_OPTS="notebook --NotebookApp.open_browser=True --NotebookApp.ip=0.0.0.0 --NotebookApp.port=8880"
export PYSPARK_PYTHON=\$ANACONDA_ROOT/bin/python 

export TZ=UTC
export PYTHONHASHSEED=0
export SPARK_YARN_USER_ENV=PYTHONHASHSEED=0

pyspark --master=yarn-client
EOT
chmod a+x bin/jupyter-spark


# Create additional script for Python3 PySpark
cat <<EOT > bin/pyspark3
#!/bin/bash

export ANACONDA_ROOT=/opt/anaconda3
export PYSPARK_DRIVER_PYTHON=\$ANACONDA_ROOT/bin/python
export PYSPARK_PYTHON=\$ANACONDA_ROOT/bin/python 

export TZ=UTC
export PYTHONHASHSEED=0
export SPARK_YARN_USER_ENV=PYTHONHASHSEED=0

pyspark \$@
EOT
chmod a+x bin/pyspark3

