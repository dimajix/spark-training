#!/bin/bash

ANACONDA_PREFIX=/opt/anaconda3
ANACONDA_VERSION=4.2.0
ANACONDA_INSTALLER=Anaconda3-${ANACONDA_VERSION}-Linux-x86_64.sh

SPARK_MAJOR_VERSION=2
SPARK_HOME=/usr/hdp/2.5.0.0-1245/spark2

# Check if current user is root. Since we will be installing YUM packages and
# let ambari perform actions as root, we need to be root to setup everything.
if [ ! $(id -u) -eq 0 ]; then
    echo "Need to be root to setup Hadoop client"
    exit 1
fi

# Install potentially missing software
yum install -y bzip2 wget curl

cd /root

# Download Anaconda3 if it is not already present
if [ ! -f ${ANACONDA_INSTALLER} ];
then
    wget https://repo.continuum.io/archive/${ANACONDA_INSTALLER}
    chmod a+rx ${ANACONDA_INSTALLER}
fi

# Start automatic installation into /opt/anaconda3. The parameters
#  -f force the installation, even if the directory already exists
#  -b silently accepts the license
#  -p specifies the installation location
sh ${ANACONDA_INSTALLER} -f -b -p ${ANACONDA_PREFIX}

# Activate Anaconda3 environment
source ${ANACONDA_PREFIX}/bin/activate

mkdir -p ${ANACONDA_PREFIX}/share/jupyter/kernels/IPython
cat > ${ANACONDA_PREFIX}/share/jupyter/kernels/IPython/kernel.json <<EOL
{
 "argv": ["python3", "-m", "IPython.kernel",
          "-f", "{connection_file}"],
 "display_name": "Python 3",
 "language": "python"
}
EOL

mkdir -p ${ANACONDA_PREFIX}/share/jupyter/kernels/PySpark3
cat > ${ANACONDA_PREFIX}/share/jupyter/kernels/PySpark3/kernel.json <<EOL
{
 "display_name": "PySpark 2.0 (Python 3.4)",
 "language": "python",
 "argv": [
  "${ANACONDA_PREFIX}/bin/python3",
  "-m",
  "IPython.kernel",
  "-f",
  "{connection_file}"
 ],
 "env": {
  "http_proxy": "http://proxy:3128",
  "https_proxy": "http://proxy:3128",
  "TZ": "UTC",
  "SPARK_MAJOR_VERSION": "${SPARK_MAJOR_VERSION}",
  "SPARK_HOME": "${SPARK_HOME}",
  "PYTHONPATH": "${SPARK_HOME}/python/:${SPARK_HOME}/python/lib/py4j-0.10.1-src.zip",
  "PYTHONSTARTUP": "${SPARK_HOME}/python/pyspark/shell.py",
  "PYTHONHASHSEED": "0",
  "SPARK_YARN_USER_ENV": "PYTHONHASHSEED=0",
  "PYSPARK_PYTHON": "${ANACONDA_PREFIX}/bin/python3",
  "PYSPARK_SUBMIT_ARGS": "--master yarn --driver-memory=2G --executor-cores=4 --executor-memory=4G --num-executors=2 --driver-java-options=\"-Dhttp.proxyHost=proxy -Dhttp.proxyPort=3128 -Dhttps.proxyHost=proxy -Dhttps.proxyPort=3128\" pyspark-shell"
 }
}
EOL


echo "Starting jupyter notebook:"
echo "    source ${ANACONDA_PREFIX}/bin/activate"
echo "    jupyter notebook --NotebookApp.ip=0.0.0.0 --NotebookApp.port=8888 --NotebookApp.open_browser=False"

