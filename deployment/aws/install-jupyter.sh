#!/bin/bash

AWS_INFO_DIR="/mnt/var/lib/info/"

ANACONDA_PREFIX=/opt/anaconda3
ANACONDA_VERSION=4.2.0
ANACONDA_INSTALLER=Anaconda3-${ANACONDA_VERSION}-Linux-x86_64.sh

SPARK_HOME=/usr/lib/spark
SPARK_MASTER=yarn

is_master() {
    grep -q "\"isMaster\": true" ${AWS_INFO_DIR}/instance.json
    return $?
}


install_anaconda() {
    # Download Anaconda3 if it is not already present
    if [ ! -f ${ANACONDA_INSTALLER} ];
    then
        sudo wget https://repo.continuum.io/archive/${ANACONDA_INSTALLER}
        sudo chmod a+rx ${ANACONDA_INSTALLER}
    fi

    # Start automatic installation into /opt/anaconda3. The parameters
    #  -f force the installation, even if the directory already exists
    #  -b silently accepts the license
    #  -p specifies the installation location
    sudo sh ${ANACONDA_INSTALLER} -f -b -p ${ANACONDA_PREFIX}
}


install_pyspark_kernel() {
    sudo mkdir -p ${ANACONDA_PREFIX}/share/jupyter/kernels/PySpark3
    sudo tee ${ANACONDA_PREFIX}/share/jupyter/kernels/PySpark3/kernel.json >/dev/null <<EOL
{
 "display_name": "PySpark 2.1 (Python 3.5)",
 "language": "python",
 "argv": [
  "${ANACONDA_PREFIX}/bin/python3",
  "-m", "ipykernel",
  "-f", "{connection_file}"
 ],
 "env": {
  "TZ": "UTC",
  "SPARK_MAJOR_VERSION": "${SPARK_MAJOR_VERSION}",
  "SPARK_HOME": "${SPARK_HOME}",
  "PYTHONPATH": "${SPARK_HOME}/python/:${SPARK_HOME}/python/lib/py4j-src.zip",
  "PYTHONSTARTUP": "${SPARK_HOME}/python/pyspark/shell.py",
  "PYTHONHASHSEED": "0",
  "SPARK_YARN_USER_ENV": "PYTHONHASHSEED=0",
  "PYSPARK_PYTHON": "${ANACONDA_PREFIX}/bin/python3",
  "PYSPARK_SUBMIT_ARGS": "--master ${SPARK_MASTER} --driver-memory=2G --executor-cores=2 --executor-memory=2G --num-executors=2 pyspark-shell"
 }
}
EOL
}


install_python_kernel() {
    sudo mkdir -p ${ANACONDA_PREFIX}/share/jupyter/kernels/IPython
    sudo tee ${ANACONDA_PREFIX}/share/jupyter/kernels/IPython/kernel.json > /dev/null <<EOL
{
 "display_name": "Python 3.5",
 "language": "python",
 "argv": [
    "python3", 
    "-m", "ipykernel",
    "-f", "{connection_file}"
  ]
}
EOL
}


install_startup() {
    sudo tee /usr/local/bin/jupyter-notebook > /dev/null <<EOL
#!/bin/bash
source ${ANACONDA_PREFIX}/bin/activate
jupyter notebook --NotebookApp.ip=0.0.0.0 --NotebookApp.port=8888 --NotebookApp.open_browser=False
EOL
    sudo chmod a+rx /usr/local/bin/jupyter-notebook

    sudo tee /etc/init/jupyter-notebook-server.conf > /dev/null <<EOL
description "Jupyter Notebook Server"

start on runlevel [2345]
stop on runlevel [016]

start on started netfs
start on started rsyslog

stop on stopping netfs
stop on stopping rsyslog

respawn

# respawn unlimited times with 5 seconds time interval
respawn limit 0 5

env SLEEP_TIME=10

env DAEMON="jupyter-notebook-server"
env DESC="Jupyter Notebook Server"
env EXEC_PATH="/usr/local/bin/jupyter-notebook"
env SVC_USER="hadoop"
env DAEMON_FLAGS=""
env PIDFILE="/var/run/jupyter/\${DAEMON}.pid"
env LOGFILE="/var/log/jupyter/\${DAEMON}.out"
env WORKING_DIR="/home/hadoop"

pre-start script
  install -d -m 0755 -o \$SVC_USER -g \$SVC_USER \$(dirname \$PIDFILE) 1>/dev/null 2>&1 || :
  install -d -m 0755 -o \$SVC_USER -g \$SVC_USER \$(dirname \$LOGFILE) 1>/dev/null 2>&1 || :

  if [ ! -x \$EXEC_PATH ]; then
    echo "\$EXEC_PATH is not an executable"
    exit 1
  fi

  run_prestart() {
      cd \${WORKING_DIR}
      su -s /bin/bash \$SVC_USER -c "nohup nice -n 0 \
          \${EXEC_PATH} \$DAEMON_FLAGS \
          > \$LOGFILE 2>&1 & "'echo \$!' > "\$PIDFILE"
  }

  export -f run_prestart
  $EXEC_LAUNCHER run_prestart
end script

script

  # sleep for sometime for the daemon to start running
  sleep \$SLEEP_TIME
  if [ ! -f \$PIDFILE ]; then
    echo "\$PIDFILE not found"
    exit 1
  fi
  pid=\$(<"\$PIDFILE")
  while ps -p \$pid > /dev/null; do
    sleep \$SLEEP_TIME
  done
  echo "\$pid stopped running..."

end script

pre-stop script

 # do nothing

end script

post-stop script
  if [ ! -f \$PIDFILE ]; then
    echo "\$PIDFILE not found"
    exit
  fi
  pid=\$(<"\$PIDFILE")
  if kill \$pid > /dev/null 2>&1; then
    echo "process \$pid is killed"
  fi
  rm -rf \$PIDFILE
end script
EOL
    sudo initctl start jupyter-notebook-server
}


install_anaconda

if is_master;
then
    install_pyspark_kernel
    install_python_kernel
    install_startup
fi

