# HBase Intro

This is a first small introduction into HBase.

## Preparing

You need to create an appropriate HBase table before running the example. This can be done within the HBase shell.

create_namespace 'training'
create 'training:mytable', 'f1'


## Running

    ./run.sh --table training:mytable --family f1
    