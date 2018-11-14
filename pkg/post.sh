#!/bin/sh -x
#
# Post script
#

# Channge project folder owner
DEPLOY_HOME=/home/xad
PROJ_HOME=$DEPLOY_HOME/derived_speed

# Create project directories
#if [ ! -d $PROJ_HOME/log/pig/ ]; then
#    mkdir -p $PROJ_HOME/log/pig
#fi
#if [ ! -d $PROJ_HOME/tmp/ ]; then
#    mkdir -p $PROJ_HOME/tmp
#fi

# Change user:group and mode
XAD_GROUP=`id -g -n xad`
chown -R xad:$XAD_GROUP $PROJ_HOME
chmod -R g+w $PROJ_HOME

