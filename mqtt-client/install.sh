#!/bin/bash
FILENAME=install.sh
INSTALL_PATH=/usr/local/
INIT_PATH=/etc/init.d/

if [ -f $FILENAME ]; then
	echo "Copying files to '$INSTALL_PATH'"
	cp -rf ../../mqtt-client $INSTALL_PATH
	echo "Copying init.d script to '$INIT_PATH'"
	cp -f ./script/init.d/adaxsub $INIT_PATH
	\rm -f $INSTALL_PATH/mqtt-client/mqtt-client/$FILENAME
else
	echo "Need to be in the path containing '$FILENAME'."
fi
