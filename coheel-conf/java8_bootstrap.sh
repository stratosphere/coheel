#!/bin/bash
###############
# Install JAVA 8
###############

# Check java version
JAVA_VER=$(java -version 2>&1 | sed 's/java version "\(.*\)\.\(.*\)\..*"/\1\2/; 1q')
echo "Java Version"
echo $JAVA_VER

if [ "$JAVA_VER" -lt 18 ]
then
    # Make java 8 the default
    sudo alternatives --config java <<< '2'
    sudo alternatives --config javac <<< '2'

    # Fix wrong links
    sudo rm /etc/alternatives/java_sdk_openjdk;sudo ln -s /usr/lib/jvm/java-1.8.0-openjdk.x86_64 /etc/alternatives/java_sdk_openjdk
    sudo rm /etc/alternatives/java_sdk_openjdk_exports;sudo ln -s /usr/lib/jvm-exports/java-1.8.0-openjdk.x86_64 /etc/alternatives/java_sdk_openjdk_exports

    export JAVA_HOME='/etc/alternatives/java_sdk_openjdk'

fi

# Check java version again
JAVA_VER=$(java -version 2>&1 | sed 's/java version "\(.*\)\.\(.*\)\..*"/\1\2/; 1q')

echo ""
echo "Java version is $JAVA_VER!"
echo "JAVA_HOME: $JAVA_HOME"
echo "JRE_HOME: $JRE_HOME"
echo "PATH: $PATH"
