#!/bin/sh

java -Djava.rmi.server.randomIDs=true -Djava.rmi.server.disableHttp=true -Djava.library.path=lib -Djava.security.manager -Djava.security.auth.login.config=etc/login.config -Djava.security.policy=etc/policy -Dlog4j.configuration=file:./etc/log4j.properties -jar jdcp-server.jar
