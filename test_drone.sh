#!/usr/bin/env bash

# drone runs tests in a container w/ a linked redis
# docker has its own convention around env vars for linked containers
# translate that env into what our tests expect
REDIS_URL=$REDIS_PORT_6379_TCP_ADDR:$REDIS_PORT_6379_TCP_PORT \

make test
