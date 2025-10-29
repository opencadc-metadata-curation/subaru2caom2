#!/bin/bash

COLLECTION="subaru"
IMAGE="opencadc-metadata-curation/${COLLECTION}2caom2"

echo "Get a proxy certificate"
cp $HOME/.ssl/cadcproxy.pem ./ || exit $?

echo "Get image ${IMAGE}"
docker pull ${IMAGE}

echo "Run image ${IMAGE}"
docker run --rm --name ${COLLECTION}_remote -v ${PWD}:/usr/src/app/ ${IMAGE} ${COLLECTION}_run_remote || exit $?

date
exit 0
