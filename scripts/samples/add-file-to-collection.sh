#!/bin/bash

fname="$2"
if [ -z "$fname" ]; then
  fname=$(basename $1)
fi

echo "$1"
echo "$fname"

#ESTUARY_TOKEN="whysaccesstoken3"
#EST_HOST="http://localhost:3004"
#EST_HOST="https://api.estuary.tech"
#EST_HOST="https://upload.estuary.tech"
EST_HOST="https://shuttle-4.estuary.tech"
#EST_HOST="https://shuttle-5.estuary.tech"

COLLECTION=""

set -x
#curl --progress-bar -X POST -H "Authorization: Bearer $ESTUARY_TOKEN" -F "data=@$1" -F "name=$fname" $EST_HOST/content/add
curl --progress-bar -X POST -H "Authorization: Bearer $ESTUARY_TOKEN" -F "data=@$1" -F "name=$fname" $EST_HOST/content/add\?collection="$COLLECTION"

