set -x
export image_repo=magma/
export image_name=parallax_service_httpout
export image_tag=1.0.3
export local_ip=localhost

docker build ./ -t $image_repo$image_name:$image_tag

docker run --net host -it --rm \
  --name $image_name \
  -e IMAGE_SERVICE_URL="" \
  -e REQUEST_HANDLER="src.RequestsHandlers.httpout.HttpOutRequestsHandler" \
  -e SERVICE_URL="" \
  -e CPU_BOUNDING_BOX_THRESHOLD="1" \
  -e VERSION="sandboxing" \
  -e SERVICE_TIMEOUT="30" \
  -e IMAGE_SERVICE_TIMEOUT="10" \
  -e LOGSTASH_ENABLE="False" \
  -e TRACING_ENABLE="True" \
  -e SERVICE_NAME="httpOut" \
  -e CONSUME_QUEUE="httpOut" \
  $image_repo$image_name:$image_tag
