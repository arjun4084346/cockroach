aws s3 rb s3://b1 --endpoint-url http://10.247.78.171:9020 --force
s3curl.pl --id=personal --createBucket -- http://10.247.78.171:9020/b1
aws s3 ls s3://b1 --endpoint-url http://10.247.78.171:9020
