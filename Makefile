# You can follow the steps below in order to get yourself a local ODC.
# Once running, you can access a Jupyter environment 
# at 'http://localhost' with password 'secretpassword'

# 1. Start your Docker environment
up:
	docker-compose up

# 2. Prepare the database
initdb:
	docker-compose exec jupyter datacube -v system init

# 3.0 Add metadata definition
metadata:
	docker-compose exec jupyter datacube metadata add \
		https://raw.githubusercontent.com/digitalearthafrica/config/master/products/eo3.yaml

# 3. Add a product definition for landsat level 1
product:
	docker-compose exec jupyter datacube product add \
		/opt/odc/scripts/sentinel-2-stac.yaml

# 3. Index a dataset (just an example, you can change the extents)
index:
	docker-compose exec jupyter bash -c \
		"cd /opt/odc/scripts && python3 ./autoIndex.py \
			--start_date '2020-02-20' \
			--end_date '2020-02-22' \
			--extents '-180,180,-60,60'"

# Some extra commands to help in managing things.
# Rebuild the image
build:
	docker-compose build

# Start an interactive shell
shell:
	docker-compose exec jupyter bash

# Delete everything
clear:
	docker-compose down

# Update S3 template (this is owned by Digital Earth Australia)
upload-s3:
	aws s3 cp cube-in-a-box-cloudformation.yml s3://opendatacube-cube-in-a-box/ --acl public-read

# This section can be used to deploy onto CloudFormation instead of the 'magic link'
create-infra:
	aws cloudformation create-stack \
		--region ap-southeast-2 \
		--stack-name odc-test \
		--template-body file://opendatacube-test.yml \
		--parameter file://parameters.json \
		--tags Key=Name,Value=OpenDataCube \
		--capabilities CAPABILITY_NAMED_IAM

update-infra:
	aws cloudformation update-stack \
		--stack-name odc-test \
		--template-body file://opendatacube-test.yml \
		--parameter file://parameters.json \
		--tags Key=Name,Value=OpenDataCube \
		--capabilities CAPABILITY_NAMED_IAM
