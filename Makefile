# You can follow the steps below in order to get yourself a local ODC.
# Once running, you can access a Jupyter environment 
# at 'http://localhost' with password 'secretpassword'

# 1. Get the pathrows file
download-pathrows-file:
	wget https://landsat.usgs.gov/sites/default/files/documents/WRS2_descending.zip -O ./data/wrs2_descending.zip

# 2. Start your Docker environment
up:
	docker-compose up

# 3. Prepare the database
initdb:
	docker-compose exec jupyter datacube -v system init
	docker-compose exec jupyter datacube product add /opt/odc/docs/config_samples/dataset_types/ls_usgs.yaml

# 4. Index a dataset (just an example, you can change the extents)
index:
	# Note that you need environment variables ODC_ACCESS_KEY and ODC_SECRET_KEY set.
	# These need to be valid AWS keys. KEEP THEM SECRET, KEEP THEM SAFE!

	docker-compose exec jupyter bash -c \
		"cd /opt/odc/scripts && python3 ./autoIndex.py \
			-p '/opt/odc/data/wrs2_descending.zip' \
			-e '146.30,146.83,-43.54,-43.20'"

# Some extra commands to help in managing things.
# Rebuild the image
build:
	docker-compose build

# Start an interactive shell
shell:
	docker-compose exec jupyter bash

# Delete everything
clear:
	docker-compose stop
	docker-compose rm -fs

# Blow it all away and start again. First start the stack with `make up`
load-from-scratch: initdb download-pathrows-file index

metadata-usgs-c2:
	docker-compose exec jupyter \
		datacube metadata add /opt/odc/scripts/config/products/eo3_landsat_ard.odc-type.yaml

product-usgs-c2:
	docker-compose exec jupyter \
		datacube product add /opt/odc/scripts/config/products/usgs-level2-collection2-sample.odc-product.yaml

index-usgs-c2-one:
	docker-compose exec jupyter \
		datacube dataset add s3://deafrica-collection2-testing/nigeria/usgs_ls8c_level2_2/188/053/2018/05/01/usgs_ls8c_level2_2-0-20190821_188053_2018-05-01.odc-metadata.yaml

product-alos:
	docker-compose exec jupyter \
		datacube product add https://raw.githubusercontent.com/digitalearthafrica/config/master/products/alos_palsar.yaml

index-alos-one:
	docker-compose exec jupyter \
		datacube dataset add s3://test-results-deafrica-staging-west/alos/2017/N00E030/N00E030_2017.yaml

index-alos-2017:
	docker-compose exec jupyter \
		bash -c "aws s3 ls s3://deafrica-data/jaxa/alos_palsar_mosaic/2017/ --recursive \
			| grep yaml | awk '{print \$$4}' \
			| xargs -n1 -I {} datacube dataset add s3://deafrica-data/{}"

index-alos-2007:
	docker-compose exec jupyter \
		bash -c "aws s3 ls s3://test-results-deafrica-staging-west/alos/2007/ --recursive \
			| grep yaml | awk '{print \$$4}' \
			| xargs -n1 -I {} datacube dataset add s3://test-results-deafrica-staging-west/{}"


# Update S3 template (this is owned by FrontierSI)
update-s3:
	aws s3 cp opendatacube-test.yml s3://cubeinabox/ --acl public-read

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
