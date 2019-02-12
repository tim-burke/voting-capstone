# Shell script to download our data from S3 - currently assumes AWSCLI is configured
# run "chmod 755 download_data.sh" to make this file executable


# Download NC raw zipfile data
aws s3 cp s3://capstone.raw.data/North_Carolina/ncvhis_Statewide.zip ../../data/raw/ncvhis_Statewide.zip
aws s3 cp s3://capstone.raw.data/North_Carolina/ncvoter_Statewide.zip ../../data/raw/ncvoter_Statewide.zip

# Unpack the zip files into capstone/data/raw/
unzip -d ../../data/raw/ ../../data/raw/ncvhis_Statewide.zip
unzip -d ../../data/raw/ ../../data/raw/ncvoter_Statewide.zip

