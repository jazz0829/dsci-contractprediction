#!/usr/bin/env bash

INSTALL_COMMAND="sudo python3 -m pip install"
dependencies="numpy==1.16.5 pandas==0.24.2 slackclient==1.3.2 statsmodels==0.10.1 pyarrow==0.12.1 boto3 botocore py4j"

sudo apt-get install -y python3-pip gcc
for dep in $dependencies; do
    $INSTALL_COMMAND $dep
done;

aws s3 cp s3://dt-dsci-contract-prediction-131239767718-eu-west-1/scripts/common/ /home/hadoop/common/ --recursive
sudo chmod -R 777 /opt/

# Push EMR metric for master node
IS_MASTERNODE=false
if grep isMaster /mnt/var/lib/info/instance.json | grep true;
then
    IS_MASTERNODE=true
    instanceid=`wget -q -O - http://169.254.169.254/latest/meta-data/instance-id`
    pDNSName=`wget -q -O - http://169.254.169.254/latest/meta-data/public-hostname`
    azone=`wget -q -O - http://169.254.169.254/latest/meta-data/placement/availability-zone`
    accountid=`curl -s http://169.254.169.254/latest/dynamic/instance-identity/document | grep -oP '"accountId"\s*:\s*"\K[^"]+'`
    region=${azone/%?/}
    aws configure set region $region

    cd ~
    mkdir EMRShutdown
    cd EMRShutdown

    aws s3 cp "s3://aws-logs-$accountid-$region/pushShutDownMetrin.sh" .
    chmod 700 pushShutDownMetrin.sh
    /home/hadoop/EMRShutdown/pushShutDownMetrin.sh --isEMRUsed

    sudo bash -c 'echo "*/5 * * * * root /home/hadoop/EMRShutdown/pushShutDownMetrin.sh --isEMRUsed" >> /etc/crontab'
fi
