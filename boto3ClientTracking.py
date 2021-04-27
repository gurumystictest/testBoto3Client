import json, boto3

def lambda_handler(event, context):

    client = boto3.client('iam')
    response = client.list_users()
    print ("Full output returned by api call:")
    print (response)
    print ("Formatted response:")
    for item in response["Users"]:
        print('{} has the arn {}'.format(item["UserName"],item["Arn"]))
    return True

def non_confomring_with_global_client():
    sourceDDBClient = boto3.client(
        'dynamodb',
        region_name = SOURCE_REGION,
        aws_access_key_id=SOURCE_ACCESS_KEY,
        aws_secret_access_key=SOURCE_SECRET_KEY,
        aws_session_token=SOURCE_SESSION_TOKEN,
    )
    print('Scanning [AmazonFreeRTOSMetadata]...')

    scanResponse = sourceDDBClient.scan(
        TableName = 'AmazonFreeRTOSMetadata',
        AttributesToGet = ['FRV', 'SCL'])

    allAFRVersions = scanResponse['Items'];

    for afrVersionMetadata in allAFRVersions:
        afrVersionId = afrVersionMetadata['FRV']['S']
        sourceBucketName = afrVersionMetadata['SCL']['M']['bucket']['S']
        sourceKey = afrVersionMetadata['SCL']['M']['key']['S']
        sourceS3VersionId = afrVersionMetadata['SCL']['M']['version']['S']
        sourceETag = afrVersionMetadata['SCL']['M']['eTag']['S']
        print('Downloading AFR {}'.format(afrVersionId))

        localFilePath = './AmazonFreeRTOS.zip'
        sourceS3Client.download_file(
            Bucket = sourceBucketName,
            Key = sourceKey,
            Filename = localFilePath,
            ExtraArgs = { 'VersionId': sourceS3VersionId })

        print('Uploading AFR {}'.format(afrVersionId))
        destS3Client.upload_file(
            Filename = localFilePath,
            Bucket = 'treadstone-public-releases-swangcha',  # REPLACE IT
            Key = afrVersionId + '/AmazonFreeRTOS.zip')

    print('Finished.')


def check_boto3_client() -> None:
    logger.info('Start DSM configuration.')

    region = environment.get_region()
    asg = boto3.client('autoscaling', region_name=region)
    cfn = boto3.client('cloudformation', region_name=region)
    ec2 = boto3.client('ec2', region_name=region)
    ssm = boto3.client('ssm', region_name=region)

    tags = asg.describe_tags(Filters=[
        {
            'Name': 'key',
            'Values': ['Name'],
        },
        {
            'Name': 'value',
            'Values': ['mc-eps-dsm'],
        },
    ])['Tags']
    asg_names = [t['ResourceId'] for t in tags]


def notRecommendedAPI1(bucket, default_next_marker):
    s3_client = boto3.client("s3")

    pagination_continue = True
    next_marker = default_next_marker
    file_keys = []
    # one lambda session at most handle 4000 files due to the timeout limit for lambda function
    counter = 0

    # use list_object pagination API call, max items returned: 1000
    while pagination_continue and counter < 4:
        list_resp = None
        if next_marker is not None:
            list_resp = s3_client.list_objects(
                Bucket=bucket, Marker=next_marker, Prefix="documents"
            )
        else:
            list_resp = s3_client.list_objects(Bucket=bucket, Prefix="documents")

        pagination_continue = list_resp["IsTruncated"]
        if list_resp.get("NextMarker", None) is not None:
            next_marker = list_resp.get("NextMarker", None)
        else:
            next_marker = list_resp["Contents"][-1]["Key"]

        print("Total number in response: {}".format(len(list_resp["Contents"])))
        print("Next marker for pagination call: {}".format(next_marker))

        file_keys += list_resp["Contents"]
        counter += 1

    return file_keys, next_marker

def conforming_s3_in_keyword(self, bucket, key):
    return 'Contents' in boto3.client('s3').list_objects_v2(Bucket=bucket, Prefix=key)


def conforming_s3_if_branch(*, s3_prefix=None, url_prefix=None):
    s3 = boto3.client('s3')

    while prefix_list:
        root_prefix, continuation_token = prefix_list[0]
        if continuation_token is not None:
            res = s3.list_objects_v2(Bucket=root_bucket, Prefix=root_prefix, ContinuationToken=continuation_token)
        else:
            res = s3.list_objects_v2(Bucket=root_bucket, Prefix=root_prefix, )
        if 'NextContinuationToken' in res:
            # when there are more than 1000 object in the root_prefix
            logging.debug('continue token: %s', res['NextContinuationToken'])
            prefix_list.append((root_prefix, res['NextContinuationToken']))

        if 'Contents' in res:
            for one in res['Contents']:
                p = one['Key']
                fprefix, ext = os.path.splitext(p)
                if ext == '.csv':
                    csv_files.add('s3://{}/{}'.format(root_bucket, p))

        if 'CommonPrefixes' in res:
            for one in res['CommonPrefixes']:
                p = one['Prefix']
                ck = ct_default
                prefix_list.append((p, ck))
