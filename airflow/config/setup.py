import json
import boto3

# Create a bucket policy
bucket_name = 'estar'
bucket_policy = {
    'Version': '0.1',
    'Statement': [{
        'Sid': 'AddPerm',
        'Effect': 'Allow',
        'Principal': {
            "AWS": "arn:aws:iam::924317839435:group/Developers"
        },
        'Action': "s3:*",
        'Resource': f'arn:aws:s3:::{bucket_name}/*'
    }]
}

# Convert the policy from JSON dict to string
bucket_policy = json.dumps(bucket_policy)

# Set the new policy
s3 = boto3.client('s3')
s3.put_bucket_policy(Bucket=bucket_name, Policy=bucket_policy)