import boto3
import csv

def lambda_handler(event, context):
    # DynamoDB Table Region
    region = 'us-east-2'
    
    # DynamoDB Table Name
    tableName = 'Country'
    
    # Source Bucket Name to read CSV from
    sourceBucketName = 'country-csv-loader-bucket'
    
    recList = []
    try:            
        s3 = boto3.client('s3')            
        dyndb = boto3.client('dynamodb', region_name=region)
        filename = event['Records'][0]['s3']['object']['key']
        
        # 1. Load file from S3 bucket
        confile= s3.get_object(Bucket=sourceBucketName, Key=filename)
        recList = confile['Body'].read().split('\n')
        firstrecord=True
        csv_reader = csv.reader(recList, delimiter=',', quotechar='"')
        
        # 2. Clear Dynamo DB Table
        table = boto3.resource('dynamodb').Table(tableName)
        
        scan = table.scan()
        with table.batch_writer() as batch:
            for each in scan['Items']:
                batch.delete_item(
                    Key={
                        'Country_Name': each['Country_Name'],
                    }
                )        
        
        # 3. Insert records in Dynamo DB Table
        for row in csv_reader:
            if (firstrecord):
                firstrecord=False
                continue
            continentName = row[0]
            continentCode = row[1]
            countryName = row[2]
            twoLetterCountryCode = row[3]
            threeLetterCountryCode = row[4]
            
            response = dyndb.put_item(
                TableName=tableName,
                Item={
                'Continent_Name' : {'S':continentName},
                'Continent_Code' : {'S':continentCode},
                'Country_Name' : {'S':countryName},
                'Two_Letter_Country_Code' : {'S':twoLetterCountryCode},
                'Three_Letter_Country_Code' : {'S':threeLetterCountryCode}
                }
            )
        print('Data is uploaded successfully!')
    except Exception, e:
        print (str(e))