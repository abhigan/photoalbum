import { DynamoDBClient, PutItemCommand, UpdateItemCommand } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";
import { S3Client, HeadObjectCommand, GetObjectCommand } from "@aws-sdk/client-s3";
import { parse } from "@aws-sdk/util-arn-parser";

const s3 = new S3Client({});
const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({}));

export const handler = async (event, context) => {
    console.log(`Received ${event.Records.length} events from SQS`);
    for await (const r of event.Records) {
        await processSqsEvent(r);
    }
};

const processSqsEvent = async (sqsEvent) => {
    const s3Records = JSON.parse(sqsEvent.body);
    console.log(`Received ${s3Records.Records.length} events from s3`);
    for await (const r of s3Records.Records) {
        await processS3Event(r);
    }
};

const processS3Event = async (s3Event) => {
    const bucket = s3Event.s3.bucket.name;
    const key = decodeURIComponent(s3Event.s3.object.key.replace(/\+/g, ' '));

    await handleS3File({
        s3bucketName: bucket,
        s3Key: key
    })
};


export const handleS3File = async ({ s3BucketArn, s3bucketName, s3Key }) => {
    const key = decodeURIComponent(s3Key.replace(/\+/g, ' '));
    const bucketName = s3bucketName || parse(s3BucketArn).resource;

    if (!key.includes('Takeout/Google Photos')) {
        console.log(`Discarding '${key}'. Not in 'Takeout/Google Photos'`);
        return "Not in Google Photos";
    }

    if (key.endsWith(".jpg") || key.endsWith(".jpeg")) {
        return handleMedia(bucketName, key);

    }
    else if (key.endsWith(".json")) {
        return handleMetadata(bucketName, key);

    }
    else {
        console.log(`Discarding '${key}'. Not a jpg`);
        return "Unknown file type";
    }
};

async function handleMedia(bucketName, key, fileTime) {
    // const key = "2021-04/Takeout/Google Photos/Photos from 2020/20200215_181038.jpg";
    const keyFragments = key.split("/");
    const albumName = "Google/" + keyFragments[keyFragments.length - 2];
    
    console.log(`Attempting to locate file '${key}'`)
    // s3File = { LastModified, ETag, ContentType }
    const s3File = await s3.send(
        new HeadObjectCommand({
            Bucket: bucketName,
            Key: key,
        })
    );

    await upsert(s3File.ETag, key, s3File.ContentType, fileTime, albumName);
    
    const albums = [albumName];
    if(!!fileTime) {
        const dt = new Date(fileTime * 1000);
        const dtAlbumName = `Date/${dt.getFullYear()}/${dt.getMonth() + 1}/${dt.getDate()}`;
        albums.push(dtAlbumName);
    }
    await linkToAlbums(s3File.ETag, albums);

    return "jpg";
}

async function handleMetadata(bucketName, key) {
    // { LastModified, ETag, ContentType, Body }
    const s3File = await s3.send(
        new GetObjectCommand({
            Bucket: bucketName,
            Key: key,
        })
    );

    console.log(`Processing metadata at '${key}'`)
    const fileContents = await s3File.Body.transformToString();
    const metadata = JSON.parse(fileContents);
    
    if(! metadata.photoTakenTime) {
        // this file has no photo time stamp. perhaps an album metadata
        return "metadata"    
    }
    
    const photoTakenTime = metadata.photoTakenTime.timestamp

    try {
        const targetKey = key.replace(/(.+)(\/.+)$/, `$1/${metadata.title}`)
        await handleMedia(bucketName, targetKey, photoTakenTime);
    } catch (ex) {
        try {
            const targetKey = key.replace(".json", "");
            await handleMedia(bucketName, targetKey, photoTakenTime);
        } catch (ex) {
            console.error(ex)
            // well, we tried but we could not find the media file referenced by this metadata file
        }
    }

    return "metadata";
}

const linkToAlbums = async (etag, albums) => {
    console.log(`Linking to ${albums.length} albums - ${albums}`);
    
    for await (const albumName of albums) {

        // add to albums
        await dynamo.send(
            new PutItemCommand({
                TableName: "gw.gallery.album.items",
                Item: {
                    AlbumName: { S: albumName },
                    ETag: { S: etag },
                },
            })
        );
        
        // add to album names
        await dynamo.send(
            new PutItemCommand({
                TableName: "gw.gallery.albums",
                Item: {
                    AlbumName: { S: albumName },
                },
            })
        );

        // add to the item itself
        await dynamo.send(
            new UpdateItemCommand({
                Key: {
                    "ETag": {
                        "S": etag
                    }
                },
                ConditionExpression: "attribute_exists(ETag)",
                TableName: "gw.gallery.items",
                UpdateExpression: "SET Albums.#albumName = :true",
                ExpressionAttributeNames: { "#albumName": albumName },
                ExpressionAttributeValues: { ":true": { BOOL: true } },
            })
        );

    }

    return true;
};

export const upsert = async (etag, key, contentType, fileTime, albumName) => {
    // https://docs.aws.amazon.com/apigateway/latest/developerguide/http-api-dynamo-db.html
    for (let i = 0; i < 3; i++) {
        try { // updating 
            await update(etag, key, contentType, fileTime, albumName);
            console.log(`Appended file location for ${etag} : ${key}`);
            return;
        }
        catch (err) {
            if (!err.message.includes('The conditional request failed')) { throw err; }

            // the item does not exist
            await insert(etag);
            console.log(`Added entry for ${etag}`);

            // now retry updating
        }
    }

    // should not come here
    const msg = "For loop exhausted";
    console.err(msg);
    throw msg;
};

async function update(etag, key, contentType, fileTime, albumName) {
    let updateExp = "SET ContentType = :ContentType, Locations.#locName = :true, Albums.#albumName = :true";
    let values = {
        ":ContentType": { S: contentType },
        ":true": { BOOL: true },
    };

    if (!!fileTime) {
        updateExp += ", FileTime = :FileTime";
        values[":FileTime"] = { N: fileTime };
    }

    // update item
    await dynamo.send(
        new UpdateItemCommand({
            ConditionExpression: "attribute_exists(ETag)",
            Key: {
                "ETag": {
                    "S": etag
                }
            },
            TableName: "gw.gallery.items",
            UpdateExpression: updateExp,
            ExpressionAttributeValues: values,
            ExpressionAttributeNames: {
                "#locName": key,
                "#albumName": albumName,
            }
        })
    );
    return true;
};

async function insert(etag) {
    await dynamo.send(
        new PutItemCommand({
            TableName: "gw.gallery.items",
            ConditionExpression: "attribute_not_exists(ETag)",
            Item: {
                ETag: { S: etag },
                Locations: { M: {} },
                Albums: { M: {} },
            },
        })
    );
    return true;
};
