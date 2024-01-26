import { S3Client, HeadObjectCommand, GetObjectCommand } from "@aws-sdk/client-s3";
import { parse } from "@aws-sdk/util-arn-parser";
import { upsert, linkToAlbums } from "./Dynamodb.js";

const s3 = new S3Client({});

export const s3batchTaskhandler = async (task) => {
    return await handleS3File({
        s3BucketArn: task.s3BucketArn,
        s3Key: task.s3Key,
    });
}

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

    const fileContents = await s3File.Body.transformToString();
    const metadata = JSON.parse(fileContents);
    const photoTakenTime = metadata.photoTakenTime.timestamp;
    const targetKey = key.replace(".json", "");

    await handleMedia(bucketName, targetKey, photoTakenTime);

    return "metadata";
}
