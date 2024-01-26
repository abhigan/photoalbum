import { DynamoDBClient, PutItemCommand, UpdateItemCommand } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient } from "@aws-sdk/lib-dynamodb";

const dynamo = DynamoDBDocumentClient.from(new DynamoDBClient({}));

export const linkToAlbums = async (etag, albums) => {
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
