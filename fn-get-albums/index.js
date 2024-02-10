import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import { DynamoDBDocumentClient, ScanCommand } from "@aws-sdk/lib-dynamodb";

const client = new DynamoDBClient({});
const docClient = DynamoDBDocumentClient.from(client);

export const handler = async (event, context) => {
    const command = new ScanCommand({
        TableName: "gw.gallery.albums",
    });

    const response = await docClient.send(command);

    const albums = {
        "google" : {},
        "date": {}
    };

    response.Items.forEach(album => {
        const albumName = album.AlbumName;
        if(albumName.startsWith("Google/")) {
            albums.google[ albumName.replace("Google/", "") ] = true;

        } else if(albumName.startsWith("Date/")) {
            albums.date[ albumName.replace("Date/", "") ] = true;

        } else {
            throw `Unknown prefix '${albumName}'`;
        }
    });

    console.log(JSON.stringify(albums, null, 2));
    return albums;
};