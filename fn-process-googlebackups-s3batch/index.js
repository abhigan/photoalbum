import { s3batchTaskhandler } from "./S3.js";

export const handler = async (event, context) => {
    console.log(`Starting job ${event.job.id} with ${event.tasks.length} tasks`);

    const results = [];

    for await (const task of event.tasks) {
        console.log(`Procecssing ${task.s3Key}`);

        const res = await s3batchTaskhandler(task);

        results.push({
            "taskId": task.taskId,
            "resultCode": "Succeeded",
            "resultString": res,
        });
    }

    return {
        "invocationSchemaVersion": "1.0",
        "treatMissingKeysAs": "PermanentFailure",
        "invocationId": event.invocationId,
        "results": results,
    }
};
