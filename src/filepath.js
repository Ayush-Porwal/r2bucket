import fs from "fs";
import { S3Client, ListObjectsV2Command } from "@aws-sdk/client-s3";

const bucketName = "wrparchive";
const accessKeyId = "52d2a666e993b08710c56164c930032d";
const secretAccessKey =
  "4a8300b620494f1b3a506c61f70fd43a4c5d6fe88e6105db373f383f0965543d";
const s3Endpoint =
  "https://34f62f615789b088af9130d959f70465.r2.cloudflarestorage.com";

const S3 = new S3Client({
  region: "auto",
  endpoint: s3Endpoint,
  credentials: {
    accessKeyId: accessKeyId,
    secretAccessKey: secretAccessKey,
  },
});

async function listAllObjects() {
  const params = {
    Bucket: bucketName,
  };

  let allObjects = [];

  try {
    let isTruncated = true;
    let continuationToken = null;

    while (isTruncated) {
      if (continuationToken) {
        params.ContinuationToken = continuationToken;
      }

      const { Contents, IsTruncated, NextContinuationToken } = await S3.send(
        new ListObjectsV2Command(params)
      );

      allObjects.push(...Contents);

      console.log("Total files: ", allObjects.length);

      isTruncated = IsTruncated;
      continuationToken = NextContinuationToken;
    }

    return allObjects;
  } catch (err) {
    console.error("Error listing objects:", err);
    throw err;
  }
}

function objectsToCSV(objects) {
  let csvData = "Key,Size\n";

  objects.forEach((obj) => {
    csvData += `${obj.Key},${obj.Size}\n`;
  });

  return csvData;
}

function saveToCSV(csvData, fileName) {
  fs.writeFileSync(fileName, csvData, "utf8");
  console.log(`CSV file saved as ${fileName}`);
}

async function main() {
  try {
    const objects = await listAllObjects();
    const csvData = objectsToCSV(objects);

    saveToCSV(csvData, "files_path.csv");
  } catch (err) {
    console.error("Error:", err);
  }
}

main();
