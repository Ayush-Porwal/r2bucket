import fs from "fs";
import { S3Client, PutObjectCommand } from "@aws-sdk/client-s3";
import csv from "csv-parser";

// My Bucket keys
const bucketName = "newbucket";
const accessKeyId = "1a9b063c28014644ad48072de55b0b80";
const secretAccessKey =
  "d8c26b0ee06315145aa0e7e632f9ed5f3c923bd7b838a672bb72671c562fbba8";
const s3Endpoint =
  "https://d984c28249dc714f79940c2b1f43e5f0.r2.cloudflarestorage.com";

const S3 = new S3Client({
  region: "auto",
  endpoint: s3Endpoint,
  credentials: {
    accessKeyId: accessKeyId,
    secretAccessKey: secretAccessKey,
  },
});

function shouldUploadFile(row, fileName) {
  return (
    row.pathWhereItsNotFound.includes("images") &&
    row.pictureID === fileName.split(".")[0]
  );
}

async function uploadFile(filePath, fileName) {
  const fileStream = fs.createReadStream(filePath);

  const uploadParams = {
    Bucket: bucketName,
    Key: fileName,
    Body: fileStream,
    ContentType: "image/tiff",
  };

  try {
    const data = await S3.send(new PutObjectCommand(uploadParams));

    if (data.$metadata.httpStatusCode === 200) {
      console.log(`File ${fileName} uploaded successfully`);
    } else {
      console.log(`Failed to upload the file: ${fileName}`);
    }
  } catch (err) {
    console.error("Error uploading file:", err);
  }
}

function processCSV(folderPath, csvFilePath) {
  fs.createReadStream(csvFilePath)
    .pipe(csv({ separator: ";" }))
    .on("data", (row) => {
      const files = fs.readdirSync(folderPath);
      files.forEach((file) => {
        if (shouldUploadFile(row, file)) {
          uploadFile(`${folderPath}/${file}`, row.pathWhereItsNotFound);
        }
      });
    })
    .on("end", () => {
      console.log(
        "CSV file processed successfully... file are still uploading, please do not close the application, wait for process to finish"
      );
    })
    .on("error", (err) => {
      console.error("Error processing CSV file:", err);
    });
}

function main() {
  const csvFilePath = "sample_new_missing_pictures.csv";
  const folderPath = "tifs";
  processCSV(folderPath, csvFilePath);
}

main();
