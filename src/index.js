import fs from "fs";
import mysql from "mysql2";
import { stringify } from "csv-stringify/sync";
import { S3Client, HeadObjectCommand } from "@aws-sdk/client-s3";

const bucketName = "wrparchive";
const accessKeyId = "52d2a666e993b08710c56164c930032d";
const secretAccessKey =
  "4a8300b620494f1b3a506c61f70fd43a4c5d6fe88e6105db373f383f0965543d";
const s3Endpoint =
  "https://34f62f615789b088af9130d959f70465.r2.cloudflarestorage.com";

const connection = mysql.createConnection({
  host: "localhost",
  user: "root",
  password: "Cengage$29",
  database: "wrptest_2",
});

const S3 = new S3Client({
  region: "auto",
  endpoint: s3Endpoint,
  credentials: {
    accessKeyId: accessKeyId,
    secretAccessKey: secretAccessKey,
  },
});

function getPictureDataAndExtension() {
  return new Promise((resolve, reject) => {
    connection.connect();

    const query = `
          SELECT pictureID, currentfolder, extension 
          FROM picture
      `;

    connection.query(query, (error, results) => {
      if (error) {
        reject(error);
      } else {
        const extensionObject = {};
        const pictureObject = {};
        results.forEach((row) => {
          extensionObject[row.pictureID] = row.extension;
          pictureObject[row.pictureID] = row.currentfolder;
        });
        resolve([pictureObject, extensionObject]);
      }
    });
    connection.end();
  });
}

async function checkPictureExistence(pictureSubfolderMap, extensionObjectMap) {
  const pictureNotFound = [];
  const failedPictureIds = new Set();
  const BATCH_SIZE = 50;
  let filesProcessed = 0;

  const pathChecks = [];

  for (const pictureID in pictureSubfolderMap) {
    const subFolder = pictureSubfolderMap[pictureID];
    const pathsToCheck = [
      `images/${subFolder}/${pictureID}.${extensionObjectMap[pictureID]}`,
      `previews/${subFolder}/${pictureID}_s.jpeg`,
      `thumbnails/${subFolder}/${pictureID}_t.jpeg`,
    ];

    pathsToCheck.forEach((path) => {
      pathChecks.push({ pictureID, path });
    });
  }

  for (let i = 0; i < pathChecks.length; i += BATCH_SIZE) {
    const batch = pathChecks.slice(i, i + BATCH_SIZE);
    const promises = batch.map(({ pictureID, path }) => {
      const params = { Bucket: bucketName, Key: path };
      const command = new HeadObjectCommand(params);

      filesProcessed++;

      return S3.send(command)
        .then(() => {})
        .catch((err) => {
          if (err.$metadata && err.$metadata.httpStatusCode === 404) {
            pictureNotFound.push({ pictureID, pathWhereItsNotFound: path });
          } else {
            // network error or cloudflare errors, cant do anything about these except run the job again for these ids
            console.error(`Error checking ${path}:`, JSON.stringify(err));
            failedPictureIds.add(pictureID);
          }
        });
    });

    await Promise.all(promises);

    console.log(`Processed ${filesProcessed} files so far...`);
  }
  return [pictureNotFound, failedPictureIds];
}

console.log("Fetching data from db");

// const [pictureSubfolderMap, extensionObjectMap] =
//   await getPictureDataAndExtension();

const extensionObjectMap = {
  36111: "tif",
  36173: "tif",
  36340: "tif",
  36415: "tif",
  36435: "tif",
  36438: "tif",
  36501: "tif",
  36528: "tif",
  36675: "tif",
  36726: "tif",
  36740: "tif",
  36960: "tif",
  38221: "tif",
  38239: "tif",
  38280: "tif",
  38356: "tif",
  38422: "tif",
  38513: "tif",
  38671: "tif",
};

const pictureSubfolderMap = {
  36111: 1012,
  36173: 1012,
  36340: 1212,
  36415: 1312,
  36435: 1312,
  36438: 1312,
  36501: 1397,
  36528: 1412,
  36675: 1513,
  36726: 1613,
  36740: 1613,
  36960: 1813,
  38221: 3106,
  38239: 3113,
  38280: 3113,
  38356: 3213,
  38422: 3313,
  38513: 3402,
  38671: 3513,
};

console.log("Looking for pictures path in r2 bucket");
checkPictureExistence(pictureSubfolderMap, extensionObjectMap)
  .then(([missingPictures, failedPictureIds]) => {
    if (missingPictures.length > 0 || failedPictureIds.size > 0) {
      if (missingPictures.length > 0) {
        const missingPicturesCsvData = stringify(missingPictures, {
          header: true,
          columns: ["pictureID", "pathWhereItsNotFound"],
        });
        fs.writeFileSync("missing_pictures.csv", missingPicturesCsvData);
      } else {
        console.log("No pictureIDs are at incorrect path");
      }

      if (failedPictureIds.size > 0) {
        const failedPictureIdsCsvData = stringify(
          Array.from(failedPictureIds).map((id) => ({ pictureID: id })),
          { header: true }
        );
        fs.writeFileSync("failed_picture_ids.csv", failedPictureIdsCsvData);
      } else {
        console.log("No pictureIDs failed");
      }
    } else {
      console.log("All pictures found.");
    }
  })
  .catch((err) => {
    console.error("Error running the function:", err);
  });
