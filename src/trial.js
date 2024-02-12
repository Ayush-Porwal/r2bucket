import fs from "fs";
import mysql from "mysql2";
import {
  S3Client,
  HeadObjectCommand,
  ListObjectsV2Command,
} from "@aws-sdk/client-s3";
import { stringify } from "csv-stringify/sync";

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

function getPictureData() {
  return new Promise((resolve, reject) => {
    connection.connect();

    const query = "SELECT pictureID, currentfolder FROM picture";
    connection.query(query, (error, results) => {
      if (error) {
        reject(error);
      } else {
        const pictureObject = {};
        results.forEach((row) => {
          pictureObject[row.pictureID] = row.currentfolder;
        });
        resolve(pictureObject);
      }
      connection.end();
    });
  });
}

function getPictureExtension() {
  return new Promise((resolve, reject) => {
    connection.connect();

    const query = "SELECT pictureID, extension FROM picture";
    connection.query(query, (error, results) => {
      if (error) {
        reject(error);
      } else {
        const extensionObject = {};
        results.forEach((row) => {
          extensionObject[row.pictureID] = row.extension;
        });
        resolve(extensionObject);
      }
      connection.end();
    });
  });
}

// async function checkPictureExistence(pictureSubfolderMap) {
//   const pictureNotFound = [];
//   const promises = []; // Array to store check promises

//   for (const pictureID in pictureSubfolderMap) {
//     const subFolder = pictureSubfolderMap[pictureID];

//     const pathsToCheck = [
//       `images/${subFolder}/${pictureID}.tif`,
//       `previews/${subFolder}/${pictureID}.tif`,
//       `thumbnails/${subFolder}/${pictureID}.tif`,
//     ];

//     // Create Promises for each path check
//     for (const path of pathsToCheck) {
//       const params = { Bucket: bucketName, Key: path };
//       const command = new HeadObjectCommand(params);

//       promises.push(
//         S3.send(command)
//           .then(() => {})
//           .catch((err) => {
//             if (err.$metadata && err.$metadata.httpStatusCode === 404) {
//               pictureNotFound.push({ pictureID, pathWhereItsNotFound: path });
//               console.log({ pictureID, pathWhereItsNotFound: path });
//             } else {
//               console.error(`Error checking ${path}:`, err);
//             }
//           })
//       );
//     }
//   }

//   await Promise.all(promises);
//   return pictureNotFound;
// }

async function checkPictureExistence(pictureSubfolderMap, extensionObjectMap) {
  const pictureNotFound = [];
  const BATCH_SIZE = 50;
  let filesProcessed = 0;

  const pathChecks = [];

  for (const pictureID in pictureSubfolderMap) {
    const subFolder = pictureSubfolderMap[pictureID];
    const pathsToCheck = [
      `images/${subFolder}/${pictureID}.${extensionObjectMap[pictureID]}`,
      `previews/${subFolder}/${pictureID}.${extensionObjectMap[pictureID]}`,
      `thumbnails/${subFolder}/${pictureID}.${extensionObjectMap[pictureID]}`,
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
            console.log({ pictureID, pathWhereItsNotFound: path });
          } else {
            console.error(`Error checking ${path}:`, err);
          }
        });
    });

    await Promise.all(promises);

    console.log(`Processed ${filesProcessed} files so far...`);
  }
  return pictureNotFound;
}

// Logic to console log folders inside the bucket
// const params = {
//   Bucket: bucketName,
//   Delimiter: "/",
// };

// const command = new ListObjectsV2Command(params);

// try {
//   const data = await S3.send(command);
//   console.log("Folders within the bucket:");
//   data.CommonPrefixes.forEach((folder) => {
//     console.log(folder.Prefix);
//   });
// } catch (err) {
//   console.error("Error listing objects:", err);
// }

// async function findFilePathRecursive(pictureID, currentPath = "") {
//   const params = {
//     Bucket: bucketName,
//     Prefix: currentPath, // Start search from the current path
//     Delimiter: "/", // To list simulated folders
//   };

//   if (currentPath === "/") return null;

//   const command = new ListObjectsV2Command(params);

//   try {
//     const data = await S3.send(command);

//     // console.log(`Response Data for paths ${currentPath}: `, data);

//     // Check for the file in the current "folder"
//     if (data.Contents) {
//       // Check for the file in the current "folder"
//       const fileInCurrentFolder = data.Contents.find(
//         (obj) =>
//           obj.Key.endsWith(`${pictureID}.tiff`) ||
//           obj.Key.endsWith(`${pictureID}.tif`)
//       );
//       if (fileInCurrentFolder) {
//         return fileInCurrentFolder.Key;
//       }
//     }

//     // Recursively search subfolders
//     if (data.CommonPrefixes) {
//       for (const folder of data.CommonPrefixes) {
//         console.log(folder);
//         const filePathInSubfolder = await findFilePathRecursive(
//           pictureID,
//           folder.Prefix
//         );
//         if (filePathInSubfolder) {
//           return filePathInSubfolder;
//         }
//       }
//     }

//     // File not found in this path
//     return null;
//   } catch (err) {
//     console.error("Error searching for file:", err);
//     return null;
//   }
// }

// async function fileExistsInFolder(folderPath, filename) {
//   const params = {
//     Bucket: bucketName,
//     Prefix: folderPath, // Assuming folderPath ends with a '/'
//   };

//   const command = new ListObjectsV2Command(params);

//   try {
//     let allData = [];

//     do {
//       const data = await S3.send(command);
//       allData = allData.concat(data.Contents);
//       command.input.ContinuationToken = data.NextContinuationToken;
//       console.log("allData length is: ", allData.length);
//     } while (command.input.ContinuationToken);

//     const fileExists = allData.some((obj) => {
//       const fileKey = obj.Key;
//       return filename === fileKey.substring(fileKey.lastIndexOf("/") + 1);
//     });
//     return fileExists;
//   } catch (err) {
//     return false;
//   }
// }

// const pictureSubfolderMap = await getPictureData();

// const pictureSubfolderMap = {
//   41682: 0,
// };

// fileExistsInFolder("images/0/", "41682.tif")
//   .then((fileExists) => {
//     if (fileExists) {
//       console.log("File exists!");
//     } else {
//       console.log("File not found.");
//     }
//   })
//   .catch((err) => {
//     console.error("Error encountered:", err);
//   });

// findFilePathRecursive(41682)
//   .then((filePath) => {
//     // ... (Handle found file)
//   })
//   .catch((err) => {
//     // ... (Handle errors)
//   });

console.log("fetching data");
const extensionObjectMap = await getPictureExtension();
const pictureSubfolderMap = await getPictureData();
// const pictureSubfolderMap = {
//   41696: 0,
// };

console.log("looking in r2 bucket");
checkPictureExistence(pictureSubfolderMap, extensionObjectMap)
  .then((missingPictures) => {
    if (missingPictures.length > 0) {
      const csvData = stringify(missingPictures, {
        header: true,
        columns: ["pictureID", "pathWhereItsNotFound"],
      });

      const filename = "missing_pictures.csv";
      fs.writeFileSync(filename, csvData);
    } else {
      console.log("All pictures found.");
    }
  })
  .catch((err) => {
    console.error("Error running the function:", err);
  });

// My Bucket keys
// const accessKeyId = "1a9b063c28014644ad48072de55b0b80";
// const secretAccessKey =
//   "d8c26b0ee06315145aa0e7e632f9ed5f3c923bd7b838a672bb72671c562fbba8";;
// const s3Endpoint =
//   "https://d984c28249dc714f79940c2b1f43e5f0.r2.cloudflarestorage.com";
