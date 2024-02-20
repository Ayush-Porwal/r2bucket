import fs from "fs";
import mysql from "mysql2";
import csv from "csv-parser";

const connection = mysql.createConnection({
  host: "localhost",
  user: "root",
  password: "Cengage$29",
  database: "wrptest_2",
});

connection.connect();

// const createTableQuery = `
//   CREATE TABLE IF NOT EXISTS updatedpicturessample (
//     pictureID int NOT NULL,
//     type varchar(20) NOT NULL,
//     currentfolder int NOT NULL,
//     extension varchar(15) NOT NULL
//   ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
// `;

// connection.query(createTableQuery, (err, result) => {
//   if (err) {
//     console.error("Error creating table:", err);
//   } else {
//     console.log('Table "updatedpicturessample" created or already exists');
//   }
// });

// fs.createReadStream("sample.csv")
//   .pipe(csv())
//   .on("data", (row) => {
//     const query = `INSERT INTO updatedpicturessample (pictureID, type, currentfolder, extension) VALUES (${
//       row.pictureID
//     }, '${row.newPath.split("/")[0]}', '${row.newPath.split("/")[1]}', '${
//       row.newPath.split("/")[2].split(".")[1]
//     }');`;
//     console.log("inserting data...");
//     connection.query(query, (err, result) => {
//       if (err) {
//         console.error("Error during insertion:", err);
//       }
//     });
//   })
//   .on("end", () => {
//     console.log("CSV file processed");
//     connection.end();
//   });

const createTableQuery = `
  CREATE TABLE IF NOT EXISTS correctedpaths (
    pictureID int NOT NULL,
    pathWhereItsNotFound varchar(255) NOT NULL,
    newPath varchar(255) NOT NULL
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
`;

connection.query(createTableQuery, (err, result) => {
  if (err) {
    console.error("Error creating table:", err);
  } else {
    console.log('Table "correctedpaths" created or already exists');
  }
});

function processAndInsertCSV(filePath, tableName) {
  const insertQuery = `INSERT INTO ${tableName} (pictureID, pathWhereItsNotFound, newPath) VALUES ?`;

  const data = [];
  fs.createReadStream(filePath)
    .pipe(csv())
    .on("data", (row) => {
      data.push([row.pictureID, row.pathWhereItsNotFound, row.newPath]);
    })
    .on("end", () => {
      connection.query(insertQuery, [data], (err, result) => {
        if (err) {
          console.error("Error inserting data:", err);
        } else {
          console.log(
            `Successfully inserted ${result.affectedRows} rows into ${tableName}`
          );
        }
      });
    });
}

processAndInsertCSV("new_corrected_path.csv", "correctedpaths");

// connection.end();
