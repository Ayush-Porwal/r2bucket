import fs from "fs";
import mysql from "mysql2";
import csv from "csv-parser";

// SQL to create table, manually done in db
// CREATE TABLE `updatedpictures` (
//   `pictureID` int NOT NULL,
//   `type` varchar(20) NOT NULL,
//   `currentfolder` int NOT NULL,
//   `extension` varchar(15) NOT NULL
// ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

const connection = mysql.createConnection({
  host: "localhost",
  user: "root",
  password: "password",
  database: "wrptest_2",
});

connection.connect();

fs.createReadStream("new_corrected_path.csv")
  .pipe(csv())
  .on("data", (row) => {
    const query = `INSERT INTO updatedpictures (pictureID, type, currentfolder, extension) VALUES (${
      row.pictureID
    }, '${row.newPath.split("/")[0]}', '${row.newPath.split("/")[1]}', '${
      row.newPath.split("/")[2].split(".")[1]
    }');`;
    connection.query(query, (err, result) => {
      if (err) {
        console.error("Error during insertion:", err);
      }
    });
  })
  .on("end", () => {
    console.log("CSV file processed");
    connection.end();
  });
