import fs from "fs";
import mysql from "mysql2";
import csv from "csv-parser";

const connection = mysql.createConnection({
  host: "localhost",
  user: "root",
  password: "yourpassword",
  database: "yourdb",
});

connection.connect();

const createTableQuery = `
  CREATE TABLE IF NOT EXISTS updatedpictures (
    pictureID int NOT NULL,
    type varchar(20) NOT NULL,
    currentfolder int NOT NULL,
    extension varchar(15) NOT NULL
  ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
`;

connection.query(createTableQuery, (err, result) => {
  if (err) {
    console.error("Error creating table:", err);
  } else {
    console.log('Table "updatedpictures" created or already exists');
  }
});

fs.createReadStream("new_corrected_path.csv")
  .pipe(csv())
  .on("data", (row) => {
    const query = `INSERT INTO updatedpictures (pictureID, type, currentfolder, extension) VALUES (${
      row.pictureID
    }, '${row.newPath.split("/")[0]}', '${row.newPath.split("/")[1]}', '${
      row.newPath.split("/")[2].split(".")[1]
    }');`;
    console.log("inserting data...");
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
