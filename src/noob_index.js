import fs from "fs";
import csv from "csv-parser";

function compareAndSaveDifferences(file1Path, file2Path, outputFilePath) {
  const results = [];

  // Process the first CSV file
  fs.createReadStream(file1Path)
    .pipe(csv())
    .on("data", (row1) => {
      let found = false;

      // Process the second CSV file for comparison
      fs.createReadStream(file2Path)
        .pipe(csv())
        .on("data", (row2) => {
          if (
            row1.pictureID === row2.pictureID &&
            row1.pathWhereItsNotFound === row2.pathWhereItsNotFound
          ) {
            found = true;
          }
        })
        .on("end", () => {
          // If not found in the second file, save the entry
          if (!found) {
            results.push(row1);
          }
        });
    })
    .on("end", () => {
      // Write differences to the third file
      const writeStream = fs.createWriteStream(outputFilePath);
      const csvWriter = csv({ headers: ["pictureID", "pathWhereItsNotFound"] });
      csvWriter.pipe(writeStream);
      results.forEach((row) => csvWriter.write(row));
      csvWriter.end();
    });
}

compareAndSaveDifferences(
  "missing_pictures.csv",
  "new_corrected_path.csv",
  "missing_pictures_new.csv"
);
