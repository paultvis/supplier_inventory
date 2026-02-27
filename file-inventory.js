import { createWriteStream } from 'fs';
import { mkdir, unlink, writeFile } from 'fs/promises';
import { join } from 'path';
import { parseArgs } from 'util';
import { Readable } from 'stream';
import { finished } from 'stream/promises';
import * as XLSX from 'xlsx'; // New Excel parsing library

// --- 1. PARAMETER PARSING ---
const { values } = parseArgs({
    options: {
        out_dir: { type: 'string', default: './smg_downloads' },
        price_url: { type: 'string' },
        inventory_url: { type: 'string' },
        discontinued_url: { type: 'string' },
        http_user: { type: 'string' }, 
        http_pass: { type: 'string' }
    },
    strict: false
});

// --- 2. DOWNLOAD & CONVERT HELPER ---
async function downloadAndConvertFile(url, finalFilename) {
    if (!url) return;

    // Detect if the source is an Excel file based on the URL
    const isExcel = url.toLowerCase().includes('.xlsx') || url.toLowerCase().includes('.xls');
    
    // Set up paths
    const finalPath = join(values.out_dir, finalFilename); // e.g., smg_prices.csv
    const tempPath = join(values.out_dir, `temp_${finalFilename}.xlsx`);
    const downloadPath = isExcel ? tempPath : finalPath;

    console.log(`Starting download: ${url}`);

    const options = { method: 'GET' };
    if (values.http_user && values.http_pass) {
        const authHeader = 'Basic ' + Buffer.from(`${values.http_user}:${values.http_pass}`).toString('base64');
        options.headers = { 'Authorization': authHeader };
    }

    try {
        const response = await fetch(url, options);

        if (!response.ok) {
            throw new Error(`Failed to download from ${url}: ${response.status} ${response.statusText}`);
        }

        // 1. Stream the download to disk
        const fileStream = createWriteStream(downloadPath);
        await finished(Readable.fromWeb(response.body).pipe(fileStream));
        
        // 2. Convert if it is an Excel file
        if (isExcel) {
            console.log(`Converting ${downloadPath} to CSV format...`);
            
            // Read the Excel file into memory
            const workbook = XLSX.readFile(downloadPath);
            
            // Grab the first sheet (assuming the data is on tab 1)
            const firstSheetName = workbook.SheetNames[0];
            const worksheet = workbook.Sheets[firstSheetName];
            
            // Convert that sheet to a CSV string
            const csvData = XLSX.utils.sheet_to_csv(worksheet);
            
            // Write the new CSV file and delete the temporary Excel file
            await writeFile(finalPath, csvData);
            await unlink(downloadPath);
            
            console.log(`✅ Success: Downloaded Excel, converted, and saved to ${finalPath}`);
        } else {
            console.log(`✅ Success: Downloaded directly to ${finalPath}`);
        }

    } catch (error) {
        console.error(`❌ Error processing ${finalFilename}:`, error.message);
    }
}

// --- 3. MAIN EXECUTION ---
async function main() {
    try {
        await mkdir(values.out_dir, { recursive: true });
        console.log(`Ensured output directory exists: ${values.out_dir}\n`);

        const downloadTasks = [];

        // The final filename will ALWAYS be .csv, regardless of the source file type
        if (values.price_url) {
            downloadTasks.push(downloadAndConvertFile(values.price_url, 'smg_prices.csv'));
        }
        if (values.inventory_url) {
            downloadTasks.push(downloadAndConvertFile(values.inventory_url, 'smg_inventory.csv'));
        }
        if (values.discontinued_url) {
            downloadTasks.push(downloadAndConvertFile(values.discontinued_url, 'smg_discontinued.csv'));
        }

        if (downloadTasks.length === 0) {
            console.log('No URLs provided. Please pass at least one URL (e.g., --price_url "http...").');
            return;
        }

        await Promise.all(downloadTasks);
        console.log('\nAll download and conversion tasks completed.');

    } catch (error) {
        console.error('Fatal error in downloader:', error);
    }
}

main();