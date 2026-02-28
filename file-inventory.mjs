import { createWriteStream } from 'fs';
import { mkdir, unlink, writeFile, readFile as readLocalFile } from 'fs/promises';
import { join } from 'path';
import { parseArgs } from 'util';
import { Readable } from 'stream';
import { finished } from 'stream/promises';
import * as XLSX from 'xlsx';

// --- 1. PARAMETER PARSING ---
const { values } = parseArgs({
    options: {
        supplier_name: { type: 'string' }, // New parameter for the file prefix
        out_dir: { type: 'string', default: './downloads' },
        price_url: { type: 'string' },
        inventory_url: { type: 'string' },
        discontinued_url: { type: 'string' },
        http_user: { type: 'string' }, 
        http_pass: { type: 'string' }
    },
    strict: false
});

// Validate the supplier name
if (!values.supplier_name) {
    console.error('Missing required parameter: --supplier_name (e.g., --supplier_name "SMG Europe")');
    process.exit(1);
}

// Format the prefix: Replace spaces with underscores
const filePrefix = values.supplier_name.replace(/ /g, '_');

// --- 2. DOWNLOAD & CONVERT HELPER ---
async function downloadAndConvertFile(url, finalFilename) {
    if (!url) return;

    const isExcel = url.toLowerCase().includes('.xlsx') || url.toLowerCase().includes('.xls');
    
    const finalPath = join(values.out_dir, finalFilename); 
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

        const fileStream = createWriteStream(downloadPath);
        await finished(Readable.fromWeb(response.body).pipe(fileStream));
        
        if (isExcel) {
            console.log(`Converting ${downloadPath} to CSV format...`);
            
            const fileBuffer = await readLocalFile(downloadPath);
            const workbook = XLSX.read(fileBuffer, { type: 'buffer' });
            
            const firstSheetName = workbook.SheetNames[0];
            const worksheet = workbook.Sheets[firstSheetName];
            
            const csvData = XLSX.utils.sheet_to_csv(worksheet);
            
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

        // Safely check which URLs were provided and apply the dynamic prefix
        if (values.price_url) {
            downloadTasks.push(downloadAndConvertFile(values.price_url, `${filePrefix}_prices.csv`));
        }
        if (values.inventory_url) {
            downloadTasks.push(downloadAndConvertFile(values.inventory_url, `${filePrefix}_inventory.csv`));
        }
        if (values.discontinued_url) {
            downloadTasks.push(downloadAndConvertFile(values.discontinued_url, `${filePrefix}_discontinued.csv`));
        }

        if (downloadTasks.length === 0) {
            console.log('No URLs provided. Please pass at least one URL.');
            return;
        }

        await Promise.all(downloadTasks);
        console.log('\nAll download and conversion tasks completed.');

    } catch (error) {
        console.error('Fatal error in downloader:', error);
    }
}

main();