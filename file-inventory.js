import { createWriteStream } from 'fs';
import { mkdir } from 'fs/promises';
import { join } from 'path';
import { parseArgs } from 'util';
import { Readable } from 'stream';
import { finished } from 'stream/promises';

// --- 1. PARAMETER PARSING ---
const { values } = parseArgs({
    options: {
        out_dir: { type: 'string', default: './smg_downloads' }, // Where to save the files
        price_url: { type: 'string' },
        inventory_url: { type: 'string' },
        discontinued_url: { type: 'string' },
        // Add auth parameters just in case they are behind a basic HTTP login
        http_user: { type: 'string' }, 
        http_pass: { type: 'string' }
    },
    strict: false
});

// --- 2. DOWNLOAD HELPER ---
async function downloadFile(url, filename) {
    if (!url) return; // Skip if URL wasn't provided

    const destPath = join(values.out_dir, filename);
    console.log(`Starting download: ${filename}`);

    const options = { method: 'GET' };

    // If the files are protected by Basic Auth, attach the header
    if (values.http_user && values.http_pass) {
        const authHeader = 'Basic ' + Buffer.from(`${values.http_user}:${values.http_pass}`).toString('base64');
        options.headers = { 'Authorization': authHeader };
    }

    try {
        const response = await fetch(url, options);

        if (!response.ok) {
            throw new Error(`Failed to download ${filename}: ${response.status} ${response.statusText}`);
        }

        // Pipe the web stream directly to a file stream to keep memory usage tiny
        const fileStream = createWriteStream(destPath);
        await finished(Readable.fromWeb(response.body).pipe(fileStream));
        
        console.log(`✅ Success: Saved to ${destPath}`);
    } catch (error) {
        console.error(`❌ Error downloading ${filename}:`, error.message);
    }
}

// --- 3. MAIN EXECUTION ---
async function main() {
    try {
        // Ensure the target directory exists
        await mkdir(values.out_dir, { recursive: true });
        console.log(`Ensured output directory exists: ${values.out_dir}\n`);

        // Run all provided downloads concurrently for maximum speed
        const downloadTasks = [];

        if (values.price_url) {
            downloadTasks.push(downloadFile(values.price_url, 'smg_prices.csv'));
        }
        if (values.inventory_url) {
            downloadTasks.push(downloadFile(values.inventory_url, 'smg_inventory.csv'));
        }
        if (values.discontinued_url) {
            downloadTasks.push(downloadFile(values.discontinued_url, 'smg_discontinued.csv'));
        }

        if (downloadTasks.length === 0) {
            console.log('No URLs provided. Please pass at least one URL (e.g., --price_url "http...").');
            return;
        }

        await Promise.all(downloadTasks);
        console.log('\nAll download tasks completed.');

    } catch (error) {
        console.error('Fatal error in downloader:', error);
    }
}

main();