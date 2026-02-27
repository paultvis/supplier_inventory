import { parseArgs } from 'util';

// --- 1. PARAMETER PARSING ---
const { values } = parseArgs({
    options: {
        app_id: { type: 'string' },       // Your Linnworks Application ID
        app_secret: { type: 'string' },   // Your Linnworks Application Secret
        token: { type: 'string' },        // Your Linnworks Installation Token
        type: { type: 'string' },         // 'import' or 'export'
        job_name: { type: 'string' },     // The exact name of the job in Linnworks
    },
    strict: false
});

const requiredArgs = ['app_id', 'app_secret', 'token', 'type', 'job_name'];
for (const arg of requiredArgs) {
    if (!values[arg]) {
        console.error(`Missing required parameter: --${arg}`);
        process.exit(1);
    }
}

const jobType = values.type.toLowerCase();
if (jobType !== 'import' && jobType !== 'export') {
    console.error('The --type parameter must be either "import" or "export".');
    process.exit(1);
}

// --- 2. API HELPERS ---

// Helper for URL-encoded form requests (Linnworks prefers this format)
async function fetchLinnworks(url, params = {}, sessionToken = null) {
    const headers = { 'Content-Type': 'application/x-www-form-urlencoded' };
    if (sessionToken) headers['Authorization'] = sessionToken;

    const body = new URLSearchParams(params).toString();

    const response = await fetch(url, { method: 'POST', headers, body });
    const data = await response.json();

    if (!response.ok) {
        throw new Error(`Linnworks API Error: ${response.status} - ${JSON.stringify(data)}`);
    }
    return data;
}

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// --- 3. MAIN EXECUTION ---
async function main() {
    try {
        console.log(`Authenticating with Linnworks...`);
        
        // Step 1: Authenticate
        const authData = await fetchLinnworks(
            'https://api.linnworks.net/api/Auth/AuthorizeByApplication', 
            {
                applicationId: values.app_id,
                applicationSecret: values.app_secret,
                token: values.token
            }
        );

        const sessionToken = authData.Token;
        const serverUrl = authData.Server; // e.g., https://eu-ext.linnworks.net
        console.log(`Authenticated successfully. Routed to server: ${serverUrl}`);

        const isImport = jobType === 'import';
        const getListEndpoint = `${serverUrl}/api/ImportExport/Get${isImport ? 'Imports' : 'Exports'}`;
        const enableEndpoint = `${serverUrl}/api/ImportExport/Enable${isImport ? 'Import' : 'Export'}`;
        const runEndpoint = `${serverUrl}/api/ImportExport/RunNow${isImport ? 'Import' : 'Export'}`;
        const getJobEndpoint = `${serverUrl}/api/ImportExport/Get${isImport ? 'Import' : 'Export'}`;

        // Step 2: Get the list of jobs to find the correct ID
        console.log(`Fetching ${jobType} jobs to find "${values.job_name}"...`);
        const jobsList = await fetchLinnworks(getListEndpoint, {}, sessionToken);
        
        const targetJob = jobsList.find(j => j.Name === values.job_name);
        if (!targetJob) {
            throw new Error(`Could not find an ${jobType} job named "${values.job_name}"`);
        }

        const jobId = targetJob.Id;
        console.log(`Found job ID: ${jobId}. Current Status: ${targetJob.Status}`);

        // Step 3: Enable the job if it is currently disabled
        if (!targetJob.Enabled) {
            console.log(`Job is disabled. Enabling...`);
            await fetchLinnworks(enableEndpoint, { 
                [isImport ? 'importId' : 'exportId']: jobId, 
                enable: true 
            }, sessionToken);
            console.log(`Job enabled.`);
        }

        // Step 4: Trigger the job
        console.log(`Triggering ${jobType} job to run now...`);
        await fetchLinnworks(runEndpoint, { 
            [isImport ? 'importId' : 'exportId']: jobId 
        }, sessionToken);

        // Step 5: Poll the status
        console.log(`Job triggered. Monitoring status...`);
        let isFinished = false;

        while (!isFinished) {
            // Wait 10 seconds between checks to avoid rate limiting
            await sleep(10000); 

            const jobStatusData = await fetchLinnworks(getJobEndpoint, { id: jobId }, sessionToken);
            const status = jobStatusData.Status; // Typically: None, Queued, Running, Completed, Failed
            
            console.log(`Current status: ${status}...`);

            // If the status is NOT Queued and NOT Running, it has finished
            if (status !== 'Queued' && status !== 'Running' && status !== 'Executing') {
                isFinished = true;
                
                if (status === 'Completed' || status === 'None') {
                    console.log(`\n✅ ${jobType} job "${values.job_name}" finished successfully!`);
                } else {
                    console.warn(`\n⚠️ ${jobType} job "${values.job_name}" finished with status: ${status}`);
                }
            }
        }

    } catch (error) {
        console.error(`\n❌ Script failed:`, error.message);
        process.exit(1);
    }
}

main();