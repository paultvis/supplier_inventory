import { parseArgs } from 'util';

// --- 1. PARAMETER PARSING ---
const { values } = parseArgs({
    options: {
        app_id: { type: 'string' },
        app_secret: { type: 'string' },
        token: { type: 'string' },
        type: { type: 'string' },
        job_name: { type: 'string' },
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
async function fetchLinnworks(url, method = 'POST', params = {}, sessionToken = null) {
    const headers = {};
    if (sessionToken) headers['Authorization'] = sessionToken;

    let finalUrl = url;
    let body = undefined;

    // Format parameters correctly for Linnworks
    const searchParams = new URLSearchParams();
    for (const [key, value] of Object.entries(params)) {
        if (value !== undefined && value !== null) {
            searchParams.append(key, value.toString());
        }
    }

    // Route GET params to the URL, and POST params to the Body
    if (method.toUpperCase() === 'GET') {
        const query = searchParams.toString();
        if (query) finalUrl = `${url}?${query}`;
    } else {
        headers['Content-Type'] = 'application/x-www-form-urlencoded';
        body = searchParams.toString();
    }

    const response = await fetch(finalUrl, { method, headers, body });
    
    // Linnworks sometimes returns empty bodies on successful POSTs
    const text = await response.text();
    let data;
    try {
        data = text ? JSON.parse(text) : {};
    } catch(e) {
        data = text;
    }

    if (!response.ok) {
        throw new Error(`Linnworks API Error: ${response.status} - ${text}`);
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
            'POST',
            {
                applicationId: values.app_id,
                applicationSecret: values.app_secret,
                token: values.token
            }
        );

        const sessionToken = authData.Token;
        const serverUrl = authData.Server; 
        console.log(`Authenticated successfully. Routed to server: ${serverUrl}`);

        const isImport = jobType === 'import';
        
        // Corrected Linnworks Endpoints
        const getListEndpoint = `${serverUrl}/api/ImportExport/Get${isImport ? 'ImportList' : 'ExportList'}`;
        const enableEndpoint = `${serverUrl}/api/ImportExport/Enable${isImport ? 'Import' : 'Export'}`;
        const runEndpoint = `${serverUrl}/api/ImportExport/RunNow${isImport ? 'Import' : 'Export'}`;
        const getJobEndpoint = `${serverUrl}/api/ImportExport/Get${isImport ? 'Import' : 'Export'}`;

        // Step 2: Get the list of jobs
        console.log(`Fetching ${jobType} jobs to find "${values.job_name}"...`);
        // The List endpoints are strictly GET requests
        const jobsListResponse = await fetchLinnworks(getListEndpoint, 'GET', {}, sessionToken);
        
        // Linnworks nests the list under "register"
        const list = Array.isArray(jobsListResponse) 
            ? jobsListResponse 
            : (jobsListResponse.register || jobsListResponse.Register || []);
            
        // Job names are stored in "FriendlyName" or "Name" depending on the module
        const targetJob = list.find(j => j.Name === values.job_name || j.FriendlyName === values.job_name);
        
        if (!targetJob) {
            throw new Error(`Could not find an ${jobType} job named "${values.job_name}"`);
        }

        const jobId = targetJob.Id;
        console.log(`Found job ID: ${jobId}. Current Status: ${targetJob.Executing ? 'Running' : (targetJob.IsQueued ? 'Queued' : 'Idle')}`);

        // Step 3: Enable the job if it is currently disabled
        if (!targetJob.Enabled) {
            console.log(`Job is disabled. Enabling...`);
            await fetchLinnworks(enableEndpoint, 'POST', { 
                [isImport ? 'importId' : 'exportId']: jobId, 
                id: jobId, 
                enable: true 
            }, sessionToken);
            console.log(`Job enabled.`);
        }

        // Step 4: Trigger the job
        console.log(`Triggering ${jobType} job to run now...`);
        await fetchLinnworks(runEndpoint, 'POST', { 
            [isImport ? 'importId' : 'exportId']: jobId,
            id: jobId
        }, sessionToken);

        // Step 5: Poll the status
        console.log(`Job triggered. Monitoring status...`);
        let isFinished = false;

        while (!isFinished) {
            await sleep(10000); 

            // The single job endpoint is a GET request and takes the `id` param in the URL
            const jobStatusData = await fetchLinnworks(getJobEndpoint, 'GET', { id: jobId }, sessionToken);
            const statusData = jobStatusData.Register || jobStatusData.register || jobStatusData;
            
            const isQueued = statusData.IsQueued;
            const isExecuting = statusData.Executing;
            
            console.log(`Current state: ${isExecuting ? 'Executing' : (isQueued ? 'Queued' : 'Finished')}...`);

            if (!isQueued && !isExecuting) {
                isFinished = true;
                console.log(`\n✅ ${jobType} job "${values.job_name}" finished successfully!`);
            }
        }

    } catch (error) {
        console.error(`\n❌ Script failed:`, error.message);
        process.exit(1);
    }
}

main();