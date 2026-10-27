const { createHmac } = require('node:crypto');
const { logger } = require('./log.cjs');

function createWebhookDispatcher({ store, apps, pollMs = 1000, timeoutMs = 2000, log = logger() }) {
    const appMap = new Map(apps.map(app => [app.id, app]));
    let running = null;
    let closed = false;

    async function deliver(job) {
        const app = appMap.get(job.app);
        const webhook = app?.webhooks.find(candidate => candidate.url === job.endpoint);
        if (!webhook) {
            store.completeWebhookJob(job.id);
            log.warn('Webhook dropped because endpoint is no longer configured', { app_id: job.app, job_id: job.id });
            return;
        }
        try {
            const signature = createHmac('sha256', webhook.secret || app.secret).update(job.body).digest('hex');
            const response = await fetch(job.endpoint, {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                    'X-Pusher-Key': app.key,
                    'X-Pusher-Signature': signature,
                    'X-Pocket-Event-Id': job.event_id,
                },
                body: job.body,
                redirect: 'error',
                signal: AbortSignal.timeout(timeoutMs),
            });
            await response.body?.cancel().catch(() => {});
            if (response.status < 200 || response.status >= 300) throw new Error(`HTTP ${response.status}`);
            store.completeWebhookJob(job.id);
        } catch (error) {
            const reason = error.name === 'TimeoutError' ? 'timeout' : error.message;
            store.failWebhookJob(job.id, reason);
            const failedJob = store.database.prepare('SELECT attempts,status FROM webhook_jobs WHERE id=?').get(job.id);
            log[failedJob?.status === 'dead' ? 'error' : 'warn']('Webhook delivery failed', {
                app_id: job.app,
                job_id: job.id,
                attempt: failedJob?.attempts ?? 1,
                error: reason.slice(0, 120),
            });
        }
    }

    async function pump() {
        if (closed && !store.database.open) return;
        if (running) return running;
        running = (async () => {
            const jobs = store.claimWebhookJobs();
            for (const job of jobs) await deliver(job);
        })().finally(() => { running = null; });
        return running;
    }

    const interval = setInterval(() => {
        pump().catch(error => log.error('Webhook queue failed', { error: error.message }));
    }, pollMs);

    async function close() {
        if (closed) return running;
        closed = true;
        clearInterval(interval);
        if (running) await running;
        await pump();
    }

    return { pump, close };
}

module.exports = { createWebhookDispatcher };
