// src/scheduler.js
import cron from 'node-cron';
import { pool } from './db.js';
import { exec } from 'child_process';
import dotenv from 'dotenv';

dotenv.config();

const scheduledJobs = new Map();
const VERBOSE = process.env.VERBOSE_LOGS === 'true';

// Helper log functions
const log = (...args) => { if (VERBOSE) console.log(...args); };
const info = (...args) => console.log(...args);
const warn = (...args) => console.warn(...args);
const error = (...args) => console.error(...args);

export async function scheduleJob(job, runNow = false) {
  const runJob = async () => {
    const { id, command, name } = job;
    let runId;
    const startTimestamp = new Date().toISOString();
    info(` [${startTimestamp}] Starting job #${id}: "${name}"`);
    log(`   ├─ Command: ${command}`);

    try {
      // Record job start
      const start = await pool.query(
        'INSERT INTO job_runs (job_id, status, started_at) VALUES ($1, $2, NOW()) RETURNING id',
                                     [id, 'running']
      );
      runId = start.rows[0].id;
      log(`   ├─ Logged new job run ID: ${runId}`);

      const startTime = Date.now();

      await new Promise((resolve, reject) => {
        exec(command, { timeout: 60000 }, async (err, stdout, stderr) => {
          const output = (stdout || '') + (stderr || '');
          const status = err ? 'error' : 'success';
          const finished_at = new Date();
          const duration = ((Date.now() - startTime) / 1000).toFixed(2);

          try {
            await pool.query(
              'UPDATE job_runs SET status=$1, output=$2, finished_at=$3 WHERE id=$4',
              [status, output.trim() || '(no output)', finished_at, runId]
            );

            info(`✅ [${new Date().toISOString()}] Job #${id} "${name}" completed with status: ${status.toUpperCase()}`);
            log(`   ├─ Duration: ${duration}s`);
            log(`   ├─ Finished at: ${finished_at.toISOString()}`);
            log(`   └─ Output: ${output.trim() || '(no output)'}`);
          } catch (dbErr) {
            error(`   ❌ Failed to update job run #${runId}:`, dbErr.message);
          }

          if (err) reject(err);
          else resolve();
        });
      });
    } catch (err) {
      error(`❌ [${new Date().toISOString()}] Job "${name}" (id=${id}) failed: ${err.message}`);
      if (runId) {
        await pool.query(
          'UPDATE job_runs SET status=$1, output=$2, finished_at=NOW() WHERE id=$3',
                         ['error', err.message, runId]
        );
        log(`   └─ Recorded error in job_runs table for run ID ${runId}`);
      }
    }
  };

  if (runNow) {
    info(`⚡ Running job #${job.id} "${job.name}" immediately on manual trigger`);
    return runJob();
  }

  // Normal scheduled job
  if (cron.validate(job.schedule)) {
    const task = cron.schedule(job.schedule, runJob);
    scheduledJobs.set(job.id, task);
    info(`⏰ Scheduled job #${job.id}: "${job.name}" (${job.schedule})`);
  } else {
    warn(`⚠️ Invalid cron expression for job #${job.id}: ${job.schedule}`);
  }
}

export function cancelJob(id) {
  const job = scheduledJobs.get(id);
  if (job) {
    job.stop();
    scheduledJobs.delete(id);
    info(`🛑 Canceled job #${id} and removed it from the schedule`);
  } else {
    warn(`⚠️ Tried to cancel non-existent job #${id}`);
  }
}

export async function initScheduler() {
  info('🔄 Initializing scheduler and loading jobs from database...');
  const result = await pool.query('SELECT * FROM jobs');
  let loaded = 0;
  let skipped = 0;

  for (const job of result.rows) {
    if (job.status === 'paused') {
      log(`⏸️  Skipping paused job: "${job.name}" (id=${job.id})`);
      skipped++;
      continue;
    }

    await scheduleJob(job);
    loaded++;
  }

  info(`✅ Scheduler initialization complete.`);
  info(`   ├─ Loaded ${loaded} active jobs`);
  info(`   └─ Skipped ${skipped} paused jobs`);
}

/**
 * Cleanup old job runs (older than 30 days)
 */
export async function cleanupOldRuns() {
  info(`🧹 [${new Date().toISOString()}] Starting cleanup of old job runs...`);
  try {
    const before = VERBOSE ? await pool.query('SELECT COUNT(*) FROM job_runs') : null;
    const res = await pool.query(
      "DELETE FROM job_runs WHERE finished_at < NOW() - INTERVAL '30 days'"
    );
    const after = VERBOSE ? await pool.query('SELECT COUNT(*) FROM job_runs') : null;

    if (VERBOSE && before && after) {
      log(`   ├─ Total runs before cleanup: ${before.rows[0].count}`);
      log(`   ├─ Deleted ${res.rowCount} old job runs (>30 days)`);
      log(`   └─ Total runs after cleanup: ${after.rows[0].count}`);
    } else if (res.rowCount > 0) {
      info(`🧹 Deleted ${res.rowCount} old job runs (>30 days)`);
    } else {
      log('🧹 No old job runs to clean up');
    }
  } catch (err) {
    error(`[Cleanup] ❌ Failed to clean old runs: ${err.message}`);
  }
}

// Schedule cleanup once per day at midnight
cron.schedule('0 0 * * *', cleanupOldRuns);
