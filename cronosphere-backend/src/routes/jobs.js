import express from 'express';
import cron from 'node-cron';
import { pool } from '../db.js';
import { scheduleJob, cancelJob } from '../scheduler.js';

const router = express.Router();

router.use((req, res, next) => {
  const apiKey = req.headers['x-api-key'];
  if (!process.env.API_KEY) return next(); // skip if not configured
  if (apiKey !== process.env.API_KEY)
    return res.status(401).json({ error: 'unauthorized' });
  next();
});

const FORBIDDEN_PATTERNS = [
  /(^|\s)sudo(\s|$)/i,
  /rm\s+-rf/i,
/:\s*\(\)\s*{\s*:\s*\|\s*:\s*;\s*}/, // fork bomb
/dd\s+if=/i,
/mkfs\./i,
/:(){:|:&};:/ // another fork bomb
];

function isForbidden(command) {
  if (!command) return false;
  for (const re of FORBIDDEN_PATTERNS) {
    if (re.test(command)) return true;
  }
  return false;
}

// Create new job
router.post('/', async (req, res) => {
  try {
    const { name, command, schedule } = req.body;
    if (!name || !command || !schedule)
      return res.status(400).json({ error: 'name, command, schedule required' });

    if (command.length > 2000)
      return res.status(400).json({ error: 'command too long' });

    if (isForbidden(command))
      return res.status(400).json({ error: 'command contains forbidden operations' });

    if (!cron.validate(schedule))
      return res.status(400).json({ error: 'invalid cron expression' });

    const result = await pool.query(
      'INSERT INTO jobs (name, command, schedule, status) VALUES ($1, $2, $3, $4) RETURNING *',
                                    [name, command, schedule, 'active']
    );

    const job = result.rows[0];
    scheduleJob(job);
    res.status(201).json(job);
  } catch (err) {
    console.error('POST /api/jobs error:', err);
    res.status(500).json({ error: 'internal server error' });
  }
});

// List jobs
router.get('/', async (req, res) => {
  const result = await pool.query('SELECT * FROM jobs ORDER BY id DESC');
  res.json(result.rows);
});

// Run job manually
router.post('/:id/run', async (req, res) => {
  try {
    const { id } = req.params;
    const result = await pool.query('SELECT * FROM jobs WHERE id = $1', [id]);
    const job = result.rows[0];
    if (!job) return res.status(404).json({ error: 'job not found' });

    // Log the run first
    const runResult = await pool.query(
      'INSERT INTO job_runs (job_id, status) VALUES ($1, $2) RETURNING id',
                                       [id, 'queued']
    );

    const runId = runResult.rows[0].id;

    // Schedule job to run immediately
    scheduleJob(job, true);

    res.json({ ok: true, runId });
  } catch (err) {
    console.error('POST /api/jobs/:id/run error:', err);
    res.status(500).json({ error: 'internal server error' });
  }
});

// Pause a job
router.post('/:id/pause', async (req, res) => {
  try {
    const { id } = req.params;
    const result = await pool.query('SELECT * FROM jobs WHERE id=$1', [id]);
    const job = result.rows[0];
    if (!job) return res.status(404).json({ error: 'job not found' });

    // Update DB
    await pool.query('UPDATE jobs SET status=$1 WHERE id=$2', ['paused', id]);

    // Stop scheduler task
    cancelJob(id);

    res.json({ ok: true, message: `Job ${id} paused` });
  } catch (err) {
    console.error('POST /api/jobs/:id/pause error:', err);
    res.status(500).json({ error: 'internal server error' });
  }
});

// Resume a paused job
router.post('/:id/resume', async (req, res) => {
  try {
    const { id } = req.params;
    const result = await pool.query('SELECT * FROM jobs WHERE id=$1', [id]);
    const job = result.rows[0];
    if (!job) return res.status(404).json({ error: 'job not found' });

    if (job.status !== 'paused')
      return res.status(400).json({ error: 'job is not paused' });

    // Update DB
    await pool.query('UPDATE jobs SET status=$1 WHERE id=$2', ['active', id]);

    // Re-schedule the job
    scheduleJob(job);

    res.json({ ok: true, message: `Job ${id} resumed` });
  } catch (err) {
    console.error('POST /api/jobs/:id/resume error:', err);
    res.status(500).json({ error: 'internal server error' });
  }
});

// Get all runs for a specific job
router.get('/:id/runs', async (req, res) => {
  const { id } = req.params;
  const result = await pool.query(
    'SELECT * FROM job_runs WHERE job_id=$1 ORDER BY id DESC',
    [id]
  );
  res.json(result.rows);
});

// Get single run by ID
router.get('/runs/:runId', async (req, res) => {
  const { runId } = req.params;
  const result = await pool.query(
    'SELECT * FROM job_runs WHERE id=$1',
    [runId]
  );
  if (result.rows.length === 0)
    return res.status(404).json({ error: 'not found' });
  res.json(result.rows[0]);
});

// Delete a job
router.delete('/:id', async (req, res) => {
  const { id } = req.params;
  await pool.query('DELETE FROM jobs WHERE id=$1', [id]);
  cancelJob(id);
  res.json({ ok: true });
});

export default router;
