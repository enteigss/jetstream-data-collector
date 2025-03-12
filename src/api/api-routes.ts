// Create a new file: api-routes.ts
import express from 'express';
import { Pool } from 'pg';
import { AggregationService } from '../aggregation-service';

export function setupApiRoutes(app: express.Application, pool: Pool) {
  const router = express.Router();
  const aggregationService = new AggregationService(pool);

  // Get dashboard summary data
  router.get('/dashboard/summary', async (req, res) => {
    try {
      const timeRange = req.query.range || '24h';
      const data = await aggregationService.getDashboardData(timeRange as string);
      res.json(data);
    } catch (error) {
      console.error('Error fetching dashboard data:', error);
      res.status(500).json({ error: 'Failed to fetch dashboard data' });
    }
  });

  // Get hourly metrics
  router.get('/metrics/hourly', async (req, res) => {
    try {
      const days = parseInt(req.query.days as string || '1');
      const posts = await pool.query(`
        SELECT COUNT(*)
        FROM posts
        `)

      console.log("Posts count:", posts.rows)
      const data1 = aggregationService.computeShortTermMetrics()
      console.log("Hourly metrics:", data1)
      const data = await pool.query(`
        SELECT 
          to_char(hour_timestamp, 'YYYY-MM-DD HH24:00') AS time,
          post_count,
          unique_author_count
        FROM metrics.hourly_post_metrics
        WHERE hour_timestamp >= NOW() - INTERVAL '${days} days'
        ORDER BY hour_timestamp
      `);
      res.json(data1);
    } catch (error) {
      console.error('Error fetching hourly metrics:', error);
      res.status(500).json({ error: 'Failed to fetch hourly metrics' });
    }
  });

  
  router.get('/metrics/realtime', async (req, res) => {
    try {
      const postsPerMinuteResult = await pool.query(`
        SELECT COUNT(*) AS count
        FROM posts
        WHERE created_at >= NOW() - INTERVAL '1 minute'
        `)

      const activeUsersResult = await pool.query(`
        SELECT COUNT(DISTINCT did) AS count
        FROM posts
        WHERE created_at >= NOW() - INTERVAL '5 minutes'
        `)

      const mostActiveUserResult = await pool.query(`
        SELECT p.did, handle, COUNT(*) AS post_count
        FROM posts p
        JOIN users u ON p.did = u.did
        WHERE p.created_at >= NOW() - INTERVAL '10 minutes'
        GROUP BY p.did, u.handle
        ORDER BY post_count DESC
        LIMIT 1
        `)

      const realtimeData = {
        postsPerMinute: parseInt(postsPerMinuteResult.rows[0]?.count || '0'),
        activeUsers: parseInt(activeUsersResult.rows[0]?.count || '0'),
        mostActiveUser: mostActiveUserResult.rows[0] || null,
        timestamp: new Date().toISOString()
      }

      res.json(realtimeData)
    } catch (error) {
      console.error('Error fetching real-time metrics:', error)
      res.status(500).json({ error: 'Failed to fetch real-time metrics' })
    }
  })

  app.use('/api', router);
}