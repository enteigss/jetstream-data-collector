import { Pool } from 'pg'
import { Server } from 'socket.io'

export class AggregationService {
    private pool: Pool
    private io?: Server

    constructor(pool: Pool, io?: Server) {
        this.pool = pool
        this.io = io
    }

    async computeRealTimeMetrics() {
        try {
            console.log('Computing real-time metrics')

            const postsPerMinuteResults = await this.pool.query(`
                SELECT COUNT(*) AS count
                FROM posts
                WHERE created_at >= NOW() - INTERVAL '1 minute'
                `)

            const postsPerMinute = parseInt(postsPerMinuteResults.rows[0].count)

            const activeUsersResult = await this.pool.query(`
                SELECT COUNT(DISTINCT did) AS count
                FROM posts
                WHERE created_at >= NOW() - INTERVAL '5 minutes'
                `)

            const activeUsers = parseInt(activeUsersResult.rows[0].count)

            console.log(`Posts per minute: ${postsPerMinute}, Active users: ${activeUsers}`)

            return { postsPerMinute, activeUsers }
        } catch (error) {
            console.error('Error computing real-time metrics:', error)
            throw error
        }
    }

    async computeShortTermMetrics() {
        try {
            console.log('Computing short-term metrics...')

            await this.updateHourlyPostMetrics()

            console.log("Updated hourly metrics")

            const hourlyMetricsResult = await this.pool.query(`
                SELECT 
                    to_char(hour_timestamp, 'HH24:00') AS hour_label,
                    post_count,
                    unique_author_count,
                    has_image_count,
                    has_link_count,
                    text_only_count
                FROM metrics.hourly_post_metrics
                WHERE hour_timestamp >= NOW() - INTERVAL '24 hours'
                ORDER BY hour_timestamp
                `)

            const hourlyMetrics = hourlyMetricsResult.rows

            console.log(`Updated hourly metrics for ${hourlyMetrics.length} hours`)

            return hourlyMetrics
        } catch (error) {
            console.error('Error computing short-term metrics:', error)
            throw error
        }
    }

    private async updateHourlyPostMetrics(daysBack = 1) {
        try {
            const result = await this.pool.query(`
                INSERT INTO metrics.hourly_post_metrics (
                    hour_timestamp,
                    post_count,
                    unique_author_count,
                    has_image_count,
                    has_link_count,
                    text_only_count,
                    avg_text_length,
                    max_text_length,
                    min_text_length
                )
                SELECT
                    hour_timestamp,
                    COUNT(*) AS post_count,
                    COUNT(DISTINCT did) AS unique_author_count,
                    SUM(CASE WHEN has_image = true THEN 1 ELSE 0 END) AS has_image_count,
                    SUM(CASE WHEN has_external_link = true THEN 1 ELSE 0 END) AS has_link_count,
                    SUM(CASE WHEN has_image = false AND has_external_link = false THEN 1 ELSE 0 END) AS text_only_count,
                    AVG(text_length) AS avg_text_length,
                    MAX(text_length) AS max_text_length,
                    MIN(text_length) AS min_text_length
                FROM posts
                WHERE 
                    created_at >= date_trunc('hour', NOW() - INTERVAL '${daysBack} days')
                    AND created_at < date_trunc('hour', NOW())
                GROUP BY hour_timestamp
                ON CONFLICT (hour_timestamp)
                DO UPDATE SET
                    post_count = EXCLUDED.post_count,
                    unique_author_count = EXCLUDED.unique_author_count,
                    has_image_count = EXCLUDED.has_image_count,
                    has_link_count = EXCLUDED.has_link_count,
                    text_only_count = EXCLUDED.text_only_count,
                    avg_text_length = EXCLUDED.avg_text_length,
                    max_text_length = EXCLUDED.max_text_length,
                    min_text_length = EXCLUDED.min_text_length,
                    updated_at = NOW()
                    `)
                
                const hourlyPostMetrics = await this.pool.query(`
                    SELECT hour_timestamp, post_count
                    FROM metrics.hourly_post_metrics
                    `)
                
                console.log("Hourly post metrics table:", hourlyPostMetrics)
                
                console.log(`Updated hourly post metrics (${daysBack} days of data)`)
                return true
        } catch (error) {
            console.error('Error updating hourly post metrics:', error)
            throw error
        }
    }

    public async resetDb(): Promise<boolean> {

        const client = await this.pool.connect()
    
        try {
    
            await client.query(`
                DROP TABLE IF EXISTS metrics.hourly_post_metrics CASCADE;
            `);
    
            console.log('Dropped hourly post metrics tables');
    
            return await this.initializeMetricsSchema();
        } catch (err) {
            console.error('Failed to reset database schema:', err)
            return false;
        } finally {
            if (client) {
                client.release();
            }
        }
    }

    async initializeMetricsSchema() {
        const client = await this.pool.connect()

        try {
            await client.query('BEGIN')

            await client.query(`
                CREATE SCHEMA IF NOT EXISTS metrics
            `)

            await client.query(`
                CREATE TABLE IF NOT EXISTS metrics.hourly_post_metrics (
                  hour_timestamp TIMESTAMP WITH TIME ZONE PRIMARY KEY,
                  post_count INTEGER NOT NULL,
                  unique_author_count INTEGER NOT NULL,
                  has_image_count INTEGER NOT NULL DEFAULT 0,
                  has_link_count INTEGER NOT NULL DEFAULT 0,
                  text_only_count INTEGER NOT NULL DEFAULT 0,
                  avg_text_length FLOAT,
                  max_text_length INTEGER,
                  min_text_length INTEGER,
                  created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
                  updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
                );

                CREATE INDEX IF NOT EXISTS idx_hourly_post_metrics_timestamp
                ON metrics.hourly_post_metrics (hour_timestamp);
            `)

            await client.query('COMMIT')

            console.log('Metrics schema and tables initialized successfully')

            // await this.backfillHistoricalData()

            return true
        } catch (error) {
            await client.query('ROLLBACK')
            console.error('Error initializing metrics schema:', error)
            throw error
        } finally {
            client.release()
        }
    }

    async backfillHistoricalData() {
        try {
            console.log('Backfilling historical metrics data...')

            await this.updateHourlyPostMetrics(7)

            console.log('Historical data backfill completed')
            return true
        } catch (error) {
            console.error('Error backfilling historical data:', error)
            throw error
        }
    }

    async getDashboardData(timeRange = '24h') {
        try {
            const hourlyDataResult = await this.pool.query(`
                SELECT
                    to_char(hour_timestamp, 'HH24:00') AS hour_label,
                    post_count,
                    unique_author_count
                FROM metrics.hourly_post_metrics
                WHERE hour_timestamp >= NOW() - INTERVAL '${timeRange === '24h' ? '24 hours' : '7 days'}'
                ORDER BY hour_timestamp
                `)

            const hourlyData = hourlyDataResult.rows

            return {
                postVolume: {
                    hourly: hourlyData.map(h => ({
                        time: h.hour_label,
                        count: parseInt(h.post_count)
                    }))
                },
                lastUpdated: new Date().toISOString()
            }
        } catch (error) {
            console.error('Error fetching dashboard data:', error)
            throw error
        }
    }
}