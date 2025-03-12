import dotenv from 'dotenv';
import { Pool } from 'pg';
import { JetstreamCollector, PostgresConfig } from './jetstream-collector';
import { JetstreamProcessor } from './jetstream-processor';
import http from 'http'
import express from 'express'
import { Server } from 'socket.io'
import { MetricsScheduler } from './metrics-scheduler'
import { setupApiRoutes } from './api/api-routes'
import cors from 'cors'

// Load environment variables
dotenv.config();

const run = async () => {
  try {
    // Configure PostgreSQL connection
    const pgConfig: PostgresConfig = {
      host: process.env.PG_HOST || 'localhost',
      port: parseInt(process.env.PG_PORT || '5432'),
      database: process.env.PG_DATABASE || 'bluesky_collector',
      user: process.env.PG_USER || 'postgres',
      password: process.env.PG_PASSWORD || '',
      ssl: process.env.PG_SSL === 'true',
      max: parseInt(process.env.PG_MAX_CONNECTIONS || '20')
    };

    // Create PostgreSQL pool
    const pgPool = new Pool(pgConfig);

    const app = express()

    app.use(cors({
      origin: ['http://localhost:4000', 'http://127.0.0.1:4000'],
      methods: ['GET', 'POST'],
      credentials: true
    }))

    const server = http.createServer(app)
    const io = new Server(server, {
      cors: {
        origin: ['http://localhost:4000', 'http://127.0.0.1:4000'],
        methods: ['GET', 'POST'],
        credentials: true
      }
    })

    setupApiRoutes(app, pgPool)

    // Create and start the processor
    const processor = new JetstreamProcessor(pgPool, {
      bskyService: 'https://bsky.social',
      bskyHandle: process.env.BSKY_HANDLE,
      bskyPassword: process.env.BSKY_PASSWORD
    });

    // Parse wanted collections from environment variables if provided
    let wantedCollections: string[] | undefined = undefined;
    if (process.env.WANTED_COLLECTIONS) {
      wantedCollections = process.env.WANTED_COLLECTIONS.split(',').map(c => c.trim());
    }
    
    // Parse wanted DIDs from environment variables if provided
    let wantedDids: string[] | undefined = undefined;
    if (process.env.WANTED_DIDS) {
      wantedDids = process.env.WANTED_DIDS.split(',').map(d => d.trim());
    }
    
    // Configure the Jetstream collector
    const collector = new JetstreamCollector(
      pgConfig,
      {
        endpoint: process.env.JETSTREAM_ENDPOINT || 'wss://bsky.network/subscribe',
        wantedCollections,
        wantedDids,
        compress: process.env.COMPRESS === 'false',
        maxMessageSizeBytes: process.env.MAX_MESSAGE_SIZE ? parseInt(process.env.MAX_MESSAGE_SIZE) : undefined,
        requireHello: process.env.REQUIRE_HELLO === 'true',
      },
      parseInt(process.env.RECONNECT_DELAY || '5000')
    );

    const metricsScheduler = new MetricsScheduler(pgPool, io)
    await metricsScheduler.initialize()

    // Start the processor
    await processor.start();
    
    // Connect processor to the collector
    collector.on('message', async (message) => {
      try {
        await processor.processMessage(message);
      } catch (error) {
        console.error('Unhandled error in message processing:', error)
      }
      
    });
    
    // Set up event listeners for the collector
    collector.on('connected', () => {
      console.log('🚀 Connected to Jetstream');
    });
    
    collector.on('disconnected', () => {
      console.log('❌ Disconnected from Jetstream');
    });
    
    collector.on('error', (err) => {
      console.error('Error in Jetstream collector:', err);
    });
    
    // Handle shutdown signals
    process.on('SIGINT', async () => {
      console.log('Received SIGINT. Shutting down...');
      await processor.stop();
      await collector.stop();
      await pgPool.end();
      process.exit(0);
    });
    
    process.on('SIGTERM', async () => {
      console.log('Received SIGTERM. Shutting down...');
      await processor.stop();
      await collector.stop();
      await pgPool.end();
      process.exit(0);
    });
    
    // Start the collector
    await collector.run();
    await metricsScheduler.start()
    
    console.log(`📊 Bluesky data collector started`);
    console.log(`📁 Database: ${pgConfig.database} @ ${pgConfig.host}:${pgConfig.port}`);
    if (wantedCollections) {
      console.log(`🔍 Collecting: ${wantedCollections.join(', ')}`);
    } else {
      console.log(`🔍 Collecting: All collections`);
    }

    server.listen(3000, () => {
        console.log('Server running on port 3000')
    })
  } catch (err) {
    console.error('Failed to start collector:', err);
    process.exit(1);
  }
};

// Run the application
run();