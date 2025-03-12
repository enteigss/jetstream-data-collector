import WebSocket from 'ws';
import events from 'events';
import zlib from 'zlib';
import { Pool, PoolClient } from 'pg';

export interface JetstreamConfig {
  endpoint: string;
  wantedCollections?: string[];
  wantedDids?: string[];
  maxMessageSizeBytes?: number;
  cursor?: number;
  compress?: boolean;
  requireHello?: boolean;
}

export interface PostgresConfig {
  host: string;
  port: number;
  database: string;
  user: string;
  password: string;
  ssl?: boolean;
  max?: number; // max connections in pool
}

export class JetstreamCollector extends events.EventEmitter {
  private ws: WebSocket | null = null;
  public pgPool: Pool;
  private config: JetstreamConfig;
  private reconnectDelay: number;
  private isConnected: boolean = false;
  private lastCursor: number = 0;
  
  constructor(pgConfig: PostgresConfig, config: JetstreamConfig, reconnectDelay: number = 3000) {
    super();
    this.pgPool = new Pool(pgConfig);
    this.config = config;
    this.reconnectDelay = reconnectDelay;
    
    // Handle pool errors
    this.pgPool.on('error', (err) => {
      console.error('Unexpected error on idle PostgreSQL client', err);
    });
  }

  /**
   * Initialize database schema
   */

  private async resetDb(): Promise<boolean> {
    let client: PoolClient | null = null;

    try {
      client = await this.pgPool.connect();

      await client.query(`
        DROP TABLE IF EXISTS feature_vectors CASCADE;
        DROP TABLE IF EXISTS engagements CASCADE;
        DROP TABLE IF EXISTS follows CASCADE;
        DROP TABLE IF EXISTS posts CASCADE;
        DROP TABLE IF EXISTS tombstones CASCADE;
        DROP TABLE IF EXISTS cursor_state CASCADE;
        DROP TABLE IF EXISTS users CASCADE;
      `);

      console.log('Dropped all tables');

      return await this.initDb();
    } catch (err) {
      console.error('Failed to reset database schema:', err)
      return false;
    } finally {
      if (client) {
        client.release();
      }
    }
  }

  private async initDb(): Promise<boolean> {
    let client: PoolClient | null = null;
    
    try {
      client = await this.pgPool.connect();
      
      // Create records table
      await client.query(`
        CREATE TABLE IF NOT EXISTS users (
          did TEXT PRIMARY KEY,
          handle TEXT NOT NULL,
          display_name TEXT,
          description TEXT,
          avatar_url TEXT,
          follower_count INTEGER,
          following_count INTEGER,
          post_count INTEGER,
          activity_level FLOAT,
          account_age_days INTEGER,
          CONSTRAINT idx_users_handle UNIQUE (handle)
        )
      `);
      
      // Create accounts table
      await client.query(`
        CREATE TABLE IF NOT EXISTS posts (
          id SERIAL PRIMARY KEY,
          did TEXT NOT NULL,
          rkey TEXT NOT NULL,
          uri TEXT NOT NULL,
          cid TEXT NOT NULL,
          text TEXT,
          created_at TIMESTAMP WITH TIME ZONE NOT NULL,
          hour_timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
          indexed_at TIMESTAMP WITH TIME ZONE NOT NULL,
          text_length INTEGER,
          has_image BOOLEAN,
          image_count SMALLINT,
          has_external_link BOOLEAN,
          has_mention BOOLEAN,
          mention_count SMALLINT,
          hashtag_count SMALLINT,
          reply_to TEXT,
          reply_root TEXT,
          thread_depth SMALLINT,
          language TEXT,
          UNIQUE(did, rkey),
          CONSTRAINT idx_posts_uri UNIQUE (uri)
        )
      `);
      
      // Create identities table
      await client.query(`
        CREATE TABLE IF NOT EXISTS engagements (
          id SERIAL PRIMARY KEY,
          post_uri TEXT NOT NULL REFERENCES posts(uri),
          actor_did TEXT NOT NULL REFERENCES users(did),
          type TEXT NOT NULL,
          created_at TIMESTAMP WITH TIME ZONE NOT NULL,
          reply_uri TEXT,
          time_to_engage INTERVAL,
          actor_follows_author BOOLEAN,
          raw_data JSONB,
          UNIQUE(post_uri, actor_did, type)
        )
      `);

      // Create social graph
      await client.query(`
        CREATE TABLE IF NOT EXISTS follows (
          follower_did TEXT NOT NULL REFERENCES users(did),
          followed_did TEXT NOT NULL REFERENCES users(did),
          created_at TIMESTAMP WITH TIME ZONE NOT NULL,
          PRIMARY KEY (follower_did, followed_did)
        )
      `);

      // Feature store
      await client.query(`
        CREATE TABLE IF NOT EXISTS tombstones (
          id BIGSERIAL PRIMARY KEY,
          did TEXT NOT NULL,
          tombstone_data JSONB NOT NULL,
          created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        )
      `);

      // Create tombstones table
      await client.query(`
        CREATE TABLE IF NOT EXISTS feature_vectors (
          id SERIAL PRIMARY KEY,
          post_uri TEXT NOT NULL REFERENCES posts(uri),
          computed_at TIMESTAMP WITH TIME ZONE NOT NULL,
          text_embedding FLOAT[],
          user_embedding FLOAT[],
          engagement_features JSONB,
          UNIQUE(post_uri, computed_at)
        )
      `);
      
      // Create cursor tracking table
      await client.query(`
        CREATE TABLE IF NOT EXISTS cursor_state (
          id TEXT PRIMARY KEY,
          cursor_value BIGINT NOT NULL,
          updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        )
      `);
      
      // Try to retrieve the last cursor
      const cursorResult = await client.query(
        'SELECT cursor_value FROM cursor_state WHERE id = $1',
        ['main_cursor']
      );
      
      if (cursorResult.rows.length > 0) {
        this.lastCursor = cursorResult.rows[0].cursor_value;
        console.log(`Retrieved last cursor: ${this.lastCursor}`);
      }
      
      console.log('Database schema initialized');
      return true;
    } catch (err) {
      console.error('Failed to initialize database schema:', err);
      return false;
    } finally {
      if (client) {
        client.release();
      }
    }
  }

  /**
   * Build WebSocket URL with query parameters based on config
   */
  private buildUrl(): string {
    const url = new URL(this.config.endpoint);
    // return url.toString()
    
    if (this.config.wantedCollections && this.config.wantedCollections.length > 0) {
      this.config.wantedCollections.forEach(collection => {
        url.searchParams.append('wantedCollections', collection);
      });
    }
    
    if (this.config.wantedDids && this.config.wantedDids.length > 0) {
      this.config.wantedDids.forEach(did => {
        url.searchParams.append('wantedDids', did);
      });
    }
    
    if (this.config.maxMessageSizeBytes !== undefined) {
      url.searchParams.set('maxMessageSizeBytes', this.config.maxMessageSizeBytes.toString());
    }
    
    if (this.config.cursor !== undefined) {
      url.searchParams.set('cursor', this.config.cursor.toString());
    } else if (this.lastCursor > 0) {
      // If we're reconnecting, use the last cursor minus 5 seconds (5,000,000 microseconds)
      // to ensure we don't miss any events
      const reconnectCursor = Math.max(0, this.lastCursor - 5_000_000);
      url.searchParams.set('cursor', reconnectCursor.toString());
    }
    
    if (this.config.compress !== undefined) {
      url.searchParams.set('compress', this.config.compress.toString());
    }
    
    if (this.config.requireHello !== undefined) {
      url.searchParams.set('requireHello', this.config.requireHello.toString());
    }
    
    return url.toString();
  }

  /**
   * Connect to Jetstream WebSocket
   */
  private connect() {
    const url = this.buildUrl();
    console.log(`Connecting to Jetstream at ${url}`);
    
    this.ws = new WebSocket(url);
    
    this.ws.on('open', () => {
      this.isConnected = true;
      console.log('Connected to Jetstream');
      this.emit('connected');
      
      // If requireHello is true, send subscriber options
      if (this.config.requireHello) {
        this.sendSubscriberOptions();
      }
    });
    
    this.ws.on('message', async (data) => {
      try {
        // Handle compressed data if compression is enabled
        let messageData = data;
        if (this.config.compress) {
          messageData = zlib.unzipSync(data as Buffer);
        }
        
        const message = JSON.parse(messageData.toString());
        await this.processMessage(message);
      } catch (err) {
        console.error('Error processing message:', err);
      }
    });
    
    this.ws.on('close', () => {
      this.isConnected = false;
      console.log('Disconnected from Jetstream');
      this.emit('disconnected');
      
      // Reconnect after delay
      setTimeout(() => this.reconnect(), this.reconnectDelay);
    });
    
    this.ws.on('error', (err) => {
      console.error('WebSocket error:', err);
      this.emit('error', err);
    });
  }

  /**
   * Send subscriber options (for requireHello = true)
   */
  private sendSubscriberOptions() {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
    
    const options = {
      type: 'subscriber-options-update',
      ready: true
    };
    
    this.ws.send(JSON.stringify(options));
  }

  /**
   * Process incoming message
   */
  private async processMessage(message: any) {
    // Update cursor for reconnection
    if (message.time_us) {
      this.lastCursor = message.time_us;
      await this.updateCursor(this.lastCursor);
    }
    
    // Emit the raw message for custom processing
    this.emit('message', message);
    
    // Process different message types
    /*
    switch (message.type) {
      case 'commit':
        await this.processCommit(message);
        break;
      case 'account':
        await this.processAccount(message);
        break;
      case 'identity':
        await this.processIdentity(message);
        break;
      case 'tombstone':
        await this.processTombstone(message);
        break;
      case 'hello':
        console.log('Received hello message:', message);
        break;
      default:
        console.log('Unknown message type:', message.type);
    }
    */
  }

  /**
   * Update cursor in database
   */
  private async updateCursor(cursorValue: number) {
    try {
      await this.pgPool.query(
        `INSERT INTO cursor_state (id, cursor_value, updated_at) 
         VALUES ($1, $2, NOW()) 
         ON CONFLICT (id) 
         DO UPDATE SET cursor_value = $2, updated_at = NOW()`,
        ['main_cursor', cursorValue]
      );
    } catch (err) {
      console.error('Error updating cursor:', err);
    }
  }

  /**
   * Process commit message (new records)
   */
  private async processCommit(message: any) {
    try {
      const { repo, commit, ops, time_us } = message;
      
      // Use a client from the pool for transaction
      const client = await this.pgPool.connect();
      
      try {
        // Begin transaction
        await client.query('BEGIN');
        
        for (const op of ops) {
          if (op.action === 'create' || op.action === 'update') {
            await this.storeRecord(client, {
              did: repo,
              collection: op.collection,
              rkey: op.rkey,
              record: op.record,
              timestamp: time_us,
              cid: commit.cid
            });
          } else if (op.action === 'delete') {
            await this.deleteRecord(client, {
              did: repo,
              collection: op.collection,
              rkey: op.rkey
            });
          }
        }
        
        // Commit transaction
        await client.query('COMMIT');
        
        this.emit('commit', message);
      } catch (err) {
        // Rollback on error
        await client.query('ROLLBACK');
        console.error('Error processing commit (transaction rolled back):', err);
      } finally {
        // Release client back to pool
        client.release();
      }
    } catch (err) {
      console.error('Error processing commit:', err);
    }
  }

  /**
   * Process account message
   */
  private async processAccount(message: any) {
    try {
      // Handle account creation/updates
      await this.storeAccount(message);
      this.emit('account', message);
    } catch (err) {
      console.error('Error processing account:', err);
    }
  }

  /**
   * Process identity message
   */
  private async processIdentity(message: any) {
    try {
      // Handle identity updates
      await this.storeIdentity(message);
      this.emit('identity', message);
    } catch (err) {
      console.error('Error processing identity:', err);
    }
  }

  /**
   * Process tombstone message (deleted records)
   */
  private async processTombstone(message: any) {
    try {
      // Handle tombstones (permanent deletions)
      await this.storeTombstone(message);
      this.emit('tombstone', message);
    } catch (err) {
      console.error('Error processing tombstone:', err);
    }
  }

  /**
   * Store record in database
   */
  private async storeRecord(client: PoolClient, data: any) {
    try {
      await client.query(
        `INSERT INTO records (did, collection, rkey, record, timestamp, cid)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (did, collection, rkey)
         DO UPDATE SET record = $4, timestamp = $5, cid = $6`,
        [
          data.did,
          data.collection,
          data.rkey,
          data.record,
          data.timestamp,
          data.cid
        ]
      );
    } catch (err) {
      console.error('Error storing record:', err);
      throw err; // Re-throw to trigger transaction rollback
    }
  }

  /**
   * Delete record from database
   */
  private async deleteRecord(client: PoolClient, data: any) {
    try {
      await client.query(
        'DELETE FROM records WHERE did = $1 AND collection = $2 AND rkey = $3',
        [data.did, data.collection, data.rkey]
      );
    } catch (err) {
      console.error('Error deleting record:', err);
      throw err; // Re-throw to trigger transaction rollback
    }
  }

  /**
   * Store account information
   */
  private async storeAccount(data: any) {
    try {
      await this.pgPool.query(
        `INSERT INTO accounts (did, handle, data, updated_at)
         VALUES ($1, $2, $3, NOW())
         ON CONFLICT (did)
         DO UPDATE SET handle = $2, data = $3, updated_at = NOW()`,
        [
          data.did,
          data.handle,
          data
        ]
      );
    } catch (err) {
      console.error('Error storing account:', err);
    }
  }

  /**
   * Store identity information
   */
  private async storeIdentity(data: any) {
    try {
      await this.pgPool.query(
        `INSERT INTO identities (did, handle, data, updated_at)
         VALUES ($1, $2, $3, NOW())
         ON CONFLICT (did)
         DO UPDATE SET handle = $2, data = $3, updated_at = NOW()`,
        [
          data.did,
          data.handle,
          data
        ]
      );
    } catch (err) {
      console.error('Error storing identity:', err);
    }
  }

  /**
   * Store tombstone information
   */
  private async storeTombstone(data: any) {
    try {
      await this.pgPool.query(
        'INSERT INTO tombstones (did, tombstone_data) VALUES ($1, $2)',
        [
          data.did,
          data
        ]
      );
    } catch (err) {
      console.error('Error storing tombstone:', err);
    }
  }

  /**
   * Reconnect to WebSocket
   */
  private reconnect() {
    console.log(`Reconnecting to Jetstream in ${this.reconnectDelay}ms...`);
    this.connect();
  }

  /**
   * Start collecting data
   */
  public async run() {
    // Initialize database first
    const dbInitialized = await this.resetDb();
    if (!dbInitialized) {
      throw new Error('Failed to initialize database schema');
    }
    
    // Connect to Jetstream
    this.connect();
    return this;
  }

  /**
   * Stop collecting data
   */
  public async stop() {
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
    
    // Close PostgreSQL pool
    await this.pgPool.end();
    
    return this;
  }
}