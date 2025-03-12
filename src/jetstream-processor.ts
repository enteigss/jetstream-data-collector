import { Pool } from 'pg';
import events from 'events';
import { AtpAgent } from '@atproto/api';

/**
 * Configuration for the JetstreamProcessor
 */
export interface JetstreamProcessorConfig {
  bskyService?: string;
  bskyHandle?: string;
  bskyPassword?: string;
}

/**
 * JetstreamProcessor processes events from the Bluesky network
 * and stores them in a structured database.
 */
export class JetstreamProcessor extends events.EventEmitter {
  private agent: AtpAgent;
  private pgPool: Pool;
  private config: JetstreamProcessorConfig;
  private isRunning: boolean = false;

  /**
   * Creates a new instance of the JetstreamProcessor
   * 
   * @param pgPool PostgreSQL connection pool
   * @param config Configuration options
   */
  constructor(pgPool: Pool, config: JetstreamProcessorConfig = {}) {
    super();
    
    this.pgPool = pgPool;
    this.config = {
      bskyService: config.bskyService || 'https://bsky.social',
      bskyHandle: config.bskyHandle,
      bskyPassword: config.bskyPassword,
      ...config
    };

    // Initialize ATP agent for API calls
    this.agent = new AtpAgent({
      service: 'https://bsky.social'
    });
  }

  /**
   * Initialize the processor and login to Bluesky if credentials provided
   */
  async init(): Promise<void> {
    // Login to Bluesky if credentials are provided
    if (this.config.bskyHandle && this.config.bskyPassword) {
      try {
        await this.agent.login({
          identifier: this.config.bskyHandle,
          password: this.config.bskyPassword
        });
        console.log(`Logged in as ${this.config.bskyHandle}`);
      } catch (err) {
        console.error('Failed to login to Bluesky:', err);
        // Continue without login - we can still process events without being logged in
      }
    }
  }

  /**
   * Process a post message from Jetstream
   * 
   * @param message The message from Jetstream
   */
  async processPost(message: any): Promise<void> {
    try {
      // console.log(`Processing post from ${message.did}`);
      const record = message.commit.record;

      const postData = {
        did: message.did,
        rkey: message.commit.rkey,
        uri: `at://${message.did}/app.bsky.feed.post/${message.commit.rkey}`,
        cid: message.commit.cid,
        text: record.text,
        created_at: new Date(record.createdAt),
        hour_timestamp: new Date(new Date(record.createdAt).setMinutes(0, 0, 0)),
        indexed_at: new Date(),
        text_length: record.text ? record.text.length : 0,
        has_image: Boolean(record.embed && 
                  (record.embed.$type === 'app.bsky.embed.images' ||
                    record.embed.$type === 'app.bsky.embed.recordWithMedia')),
        image_count: this.getImageCount(record),
        has_external_link: Boolean(record.text && record.text.includes('http')),
        has_mention: Boolean(record.text && record.text.includes('@')),
        mention_count: this.countMentions(record),
        hashtag_count: this.countHashtags(record),
        reply_to: record.reply ? record.reply.parent.uri : null,
        reply_root: record.reply ? record.reply.root.uri : null,
        thread_depth: record.reply ? 1 : 0,
        language: this.detectLanguage(record.text),
        raw_data: JSON.stringify(record)
      };

      // console.log("Processed post:", postData);
      await this.storePost(postData);
      
      // Emit an event that the post was processed
      this.emit('post-processed', postData);
    } catch (err) {
      console.error('Error processing post:', err);
      this.emit('error', { type: 'post-processing', error: err, message });
    }
  }

  /**
   * Process a like message from Jetstream
   * 
   * @param message The message from Jetstream
   */
  async processLike(message: any): Promise<void> {
    try {
      console.log(`Processing like from ${message.did}`);
      const record = message.commit.record;
      
      const engagementData = {
        post_uri: record.subject.uri,
        actor_did: message.did,
        type: 'like',
        created_at: new Date(record.createdAt),
        reply_uri: null,
        time_to_engage: null, // Would require lookup of original post time
        actor_follows_author: false, // Would require social graph lookup
        raw_data: record
      };
      
      await this.storeEngagement(engagementData);
      this.emit('like-processed', engagementData);
    } catch (err) {
      console.error('Error processing like:', err);
      this.emit('error', { type: 'like-processing', error: err, message });
    }
  }

  /**
   * Process a repost message from Jetstream
   * 
   * @param message The message from Jetstream
   */
  async processRepost(message: any): Promise<void> {
    try {
      console.log(`Processing repost from ${message.did}`);
      const record = message.commit.record;
      
      const engagementData = {
        post_uri: record.subject.uri,
        actor_did: message.did,
        type: 'repost',
        created_at: new Date(record.createdAt),
        reply_uri: null,
        time_to_engage: null,
        actor_follows_author: false,
        raw_data: record
      };
      
      await this.storeEngagement(engagementData);
      this.emit('repost-processed', engagementData);
    } catch (err) {
      console.error('Error processing repost:', err);
      this.emit('error', { type: 'repost-processing', error: err, message });
    }
  }

  /**
   * Process a follow message from Jetstream
   * 
   * @param message The message from Jetstream
   */
  async processFollow(message: any): Promise<void> {
    try {
      console.log(`Processing follow from ${message.did}`);
      const record = message.commit.record;
      
      const followData = {
        follower_did: message.did,
        followed_did: record.subject,
        created_at: new Date(record.createdAt)
      };
      
      await this.storeFollow(followData);
      this.emit('follow-processed', followData);
    } catch (err) {
      console.error('Error processing follow:', err);
      this.emit('error', { type: 'follow-processing', error: err, message });
    }
  }

  /**
   * Process a profile message from Jetstream
   * 
   * @param message The message from Jetstream
   */
  async processProfile(message: any): Promise<void> {
    try {
      // console.log(`Processing profile for ${message.did}`);
      const record = message.commit.record;

      // Try to get a full profile using the agent
      let profileData: any = { handle: '' };
      if (this.agent.session) {
        try {
          const profileResponse = await this.agent.api.app.bsky.actor.getProfile({ actor: message.did });
          profileData = profileResponse.data;
          console.log("Full profile data:", profileData);
        } catch (err) {
          console.warn(`Couldn't fetch full profile for ${message.did}:`, err);
          // Continue with limited profile data
        }
      }

      const userData = {
        did: message.did,
        handle: profileData.handle || record.handle || '', 
        display_name: record.displayName || '',
        description: record.description || '',
        avatar_url: profileData.avatar || '', 
        follower_count: profileData.followersCount || null, 
        following_count: profileData.followsCount || null,
        post_count: profileData.postsCount || null,
        first_seen_at: new Date(),
        last_updated_at: new Date()
      };
      
      await this.updateUserProfile(userData);
      this.emit('profile-processed', userData);
    } catch (err) {
      console.error('Error processing profile:', err);
      this.emit('error', { type: 'profile-processing', error: err, message });
    }
  }

  /**
   * Process an account update message from Jetstream
   * 
   * @param message The message from Jetstream
   */
  async processAccountUpdate(message: any): Promise<void> {
    try {
      console.log(`Processing account update for ${message.did}`);
      
      const userData = {
        did: message.did,
        handle: message.handle,
        last_updated_at: new Date()
      };
      
      await this.updateUserHandle(userData);
      this.emit('account-processed', userData);
    } catch (err) {
      console.error('Error processing account event:', err);
      this.emit('error', { type: 'account-processing', error: err, message });
    }
  }

  /**
   * Process a message from Jetstream
   * Routes the message to the appropriate handler based on its type
   * 
   * @param message The message from Jetstream
   */
  async processMessage(message: any): Promise<void> {
    // Skip if not running
    if (!this.isRunning) return;

    try {
      // Skip non-commit messages or those without record
      if (!message || !message.commit) {
        // Check if it's an account event
        if (message && message.did && message.handle) {
          await this.processAccountUpdate(message);
        }
        return;
      }

      // Process based on collection and operation type
      const { collection, operation } = message.commit;
      
      if (collection === 'app.bsky.feed.post' && operation === 'create') {
        await this.processPost(message);
      } else if (collection === 'app.bsky.feed.like') {
        await this.processLike(message);
      } else if (collection === 'app.bsky.feed.repost') {
        await this.processRepost(message);
      } else if (collection === 'app.bsky.graph.follow') {
        await this.processFollow(message);
      } else if (collection === 'app.bsky.actor.profile') {
        await this.processProfile(message);
      }
      
      // Emit a generic message-processed event for all messages
      this.emit('message-processed', { 
        type: collection, 
        operation,
        did: message.did 
      });
    } catch (err) {
      console.error('Error processing message:', err);
      this.emit('error', { type: 'message-processing', error: err, message });
    }
  }

  /**
   * Start the processor
   */
  async start(): Promise<void> {
    await this.init();
    this.isRunning = true;
    console.log('JetstreamProcessor started');
    this.emit('started');
    return;
  }

  /**
   * Stop the processor
   */
  async stop(): Promise<void> {
    this.isRunning = false;
    console.log('JetstreamProcessor stopped');
    this.emit('stopped');
    return;
  }

  /**
   * Store a post in the database
   * 
   * @param postData The post data to store
   */
  private async storePost(postData: any): Promise<void> {
    try {
      const client = await this.pgPool.connect();

      try {
        await client.query(`
          INSERT INTO posts (
            did, rkey, uri, cid, text, created_at, hour_timestamp, indexed_at,
            text_length, has_image, image_count, has_external_link,
            has_mention, mention_count, hashtag_count, reply_to,
            reply_root, thread_depth, language
          ) VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19
          ) ON CONFLICT (uri) DO UPDATE SET
            text = $5,
            indexed_at = $8,
            text_length = $9,
            has_image = $10,
            image_count = $11,
            has_external_link = $12,
            has_mention = $13,
            mention_count = $14,
            hashtag_count = $15
          `, [
            postData.did, postData.rkey, postData.uri, postData.cid, postData.text,
            postData.created_at, postData.hour_timestamp, postData.indexed_at, postData.text_length,
            postData.has_image, postData.image_count, postData.has_external_link,
            postData.has_mention, postData.mention_count, postData.hashtag_count,
            postData.reply_to, postData.reply_root, postData.thread_depth,
            postData.language
          ]);
      } finally {
        client.release();
      }
    } catch (err) {
      console.error('Error storing post:', err);
      throw err;
    }
  }

  /**
   * Store an engagement in the database
   * 
   * @param engagementData The engagement data to store
   */
  private async storeEngagement(engagementData: any): Promise<void> {
    try {
      const client = await this.pgPool.connect();
      
      try {
        await client.query(`
          INSERT INTO engagements (
            post_uri, actor_did, type, created_at, reply_uri, 
            time_to_engage, actor_follows_author, raw_data
          ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8
          ) ON CONFLICT (post_uri, actor_did, type) DO UPDATE SET
            created_at = $4,
            raw_data = $8
        `, [
          engagementData.post_uri, engagementData.actor_did, engagementData.type,
          engagementData.created_at, engagementData.reply_uri,
          engagementData.time_to_engage, engagementData.actor_follows_author,
          engagementData.raw_data
        ]);
      } finally {
        client.release();
      }
    } catch (err) {
      console.error('Error storing engagement:', err);
      throw err;
    }
  }

  /**
   * Store a follow in the database
   * 
   * @param followData The follow data to store
   */
  private async storeFollow(followData: any): Promise<void> {
    try {
      const client = await this.pgPool.connect();
      
      try {
        await client.query(`
          INSERT INTO follows (
            follower_did, followed_did, created_at
          ) VALUES (
            $1, $2, $3
          ) ON CONFLICT (follower_did, followed_did) DO UPDATE SET
            created_at = $3
        `, [
          followData.follower_did, followData.followed_did, followData.created_at
        ]);
      } finally {
        client.release();
      }
    } catch (err) {
      console.error('Error storing follow:', err);
      throw err;
    }
  }

  /**
   * Update a user profile in the database
   * 
   * @param userData The user data to update
   */
  private async updateUserProfile(userData: any): Promise<void> {
    try {
      const client = await this.pgPool.connect();
      
      try {
        await client.query(`
          INSERT INTO users (
            did, handle, display_name, description, avatar_url,
            follower_count, following_count, post_count,
            first_seen_at, last_updated_at
          ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
          ) ON CONFLICT (did) DO UPDATE SET
            handle = $2,
            display_name = $3,
            description = $4,
            avatar_url = $5,
            follower_count = $6,
            following_count = $7,
            post_count = $8,
            last_updated_at = $10
        `, [
          userData.did, userData.handle, userData.display_name, userData.description,
          userData.avatar_url, userData.follower_count, userData.following_count,
          userData.post_count, userData.first_seen_at, userData.last_updated_at
        ]);
      } finally {
        client.release();
      }
    } catch (err) {
      console.error('Error updating user profile:', err);
      throw err;
    }
  }

  /**
   * Update a user handle in the database
   * 
   * @param userData The user data to update
   */
  private async updateUserHandle(userData: any): Promise<void> {
    try {
      const client = await this.pgPool.connect();
      
      try {
        await client.query(`
          INSERT INTO users (
            did, handle, last_updated_at
          ) VALUES (
            $1, $2, $3
          ) ON CONFLICT (did) DO UPDATE SET
            handle = $2,
            last_updated_at = $3
        `, [
          userData.did, userData.handle, userData.last_updated_at
        ]);
      } finally {
        client.release();
      }
    } catch (err) {
      console.error('Error updating user handle:', err);
      throw err;
    }
  }

  /**
   * Get the number of images in a post
   * 
   * @param record The post record
   * @returns The number of images
   */
  private getImageCount(record: any): number {
    if (!record.embed) return 0;
    
    if (record.embed.$type === 'app.bsky.embed.images') {
      return record.embed.images.length;
    }
    
    if (record.embed.$type === 'app.bsky.embed.recordWithMedia' && 
        record.embed.media.$type === 'app.bsky.embed.images') {
      return record.embed.media.images.length;
    }
    
    return 0;
  }

  /**
   * Count the number of mentions in a post
   * 
   * @param record The post record
   * @returns The number of mentions
   */
  private countMentions(record: any): number {
    if (!record.text) return 0;
    
    // Count facets of type mention
    if (record.facets) {
      return record.facets.filter((facet: any) => 
        facet.features.some((feature: any) => feature.$type === 'app.bsky.richtext.facet#mention')
      ).length;
    }
    
    // Simple @ count if facets aren't available
    return (record.text.match(/@/g) || []).length;
  }

  /**
   * Count the number of hashtags in a post
   * 
   * @param record The post record
   * @returns The number of hashtags
   */
  private countHashtags(record: any): number {
    if (!record.text) return 0;
    
    // Count facets of type hashtag
    if (record.facets) {
      return record.facets.filter((facet: any) => 
        facet.features.some((feature: any) => feature.$type === 'app.bsky.richtext.facet#tag')
      ).length;
    }
    
    // Simple # count if facets aren't available
    return (record.text.match(/#\w+/g) || []).length;
  }

  /**
   * Detect the language of a post
   * 
   * @param text The post text
   * @returns The detected language code
   */
  private detectLanguage(text: string | null | undefined): string {
    if (!text) return 'unknown';
    
    // Very simple detection
    // In production, consider using Compact Language Detector or other libraries
    if (/[\u3040-\u30ff\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff\uff66-\uff9f]/.test(text)) {
      return 'ja'; // Japanese
    }
    if (/[\u0600-\u06FF]/.test(text)) {
      return 'ar'; // Arabic
    }
    if (/[\u0400-\u04FF]/.test(text)) {
      return 'ru'; // Russian
    }
    
    // Default to English
    return 'en';
  }
}