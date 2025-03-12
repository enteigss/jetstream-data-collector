import dotenv from 'dotenv';
import { JetstreamCollector, PostgresConfig } from '../src/jetstream-collector';
import { AtpAgent, AtpSessionEvent, AtpSessionData } from '@atproto/api'

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

    const agent = new AtpAgent({
      service: 'https://bsky.social'
    })

    await agent.login({
      identifier: 'vyugen.bsky.social',
      password: process.env.BSKY_PASSWORD as string,
    })
    
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

    // Process events from Jetstream
    
    collector.on('message', async (message) => {
      if (message.commit && message.commit.collection === 'app.bsky.feed.post' && message.commit.operation === 'create') {
        try {
          console.log(`Processing post from ${message.did}`)
          console.log(`Post message being processed: ${JSON.stringify(message, null, 2)}`)
          const record = message.commit.record

          const postData = {
            did: message.did,
            rkey: message.commit.rkey,
            uri: `at://${message.did}/app.bsky.feed.post/${message.commit.rkey}`,
            cid: message.commit.cid,
            text: record.text,
            created_at: new Date(record.createdAt),
            hour_timestamp: new Date(new Date(record.createdAt).setMinutes(0, 0, 0)),
            indexed_at: new Date(),
            text_length: record.text ? record.text.length: 0,
            has_image: record.embed && 
                      (record.embed.$type === 'app.bksy.embed.images' ||
                        record.embed.$type == 'app.bsky.embed.recordWithMedia'),
            image_count: getImageCount(record),
            has_external_link: record.text && record.text.includes('http'),
            has_mention: Boolean(record.text && record.text.includes('@')),
            mention_count: countMentions(record),
            hashtag_count: countHashtags(record),
            reply_to: record.reply ? record.reply.parent.uri: null,
            reply_root: record.reply ? record.reply.root.uri: null,
            thread_depth: record.reply ? 1 : 0,
            language: detectLanguage(record.text),
            raw_data: record
          };

          console.log("Processed post:", postData)
          await storePost(collector, postData)
        } catch (err) {
          console.error('Error processing post:', err)
        }
      }
    });

    collector.on('message', async (message) => {
      if (message.commit && message.commit.collection === 'app.bsky.feed.like') {
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
          
          await storeEngagement(collector, engagementData);
        } catch (err) {
          console.error('Error processing like:', err);
        }
      }
    });
    
    // Process reposts
    collector.on('message', async (message) => {
      if (message.commit && message.commit.collection === 'app.bsky.feed.repost') {
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
          
          await storeEngagement(collector, engagementData);
        } catch (err) {
          console.error('Error processing repost:', err);
        }
      }
    });
    
    // Process follows
    collector.on('message', async (message) => {
      if (message.commit && message.commit.collection === 'app.bsky.graph.follow') {
        try {
          console.log(`Processing follow from ${message.did}`);
          const record = message.commit.record;
          
          const followData = {
            follower_did: message.did,
            followed_did: record.subject,
            created_at: new Date(record.createdAt)
          };
          
          await storeFollow(collector, followData);
        } catch (err) {
          console.error('Error processing follow:', err);
        }
      }
    });
    
    // Process user profiles
    collector.on('message', async (message) => {
      if (message.commit && message.commit.collection === 'app.bsky.actor.profile') {
        try {
          console.log(`Processing profile for ${message.did}`);
          console.log(`Message being processed: ${JSON.stringify(message, null, 2)}`)
          const record = message.commit.record;
          console.log(`Record being processed: ${JSON.stringify(record, null, 2)}`)

          const profileResponse = await agent.api.app.bsky.actor.getProfile({ actor: message.did})
          const profileData = profileResponse.data
          console.log("Full profile data:", profileData)
          const userData = {
            did: message.did,
            handle: profileData.handle, 
            display_name: record.displayName || '',
            description: record.description || '',
            avatar_url: profileData.avatar || '', 
            follower_count: profileData.followersCount || null, 
            following_count: profileData.followsCount || null,
            post_count: profileData.postsCount || null,
            created_at: profileData.createdAt || null
          };
          
          await updateUserProfile(collector, userData);
        } catch (err) {
          console.error('Error processing profile:', err);
        }
      }
    });
    
    // Process account events (handle updates)
    collector.on('account', async (message) => {
      try {
        console.log(`Processing account update for ${message.did}`);
        
        const userData = {
          did: message.did,
          handle: message.handle,
          last_updated_at: new Date()
        };
        
        await updateUserHandle(collector, userData);
      } catch (err) {
        console.error('Error processing account event:', err);
      }
    });
    
    // Set up event listeners
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
      await collector.stop();
      process.exit(0);
    });
    
    process.on('SIGTERM', async () => {
      console.log('Received SIGTERM. Shutting down...');
      await collector.stop();
      process.exit(0);
    });
    
    // Start the collector
    await collector.run();
    
    console.log(`📊 Bluesky data collector started`);
    console.log(`📁 Database: ${pgConfig.database} @ ${pgConfig.host}:${pgConfig.port}`);
    if (wantedCollections) {
      console.log(`🔍 Collecting: ${wantedCollections.join(', ')}`);
    } else {
      console.log(`🔍 Collecting: All collections`);
    }
  } catch (err) {
    console.error('Failed to start collector:', err);
    process.exit(1);
  }
};


function getImageCount(record: any): number {
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

// Count mentions in post text
function countMentions(record: any): number {
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

// Count hashtags in post text
function countHashtags(record: any): number {
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

// Simple language detection - you might want to use a proper library
function detectLanguage(text: string | null | undefined): string {
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

// Get avatar URL from profile
function getAvatarUrl(record: any): string | null {
  if (record.avatar) {
    // Return the avatar blob link
    // In real implementation, you'd resolve this to a proper URL
    return record.avatar;
  }
  return null;
}

async function storePost(collector: JetstreamCollector, postData: any) {
  try {
    const client = await collector.pgPool.connect();

    try {
      await client.query(`
        INSERT INTO posts (
          did, rkey, uri, cid, text, created_at, hour_timestamp, indexed_at,
          text_length, has_image, image_count, has_external_link,
          has_mention, mention_count, hashtag_count, reply_to,
          reply_root, thread_depth, language, raw_data
        ) VALUES (
         $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20
        ) ON CONFLICT (uri) DO UPDATE SET
          text = $5,
          indexed_at = $8,
          text_length = $9,
          has_image = $10,
          image_count = $11,
          has_external_link = $12,
          has_mention = $13,
          mention_count = $14,
          hashtag_count = $15,
          raw_data = $20
        `, [
          postData.did, postData.rkey, postData.uri, postData.cid, postData.text,
          postData.created_at, postData.hour_timestamp, postData.indexed_at, postData.text_length,
          postData.has_image, postData.image_count, postData.has_external_link,
          postData.has_mention, postData.mention_count, postData.hashtag_count,
          postData.reply_to, postData.reply_root, postData.thread_depth,
          postData.language, postData.raw_data
        ]);
    } finally {
      client.release();
    }
  } catch (err) {
    console.error('Error storing post:', err);
  }
}

async function storeEngagement(collector: JetstreamCollector, engagementData: any) {
  try {
    // @ts-ignore - accessing private property
    const client = await collector.pgPool.connect();
    
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
  }
}

// Store follow in database
async function storeFollow(collector: JetstreamCollector, followData: any) {
  try {
    // @ts-ignore - accessing private property
    const client = await collector.pgPool.connect();
    
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
  }
}

// Update user profile in database
async function updateUserProfile(collector: JetstreamCollector, userData: any) {
  try {
    // @ts-ignore - accessing private property
    const client = await collector.pgPool.connect();
    
    try {
      await client.query(`
        INSERT INTO users (
          did, handle, display_name, description, avatar_url,
          follower_count, following_count, post_count
        ) VALUES (
          $1, $2, $3, $4, $5, $6, $7, $8
        ) ON CONFLICT (did) DO UPDATE SET
          handle = $2,
          display_name = $3,
          description = $4,
          avatar_url = $5,
          follower_count = $6,
          following_count = $7,
          post_count = $8
      `, [
        userData.did, userData.handle, userData.display_name, userData.description,
        userData.avatar_url, userData.follower_count, userData.following_count,
        userData.post_count
      ]);
    } finally {
      client.release();
    }
  } catch (err) {
    console.error('Error updating user profile:', err);
  }
}

// Update user handle in database
async function updateUserHandle(collector: JetstreamCollector, userData: any) {
  try {
    // @ts-ignore - accessing private property
    const client = await collector.pgPool.connect();
    
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
  }
}

// Run the collector
run();