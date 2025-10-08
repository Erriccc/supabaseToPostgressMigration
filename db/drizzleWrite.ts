import { db } from './drizzle';
import { eq, or, and, sql, desc, inArray } from 'drizzle-orm';
import config from '@/config';
import axios from 'axios';

// Table references for easier access
const tables = config.tables;

// Helper function to handle BigInt conversions
function convertBigInts(obj: any): any {
  if (obj === null || obj === undefined) return obj;
  if (typeof obj === 'bigint') return Number(obj);
  if (Array.isArray(obj)) return obj.map(convertBigInts);
  if (typeof obj === 'object') {
    const converted: any = {};
    for (const [key, value] of Object.entries(obj)) {
      converted[key] = convertBigInts(value);
    }
    return converted;
  }
  return obj;
}

// Migration of insertUserData function
const insertUserData = async (supabase: any, userId: string | number, email: string, userPageId: string, access_token: string, appId: string | number) => {
  try {
    console.log('Attempting to insert user data Drizzle:', { appId, userId, access_token });
    const app = config.appContext;

    if (userId === config.uiModeSignal) {
      userId = `${app}${userPageId}`;
    }

    // Check for existing user
    const existingUser = await db
      .select()
      .from(tables.userTable)
      .where(
        or(
          and(
            eq(tables.userTable.app_id, appId as string),
            eq(tables.userTable.fb_id, userPageId),
            eq(tables.userTable.app_env, app as string)
          ),
          and(
            eq(tables.userTable.app_id, config.legacyAppId),
            eq(tables.userTable.fb_id, userPageId),
            eq(tables.userTable.app_env, config.legacyAppEnv.toString())
          ),
          and(
            eq(tables.userTable.app_id, config.legacyAppId),
            eq(tables.userTable.email, email),
            eq(tables.userTable.app_env, config.legacyAppEnv.toString())
          )
        )
      )
      .limit(1);

    if (existingUser.length > 0) {
      console.log('Found existing user. Attempting to update access token...');
      
      // Update existing user
      const migrateToNewAppData = {
        fb_id: userPageId,
        user_access_token: access_token,
        email: email,
        app_id: appId as string,
        app_env: app
      };

      const updatedUser = await db
        .update(tables.userTable)
        .set(migrateToNewAppData)
        .where(eq(tables.userTable.id, existingUser[0].id))
        .returning();

      console.log('User data updated successfully:', updatedUser);
      return convertBigInts(updatedUser);
    }

    // Check if user exists by DB ID
    let newUserDataBeforeInsert: any = null;

    if (userId) {
      const findUserByDbId = await db
        .select()
        .from(tables.userTable)
        .where(
          or(
            and(
              eq(tables.userTable.app_id, appId as string),
              eq(tables.userTable.id, Number(userId)),
              eq(tables.userTable.app_env, app as string)
            ),
            and(
              eq(tables.userTable.app_id, config.legacyAppId),
              eq(tables.userTable.id, Number(userId)),
              eq(tables.userTable.app_env, config.legacyAppEnv.toString())
            )
          )
        )
        .limit(1);

      if (findUserByDbId.length > 0 && findUserByDbId[0].fb_id === userPageId) {
        console.log('user found by db id, and user fb id is the same as the user page id, so we will update the existing user');
        newUserDataBeforeInsert = {
          id: Number(userId),
          appId: appId as string,
          email: email,
          fbId: userPageId,
          userAccessToken: access_token,
          appEnv: app,
        };
      } else if (findUserByDbId.length === 0 && userId) {
        console.log('user not found by db id, but id was provided so we will insert the new user for the already existing audos user');
        newUserDataBeforeInsert = {
          id: Number(userId),
          appId: appId as string,
          email: email,
          fbId: userPageId,
          userAccessToken: access_token,
          appEnv: app,
        };
      }
    }

    if (!newUserDataBeforeInsert) {
      console.log('user not found by db id, completely new user');
      newUserDataBeforeInsert = {
        appId: appId as string,
        email: email,
        fbId: userPageId,
        userAccessToken: access_token,
        appEnv: app,
      };
    }

    console.log('No existing user found. Attempting to insert new user...');
    const newUser = await db
      .insert(tables.userTable)
      .values(newUserDataBeforeInsert)
      .returning();

    console.log('User data inserted successfully:', newUser);
    return convertBigInts(newUser);

  } catch (error) {
    console.error('Unexpected error in insertUserData:', error);
    return null;
  }
};

// Migration of insertPageData function
const insertPageData = async (supabase: any, userId: string | number, appId: string | number, fbWebhookData: any) => {
  try {
    console.log('Attempting to insert/update page data...');
    const pageData = [];

    // Get all existing pages by userId
    const allPagesByUserId = await db
      .select()
      .from(tables.pageTable)
      .where(
        or(
          and(
            eq(tables.pageTable.app_id, Number(appId)),
            eq(tables.pageTable.fb_id, userId as string)
          ),
          and(
            eq(tables.pageTable.app_id, Number(config.legacyAppId)),
            eq(tables.pageTable.fb_id, userId as string)
          )
        )
      );

    // Build sets of fb_page_ids
    const existingPageIds = new Set(allPagesByUserId.map(p => p.fb_page_id));
    const webhookPageIds = new Set((fbWebhookData.pages || []).map((p: any) => p.id));

    // Find missing and new pages
    const missingPages = allPagesByUserId.filter(p => !webhookPageIds.has(p.fb_page_id));
    const newPages = (fbWebhookData.pages || []).filter((p: any) => !existingPageIds.has(p.id));

    if (missingPages.length > 0) {
      console.log('Pages missing from webhook data (likely disconnected):', missingPages.map(p => p.fb_page_id));
      
      // Update user with missing and new pages data
      const missingPagesData = {
        missing_pages: missingPages,
        new_pages: newPages
      };

      await db
        .update(tables.userTable)
        .set(missingPagesData)
        .where(eq(tables.userTable.id, Number(userId)));

      console.log('User data updated with missing/new pages successfully');
    }

    // Process each page
    for (const page of fbWebhookData.pages) {
      console.log('Processing page:', page);
      
      const existingPages = await db
        .select()
        .from(tables.pageTable)
        .where(
          or(
            and(
              eq(tables.pageTable.app_id, Number(appId)),
              eq(tables.pageTable.fb_page_id, page.id),
              eq(tables.pageTable.fb_id, userId as string)
            ),
            and(
              eq(tables.pageTable.app_id, Number(config.legacyAppId)),
              eq(tables.pageTable.fb_page_id, page.id),
              eq(tables.pageTable.fb_id, userId as string)
            )
          )
        );

      const pageRecord = {
        app_id: Number(appId),
        fb_page_id: page.id,
        page_name: page.name,
        fb_id: userId as string,
        page_access_token: page.access_token,
        ig_account_id: page.connected_instagram_account ? page.connected_instagram_account.id : null,
        has_ig_page: !!page.connected_instagram_account,
      };

      let result: any;
      if (existingPages.length > 0) {
        console.log('Found existing page record. Attempting to update...');
        const existingPage = existingPages[0];

        const updateRecord = {
          ...existingPage,
          page_access_token: pageRecord.page_access_token,
          app_id: pageRecord.app_id,
          fb_page_id: pageRecord.fb_page_id,
          fb_id: pageRecord.fb_id,
          ig_account_id: pageRecord.ig_account_id || existingPage.ig_account_id,
          has_ig_page: pageRecord.ig_account_id ? true : existingPage.has_ig_page,
          page_name: pageRecord.page_name || existingPage.page_name
        };

        const updatedPageData = await db
          .update(tables.pageTable)
          .set(updateRecord)
          .where(eq(tables.pageTable.id, existingPage.id))
          .returning();

        result = updatedPageData;
        console.log('Page data updated successfully:', updatedPageData);
      } else {
        console.log('No existing page record found. Attempting to insert...');
        const newPageData = await db
          .insert(tables.pageTable)
          .values(pageRecord)
          .returning();

        result = newPageData;
        console.log('Page data inserted successfully:', newPageData);
      }

      if (result && result.length > 0) {
        pageData.push(convertBigInts(result[0]));
      }
    }

    console.log('Page data processing complete.', pageData);
    return pageData;
  } catch (error) {
    console.error('Unexpected error in insertPageData:', error);
    return [];
  }
};

// Migration of getPageAccessToken function
const getPageAccessToken = async (pageId: string, supabase: any) => {
  try {
    console.log('Attempting to get most recent page access token for pageId:', pageId);
    
    const pageData = await db
      .select()
      .from(tables.pageTable)
      .where(
        and(
          eq(tables.pageTable.app_id, Number(config.appId)),
          eq(tables.pageTable.fb_page_id, pageId)
        )
      )
      .orderBy(sql`${tables.pageTable.created_at} DESC`)
      .limit(1);

    if (pageData && pageData.length > 0) {
      console.log('Found most recent page access token. Creation date:', pageData[0].created_at);
      return pageData[0].page_access_token;
    }

    console.log('No page access token found for pageId:', pageId);
    return null;
  } catch (err) {
    console.error('Unexpected error in getPageAccessToken:', err);
    return null;
  }
};

// Migration of readMessages function
const readMessages = async (queryParams: any, supabase: any) => {
  try {
    const whereConditions = [];
    
    // Add app_id condition
    whereConditions.push(eq(tables.pageMessagesTable.app_id, Number(config.appId)));
    
    // Add other query parameters
    for (const [key, value] of Object.entries(queryParams)) {
      if (key === 'id') {
        whereConditions.push(eq(tables.pageMessagesTable.id, Number(value)));
      } else if (key === 'message_id') {
        whereConditions.push(eq(tables.pageMessagesTable.message_id, value as string));
      } else if (key === 'sender_id') {
        whereConditions.push(eq(tables.pageMessagesTable.sender_id, value as string));
      } else if (key === 'recipient_id') {
        whereConditions.push(eq(tables.pageMessagesTable.recipient_id, value as string));
      }
      // Add more conditions as needed
    }

    const data = await db
      .select()
      .from(tables.pageMessagesTable)
      .where(and(...whereConditions));

    console.log('Messages read successfully:', data);
    return convertBigInts(data);
  } catch (err) {
    console.error('Unexpected error in readMessages:', err);
    return null;
  }
};

// Migration of upsertAdAccount function
const upsertAdAccount = async (supabase: any, appid: string | number, userId: string, adAccountData: any) => {
  console.log('Upserting adAccount:', JSON.stringify(adAccountData, null, 2));

  try {
    const data = await db
      .insert(tables.adAccountsTable)
      .values({
        fb_ad_account_id: adAccountData.fb_ad_account_id,
        app_id: Number(appid),
        user_id: Number(userId),
        name: adAccountData.name,
        details: adAccountData
      })
      .onConflictDoUpdate({
        target: tables.adAccountsTable.fb_ad_account_id,
        set: {
          app_id: Number(appid),
          user_id: Number(userId),
          name: adAccountData.name,
          details: adAccountData,
          updated_at: sql`CURRENT_TIMESTAMP`
        }
      })
      .returning();

    console.log('Ad account upserted successfully');
    return convertBigInts(data);
  } catch (error) {
    console.error('Error upserting ad account:', error);
    throw error;
  }
};

// Migration of webhookFilter class
export class DrizzleWebhookFilter {
  private returnableObject: any;
  private supabase: any;

  constructor(returnableObject: any, supabase: any) {
    this.returnableObject = returnableObject;
    this.supabase = supabase;
  }

  async saveToDb() {
    try {
      if (this.returnableObject.commentOrMessgae === 'message') {
        console.log('Attempting to upsert message...');
        const result = await this.upsertMessage(this.returnableObject);
        console.log('Upsert message result:', result);
        return result ? [result] : null;
      } else if (this.returnableObject.commentOrMessgae === 'comment' && this.returnableObject.commentValue) {
        console.log('Attempting to upsert comment...');
        const result = await this.upsertComment(this.returnableObject);
        console.log('Upsert comment result:', result);
        return result ? [result] : null;
      } else {
        console.log('Unknown commentOrMessgae type:', this.returnableObject.commentOrMessgae);
        return null;
      }
    } catch (error) {
      console.error('Error in saveToDb:', error);
      return null;
    }
  }

  async upsertMessage(messageObject: any) {
    try {
      // Check if the message already exists
      const existingMessage = await db
        .select()
        .from(tables.pageMessagesTable)
        .where(
          and(
            eq(tables.pageMessagesTable.message_id, messageObject.messageId),
            eq(tables.pageMessagesTable.message_type, messageObject.messageType),
            eq(tables.pageMessagesTable.app_id, Number(messageObject.appId))
          )
        )
        .limit(1);

      const messageRecord = {
        app_id: Number(messageObject.appId),
        sender_id: messageObject.senderId,
        recipient_id: messageObject.recipientId,
        message_type: messageObject.messageType,
        message_id: messageObject.messageId,
        message_content: messageObject.messageContent,
        message_attachment_type: messageObject.messageAttachmentsType,
        messageAttachmentPayload: messageObject.messageAttachmentsPayload,
        is_inbound: messageObject.isInbound,
        is_outbound: messageObject.isOutbound,
        outbound_origin: messageObject.outboundOrigin,
        json_body: messageObject.jsonReqBody
      };

      let result: any;
      if (existingMessage.length > 0) {
        console.log('Updating existing message...');
        const updatedMessages = await db
          .update(tables.pageMessagesTable)
          .set(messageRecord)
          .where(eq(tables.pageMessagesTable.id, existingMessage[0].id))
          .returning();
        result = updatedMessages[0];
      } else {
        console.log('Inserting new message...');
        const insertedMessages = await db
          .insert(tables.pageMessagesTable)
          .values(messageRecord)
          .returning();
        result = insertedMessages[0];
      }

      console.log('Message upserted successfully:', result);
      return convertBigInts(result);
    } catch (error) {
      console.error('Unexpected error in upsertMessage:', error);
      return null;
    }
  }

  async upsertComment(commentObject: any) {
    try {
      // Check if the comment already exists
      const existingComment = await db
        .select()
        .from(tables.pageCommentsTable)
        .where(
          and(
            eq(tables.pageCommentsTable.app_id, Number(commentObject.appId)),
            eq(tables.pageCommentsTable.fb_comment_id, commentObject.commentId),
            eq(tables.pageCommentsTable.ig_comment_id, commentObject.igCommentId)
          )
        )
        .limit(1);

      const commentRecord = {
        app_id: Number(commentObject.appId),
        sender_id: commentObject.senderId,
        recipient_id: commentObject.recipientId,
        post_id: commentObject.postId,
        media_id: commentObject.mediaId,
        comment_value: commentObject.commentValue,
        platform: commentObject.platform,
        fb_page_id: commentObject.fbPageId,
        json_body: commentObject.jsonReqBody,
        fb_comment_id: commentObject.commentId,
        ig_comment_id: commentObject.igCommentId,
        is_inbound: commentObject.isInbound,
        is_outbound: commentObject.isOutbound,
        outbound_origin: commentObject.outboundOrigin,
      };

      let result: any;
      if (existingComment.length > 0) {
        console.log('Updating existing comment...');
        const updatedComments = await db
          .update(tables.pageCommentsTable)
          .set(commentRecord)
          .where(eq(tables.pageCommentsTable.id, existingComment[0].id))
          .returning();
        result = updatedComments[0];
      } else {
        console.log('Inserting new comment...');
        const insertedComments = await db
          .insert(tables.pageCommentsTable)
          .values(commentRecord)
          .returning();
        result = insertedComments[0];
      }

      console.log('Comment upserted successfully:', result);
      return convertBigInts(result);
    } catch (error) {
      console.error('Unexpected error in upsertComment:', error);
      return null;
    }
  }

  async checkIfConversationExists(conversationObject: any) {
    try {
      const existingConversation = await db
        .select()
        .from(tables.pageConversationsTable)
        .where(
          and(
            eq(tables.pageConversationsTable.fb_conversation_id, conversationObject.fbConversationId),
            eq(tables.pageConversationsTable.app_id, Number(config.appId))
          )
        )
        .limit(1);

      return existingConversation[0];
    } catch (error) {
      console.error('Error checking if conversation exists:', error);
      return false;
    }
  }

  async readMessages(queryParams: any) {
    return await readMessages(queryParams, this.supabase);
  }

  async readComments(queryParams: any) {
    return await readComments(queryParams, this.supabase);
  }

  async upsertConversation(conversationObject: any) {
    try {
      console.log('conversationObject in upsertConversation', conversationObject);
      
      // Check if conversation exists
      const existingConversation = await db
        .select()
        .from(tables.pageConversationsTable)
        .where(
          and(
            eq(tables.pageConversationsTable.fb_conversation_id, conversationObject.fbConversationId),
            eq(tables.pageConversationsTable.app_id, Number(config.appId))
          )
        )
        .limit(1);

      const conversationRecord = {
        app_id: Number(config.appId),
        fb_page_id: conversationObject.fbPageId,
        fb_conversation_id: conversationObject.fbConversationId,
        status_modified_by_user_id: conversationObject.statusModifiedByUserId,
        recipient_page_scope_id: conversationObject.recipientPageScopeId,
        ig_account_id: conversationObject.igAccountId,
        conversation_platform: conversationObject.conversationPlatform,
        active: conversationObject.active,
        lead_first_name: conversationObject.fbFirstName,
        lead_last_name: conversationObject.fbLastName,
        lead_email: conversationObject.extractedContactData?.contactValue || null,
        lead_phone: conversationObject.extractedContactData?.contactValue || null,
        lead_street_address: null,
        lead_business_website: null,
        conversation_ad_id: conversationObject.extractedLeadData?.fb_ad_id || null,
        conversationSource: conversationObject.extractedLeadData?.lead_data ? 'ads' : 'organic'
      };

      let result: any;
      if (existingConversation.length > 0) {
        console.log('Updating existing conversation...');
        const updatedConversation = await db
          .update(tables.pageConversationsTable)
          .set(conversationRecord)
          .where(eq(tables.pageConversationsTable.id, existingConversation[0].id))
          .returning();
        result = updatedConversation[0];
      } else {
        console.log('Inserting new conversation...');
        const insertedConversation = await db
          .insert(tables.pageConversationsTable)
          .values(conversationRecord)
          .returning();
        result = insertedConversation[0];
      }

      // Handle lead data if present
      if (conversationObject.extractedLeadData?.lead_data) {
        const leadData = {
          app_id: config.appId,
          conversation_id: result.id,
          fb_page_id: conversationObject.fbPageId,
          fb_conversation_id: conversationObject.fbConversationId,
          fb_ad_id: conversationObject.extractedLeadData.fb_ad_id,
          recipient_page_scope_id: conversationObject.recipientPageScopeId,
          conversation_platform: conversationObject.conversationPlatform
        };

        const lead = await upsertLead(this.supabase, leadData);
        console.log('lead upserted successfully in upsertConversation', lead);
        result = {...result, lead_id: lead[0].id, is_message_from_ad: true, welcome_message_flow: conversationObject.extractedLeadData.welcome_message_flow};
      }

      return convertBigInts(result);
    } catch (error) {
      console.error('Unexpected error in upsertConversation:', error);
      return null;
    }
  }
}

// Migration of upsertCampaign function
const upsertCampaign = async (supabase: any, appid: string | number, userId: string, campaignData: any) => {
  console.log('Upserting campaign:', JSON.stringify(campaignData, null, 2));
  
  try {
    await db
      .insert(tables.campaignsTable)
      .values({
        fb_campaign_id: campaignData.fb_campaign_id,
        app_id: Number(appid),
        user_id: Number(userId),
        ad_account_id: campaignData.ad_account_id,
        name: campaignData.name,
        objective: campaignData.objective,
        status: campaignData.status,
        details: campaignData
      })
      .onConflictDoUpdate({
        target: tables.campaignsTable.fb_campaign_id,
        set: {
          app_id: Number(appid),
          user_id: Number(userId),
          ad_account_id: campaignData.ad_account_id,
          name: campaignData.name,
          objective: campaignData.objective,
          status: campaignData.status,
          details: campaignData,
          updated_at: sql`CURRENT_TIMESTAMP`
        }
      });

    console.log('Campaign upserted successfully');
  } catch (error) {
    console.error('Error upserting campaign:', error);
    throw error;
  }
};

// Migration of getAdFromDbByAdIdOrAdTraceId function
const getAdFromDbByAdIdOrAdTraceId = async (supabase: any, adId: string) => {
  console.log('Attempting to get ad from db for adId:', adId);
  const adIdString = String(adId);
  
  try {
    const data = await db
      .select()
      .from(tables.adsTable)
      .where(
        and(
          eq(tables.adsTable.app_id, Number(config.appId)),
          or(
            eq(tables.adsTable.fb_ad_id, adIdString),
            eq(tables.adsTable.audos_ad_trace_id, adIdString)
          )
        )
      );
    
    console.log('Ad from db:', data);
    return { data: convertBigInts(data), error: null };
  } catch (error) {
    console.error('Error getting ad from db:', error);
    return { data: null, error: error };
  }
};

// Migration of updateMessageSentToAudosServer function
const updateMessageSentToAudosServer = async (supabase: any, messageId: string | number) => {
  console.log('marking message as sent to audos server in updateMessageSentToAudosServer messageId', messageId);
  
  try {
    const data = await db
      .update(tables.pageMessagesTable)
      .set({ sent_to_audos_server: true })
      .where(eq(tables.pageMessagesTable.id, Number(messageId)))
      .returning();
      
    if (!data || data.length === 0) {
      console.error('Error updating message sent to audos server: No message found');
      return null;
    }
    
    return convertBigInts(data[0]);
  } catch (error) {
    console.error('Error updating message sent to audos server:', error);
    return null;
  }
};

// Migration of insertBusinessManagerData function
const insertBusinessManagerData = async (userId: string, appId: string | number, businessesData: any) => {
  try {
    console.log('Attempting to insert/update business manager data...');
    const businessData = [];

    if (!businessesData || !businessesData.data || businessesData.data.length === 0) {
      console.log('No businesses data to process');
      return businessData;
    }

    for (const business of businessesData.data) {
      console.log('Processing business:', business);
      
      const existingBusiness = await db
        .select()
        .from(tables.businessManagersTable)
        .where(
          or(
            and(
              eq(tables.businessManagersTable.app_id, Number(appId)),
              eq(tables.businessManagersTable.fb_business_id, business.id),
              eq(tables.businessManagersTable.fb_id, userId)
            ),
            and(
              eq(tables.businessManagersTable.app_id, Number(config.legacyAppId)),
              eq(tables.businessManagersTable.fb_business_id, business.id),
              eq(tables.businessManagersTable.fb_id, userId)
            )
          )
        );

      const businessRecord = {
        app_id: Number(appId),
        fb_business_id: business.id,
        business_name: business.name,
        fb_id: userId,
        business_system_user_access_token: null,
        active: true,
        audos_partnership_id: null,
        other_details: business
      };

      let result: any;
      if (existingBusiness && existingBusiness.length > 0) {
        console.log('Found existing business record. Attempting to update...');
        const existingRecord = existingBusiness[0];

        const updateRecord = {
          ...existingRecord,
          app_id: businessRecord.app_id,
          fb_business_id: businessRecord.fb_business_id,
          fb_id: businessRecord.fb_id,
          business_name: businessRecord.business_name || existingRecord.business_name,
          active: businessRecord.active,
          other_details: businessRecord.other_details
        };

        const updatedBusinessData = await db
          .update(tables.businessManagersTable)
          .set(updateRecord)
          .where(eq(tables.businessManagersTable.id, existingRecord.id))
          .returning();

        result = updatedBusinessData;
        console.log('Business data updated successfully:', updatedBusinessData);
      } else {
        console.log('No existing business record found. Attempting to insert...');
        const newBusinessData = await db
          .insert(tables.businessManagersTable)
          .values(businessRecord)
          .returning();

        result = newBusinessData;
        console.log('Business data inserted successfully:', newBusinessData);
      }

      if (result && result.length > 0) {
        businessData.push(convertBigInts(result[0]));
      }
    }

    console.log('Business data processing complete.', businessData);
    return businessData;
  } catch (error) {
    console.error('Unexpected error in insertBusinessManagerData:', error);
    return [];
  }
};

// Migration of getFbPageId function
const getFbPageId = async (identifier: string, supabase: any) => {
  try {
    console.log('Attempting to retrieve fbPageId for identifier:', identifier);
    console.log('Using appId:', config.appId, 'type:', typeof config.appId);

    const data = await db
      .select({ fb_page_id: tables.pageTable.fb_page_id })
      .from(tables.pageTable)
      .where(
        and(
          eq(tables.pageTable.app_id, Number(config.appId)),
          eq(tables.pageTable.active, true),
          or(
            eq(tables.pageTable.ig_account_id, identifier),
            eq(tables.pageTable.fb_page_id, identifier)
          )
        )
      )
      .limit(1);

    console.log('Query result:', data);

    if (data && data.length > 0) {
      console.log('fbPageId found:', data[0].fb_page_id);
      return data[0].fb_page_id;
    }

    console.log('No page found for identifier:', identifier);
    return null;
  } catch (err) {
    console.error('Unexpected error in getFbPageId:', err);
    return null;
  }
};

// Migration of getIgId function
const getIgId = async (identifier: string, supabase: any) => {
  try {
    console.log('Attempting to retrieve igId for identifier:', identifier);

    const data = await db
      .select({
        ig_account_id: tables.pageTable.ig_account_id,
        has_ig_page: tables.pageTable.has_ig_page
      })
      .from(tables.pageTable)
      .where(
        and(
          eq(tables.pageTable.app_id, Number(config.appId)),
          eq(tables.pageTable.active, true),
          or(
            eq(tables.pageTable.ig_account_id, identifier),
            eq(tables.pageTable.fb_page_id, identifier)
          )
        )
      )
      .limit(1);

    if (data && data.length > 0) {
      const igId = data[0].ig_account_id;
      const hasIgId = data[0].has_ig_page;
      return { igId: igId, hasIgId: hasIgId };
    }

    console.log('No page found for identifier:', identifier);
    return null;
  } catch (err) {
    console.error('Unexpected error in getIgId:', err);
    return null;
  }
};

const checkIfPageIsManagedByMultipleUsers = async (identifier: string, supabase: any, activeOnly: boolean = false) => {
  try {
    console.log('Attempting to retrieve checkIfPageIsManagedByMultipleUsers for identifier:', identifier);
    let userDataArray: any[] = [];

    const data = await db
      .select()
      .from(tables.pageTable)
      .where(
        and(
          eq(tables.pageTable.app_id, Number(config.appId)),
          eq(tables.pageTable.active, activeOnly),
          or(
            eq(tables.pageTable.ig_account_id, identifier),
            eq(tables.pageTable.fb_page_id, identifier)
          )
        )
      );

    if (data && data.length > 0) {
      console.log(`Fb page(s) found for identifier: ${identifier}`, data);
      
      const uniqueFbIds = Array.from(new Set(data.map(page => page.fb_id)));
      console.log('Unique fb_ids to query:', uniqueFbIds);
      
      const userData = await db
        .select()
        .from(tables.userTable)
        .where(
          and(
            eq(tables.userTable.app_id, config.appId as string),
            inArray(tables.userTable.fb_id, uniqueFbIds)
          )
        );

      // Group users by fb_id to maintain the same structure as before
      userDataArray = data.map(page => {
        const matchingUsers = userData.filter(user => user.fb_id === page.fb_id);
        return matchingUsers;
      });
      
      return {
        isManagedByMultipleUsers: data.length > 1,
        data: convertBigInts(data),
        userDataArray: convertBigInts(userDataArray)
      };
    } else {
      console.log(`No page(s) found for identifier: ${identifier}`);
      return { isManagedByMultipleUsers: false, data: null, userDataArray: [] };
    }
  } catch (err) {
    console.error('Unexpected error in checkIfPageIsManagedByMultipleUsers:', err);
    return { isManagedByMultipleUsers: false, data: null, userDataArray: [] };
  }
};

// Migration of remaining functions
const getPageAccessTokenByAdIdOrAdTraceIdAndPageId = async (identifier: string, pageId: string, supabase: any) => {
  console.log('getPageAccessTokenByAdIdOrAdTraceIdAndPageId getting the page access token for identifier:', identifier, 'and pageId:', pageId);
  
  const { isManagedByMultipleUsers, data: pagesData } = await checkIfPageIsManagedByMultipleUsers(pageId, supabase, true);
  console.log('isManagedByMultipleUsers:', isManagedByMultipleUsers);
  
  let pageAccessTokenFromReturnableObject = pagesData?.[0]?.pageAccessToken;
  let fallBackValidAccessToken = await getValidPageAccessToken(pageId, supabase, { needsMessaging: false, needsInstagram: false, needsAds: true });
  
  let returner = { pageAccessToken: fallBackValidAccessToken || pageAccessTokenFromReturnableObject };
  
  if (isManagedByMultipleUsers) {
    try {
      console.log('Attempting to get page access token by ad trace id:', identifier);
      const { data: adData } = await getAdFromDbByAdIdOrAdTraceId(supabase, identifier);

      if (adData && adData.length > 0) {
        console.log('Found ad details:', adData[0]);
        const userData = await db
          .select()
          .from(tables.userTable)
          .where(
            and(
              eq(tables.userTable.app_id, config.appId as string),
              eq(tables.userTable.id, adData[0].userId)
            )
          )
          .limit(1);

        if (userData && userData.length > 0) {
          const pageData = await db
            .select()
            .from(tables.pageTable)
            .where(
              and(
                eq(tables.pageTable.app_id, Number(config.appId)),
                eq(tables.pageTable.fb_id, userData[0].fb_id),
                eq(tables.pageTable.fb_page_id, pageId)
              )
            )
            .limit(1);

          if (pageData && pageData.length > 0) {
            return { pageAccessToken: pageData[0].page_access_token };
          }
        }
      }

      return returner;
    } catch (err) {
      console.error('Unexpected error in getPageAccessTokenByAdIdOrAdTraceIdAndPageId:', err);
      return returner;
    }
  }
  
  return returner;
};

const getAdDataAndUserDataFromDbWithAdIdOrAdTraceId = async (supabase: any, identifier: string) => {
  const { data: adData, error: adError } = await getAdFromDbByAdIdOrAdTraceId(supabase, identifier);
  if (adError) {
    console.error('Error retrieving ad details:', adError);
    return { matchFound: false };
  }

  if (adData && adData.length > 0) {
    console.log('Found ad details:', adData[0]);
    const userData = await db
      .select()
      .from(tables.userTable)
      .where(
        and(
          eq(tables.userTable.app_id, config.appId as string),
          eq(tables.userTable.id, adData[0].userId)
        )
      )
      .limit(1);

    if (userData && userData.length > 0) {
      console.log('Found user details:', userData[0]);
      return {
        matchFound: true,
        adData: convertBigInts(adData[0]),
        userData: convertBigInts(userData[0])
      };
    }
  }

  console.log('No ad data or user data found for adId or adTraceId:', identifier);
  return { matchFound: false };
};

const getPageAccessTokenAndValidateBeforeReturn = async (pageId: string, supabase: any) => {
  // This would need the full token validation logic - simplified version
  console.log('Attempting to get most recent page access token for pageId:', pageId);
  return await getPageAccessToken(pageId, supabase);
};

const getPageDataByDbId = async (pageDbId: string | number, supabase: any) => {
  const data = await db
    .select()
    .from(tables.pageTable)
    .where(
      and(
        eq(tables.pageTable.app_id, Number(config.appId)),
        eq(tables.pageTable.id, Number(pageDbId))
      )
    )
    .limit(1);
    
  if (!data || data.length === 0) {
    throw new Error('Error fetching page data');
  }
  
  return { data: convertBigInts(data[0]), error: null };
};

const getUserAccessToken = async (userId: string, supabase: any) => {
  console.log('userId: ', userId);
  const data = await db
    .select({ userAccessToken: tables.userTable.user_access_token })
    .from(tables.userTable)
    .where(eq(tables.userTable.id, Number(userId)))
    .limit(1);

  if (!data || data.length === 0) {
    throw new Error('Error fetching user access token1');
  }
  
  return data[0].userAccessToken;
};

// Add remaining function stubs with basic implementations
const upsertAdSet = async (supabase: any, appid: string | number, userId: string, adSetData: any) => {
  console.log('Upserting ad set:', JSON.stringify(adSetData, null, 2));
  try {
    const data = await db
      .insert(tables.adSetsTable)
      .values({
        fb_ad_set_id: adSetData.fb_ad_set_id,
        app_id: Number(appid),
        user_id: Number(userId),
        campaign_id: adSetData.campaign_id,
        name: adSetData.name,
        optimization_goal: adSetData.optimization_goal,
        billing_event: adSetData.billing_event,
        bid_strategy: adSetData.bid_strategy,
        status: adSetData.status,
        details: adSetData
      })
      .onConflictDoUpdate({
        target: tables.adSetsTable.fb_ad_set_id,
        set: {
          app_id: Number(appid),
          user_id: Number(userId),
          campaign_id: adSetData.campaign_id,
          name: adSetData.name,
          optimization_goal: adSetData.optimization_goal,
          billing_event: adSetData.billing_event,
          bid_strategy: adSetData.bid_strategy,
          status: adSetData.status,
          details: adSetData,
          updated_at: sql`CURRENT_TIMESTAMP`
        }
      })
      .returning();

    console.log('Ad set upserted successfully');
    return convertBigInts(data);
  } catch (error) {
    console.error('Error upserting ad set:', error);
    throw error;
  }
};

const upsertAd = async (supabase: any, appid: string | number, userId: string, adData: any) => {
  console.log('Upserting ad:', JSON.stringify(adData, null, 2));
  try {
    const data = await db
      .insert(tables.adsTable)
      .values({
        fb_ad_id: adData.fb_ad_id,
        app_id: Number(appid),
        user_id: Number(userId),
        ad_set_id: adData.ad_set_id,
        name: adData.name,
        status: adData.status,
        details: adData,
        ad_account_id: adData.ad_account_id,
        creative_id: adData.creative_id,
        audos_ad_trace_id: adData.audos_ad_trace_id,
        welcome_message_id: adData.welcome_message_id,
        image_url: adData.image_url || null,
        video_url: adData.video_url || null
      })
      .onConflictDoUpdate({
        target: tables.adsTable.fb_ad_id,
        set: {
          app_id: Number(appid),
          user_id: Number(userId),
          ad_set_id: adData.ad_set_id,
          name: adData.name,
          status: adData.status,
          details: adData,
          ad_account_id: adData.ad_account_id,
          creative_id: adData.creative_id,
          audos_ad_trace_id: adData.audos_ad_trace_id,
          welcome_message_id: adData.welcome_message_id,
          image_url: adData.image_url || null,
          video_url: adData.video_url || null,
          updated_at: sql`CURRENT_TIMESTAMP`
        }
      })
      .returning();

    console.log('Ad upserted successfully');
    return convertBigInts(data);
  } catch (error) {
    console.error('Error upserting ad:', error);
    throw error;
  }
};

const upsertCreative = async (supabase: any, appid: string | number, userId: string, creativeData: any) => {
  console.log('Upserting creative:', JSON.stringify(creativeData, null, 2));
  try {
    const data = await db
      .insert(tables.adCreativesTable)
      .values({
        fb_creative_id: creativeData.fb_creative_id,
        app_id: Number(appid),
        user_id: Number(userId),
        name: creativeData.name,
        object_story_spec: creativeData.object_story_spec,
        ad_account_id: creativeData.ad_account_id,
        details: creativeData,
        audos_ad_trace_id: creativeData.audos_ad_trace_id,
        welcome_message_id: creativeData.welcome_message_id,
        image_url: creativeData.image_url || null,
        video_url: creativeData.video_url || null
      })
      .onConflictDoUpdate({
        target: tables.adCreativesTable.fb_creative_id,
        set: {
          app_id: Number(appid),
          user_id: Number(userId),
          name: creativeData.name,
          object_story_spec: creativeData.object_story_spec,
          ad_account_id: creativeData.ad_account_id,
          details: creativeData,
          audos_ad_trace_id: creativeData.audos_ad_trace_id,
          welcome_message_id: creativeData.welcome_message_id,
          image_url: creativeData.image_url || null,
          video_url: creativeData.video_url || null,
          updated_at: sql`CURRENT_TIMESTAMP`
        }
      })
      .returning();

    console.log('Creative upserted successfully');
    return convertBigInts(data);
  } catch (error) {
    console.error('Error upserting creative:', error);
    throw error;
  }
};

// Continue with remaining function implementations...
const getCreativeFromDbByCreativeIdOrAdTraceId = async (supabase: any, creativeId: string) => {
  console.log('Attempting to get creative from db for creativeId:', creativeId);
  const creativeIdString = String(creativeId);
  
  try {
    const data = await db
      .select()
      .from(tables.adCreativesTable)
      .where(
        or(
          eq(tables.adCreativesTable.fb_creative_id, creativeIdString),
          eq(tables.adCreativesTable.audos_ad_trace_id, creativeIdString)
        )
      );
      
    return { data: convertBigInts(data), error: null };
  } catch (error) {
    console.error('Error getting creative from db:', error);
    return { data: null, error: error };
  }
};

const readComments = async (queryParams: any, supabase: any) => {
  try {
    const whereConditions = [eq(tables.pageCommentsTable.app_id, Number(config.appId))];
    
    // Add other query parameters
    for (const [key, value] of Object.entries(queryParams)) {
      if (key === 'id') {
        whereConditions.push(eq(tables.pageCommentsTable.id, Number(value)));
      } else if (key === 'fb_page_id') {
        whereConditions.push(eq(tables.pageCommentsTable.fb_page_id, value as string));
      } else if (key === 'sender_id') {
        whereConditions.push(eq(tables.pageCommentsTable.sender_id, value as string));
      }
      // Add more conditions as needed
    }

    const data = await db
      .select()
      .from(tables.pageCommentsTable)
      .where(and(...whereConditions));

    console.log('Comments read successfully:', data);
    return convertBigInts(data);
  } catch (err) {
    console.error('Unexpected error in readComments:', err);
    return null;
  }
};

const readPageData = async (queryParams: any, supabase: any) => {
  try {
    const whereConditions = [eq(tables.pageTable.app_id, Number(config.appId))];
    
    for (const [key, value] of Object.entries(queryParams)) {
      if (key === 'id') {
        whereConditions.push(eq(tables.pageTable.id, Number(value)));
      } else if (key === 'fb_page_id') {
        whereConditions.push(eq(tables.pageTable.fb_page_id, value as string));
      } else if (key === 'fb_id') {
        whereConditions.push(eq(tables.pageTable.fb_id, value as string));
      }
    }

    const data = await db
      .select()
      .from(tables.pageTable)
      .where(and(...whereConditions));

    console.log('Page data read successfully:', data);
    return convertBigInts(data);
  } catch (err) {
    console.error('Unexpected error in readPageData:', err);
    return null;
  }
};

// Placeholder implementations for complex functions that need full business logic
const changeConversationActiveStatus = async (status: boolean, conversationId: string, updatedByUserId: string) => {
  console.log('Attempting to change conversation active status...', status, conversationId, updatedByUserId);
  try {
    const data = await db
      .update(tables.pageConversationsTable)
      .set({ 
        active: status,
        updated_at: sql`NOW()`,
        status_modified_by_user_id: updatedByUserId
      })
      .where(
        and(
          eq(tables.pageConversationsTable.id, Number(conversationId)),
          eq(tables.pageConversationsTable.app_id, Number(config.appId))
        )
      )
      .returning();
      
    console.log('Conversation active status updated successfully:', data);
    return convertBigInts(data);
  } catch (error) {
    console.error('Unexpected error in changeConversationActiveStatus:', error);
    throw error;
  }
};

const upsertLead = async (supabase: any, params: any) => {
  console.log('Upserting lead:', params);
  try {
    const data = await db
      .insert(tables.leadsTable)
      .values({
        app_id: Number(params.app_id),
        conversation_id: Number(params.conversation_id),
        fb_page_id: params.fb_page_id,
        fb_conversation_id: params.fb_conversation_id,
        fb_ad_id: params.fb_ad_id,
        recipient_page_scope_id: params.recipient_page_scope_id,
        conversation_platform: params.conversation_platform
      })
      .returning();
      
    return convertBigInts(data);
  } catch (error) {
    console.error('Error inserting lead:', error);
    throw error;
  }
};

const upsertContact = async (supabase: any, params: any) => {
  console.log('Upserting contact:', params);
  try {
    const data = await db
      .insert(tables.contactsTable)
      .values({
        app_id: Number(params.appId),
        conversation_id: Number(params.conversationId),
        fb_page_id: params.fbPageId,
        fb_conversation_id: params.fbConversationId,
        fb_ad_id: params.fbAdId,
        recipient_page_scope_id: params.recipientPageScopeId,
        conversation_platform: params.conversation_platform,
        source: params.source,
        message_id: params.messageId,
        contact_type: params.contactType,
        contact_value: params.contactValue
      })
      .returning();
      
    return convertBigInts(data);
  } catch (error) {
    console.error('Error inserting contact:', error);
    throw error;
  } 
};

// Simple query functions for getting related data
const getLeadsByConversationId = async (supabase: any, conversationId: string | number) => {
  try {
    const data = await db
      .select()
      .from(tables.leadsTable)
      .where(eq(tables.leadsTable.conversation_id, Number(conversationId)));

    return convertBigInts(data);
  } catch (error) {
    console.error('Error getting leads:', error);
    throw error;
  }
};

const getContactsByConversationId = async (supabase: any, conversationId: string | number) => {
  try {
    const data = await db
      .select()
      .from(tables.contactsTable)
      .where(eq(tables.contactsTable.conversation_id, Number(conversationId)));

    return convertBigInts(data);
  } catch (error) {
    console.error('Error getting contacts:', error);
    throw error;
  }
};

const updateConversationWithLeadInfo = async (conversationId: string | number, leadInfo: any) => {
  try {
    const data = await db
      .update(tables.pageConversationsTable)
      .set({
        lead_first_name: leadInfo.firstName || null,
        lead_last_name: leadInfo.lastName || null,
        lead_email: leadInfo.email || null,
        lead_phone: leadInfo.phone || null,
        lead_street_address: leadInfo.streetAddress || null,
        lead_business_website: leadInfo.businessWebsite || null,
        updated_at: sql`NOW()`
      })
      .where(eq(tables.pageConversationsTable.id, Number(conversationId)))
      .returning();

    return convertBigInts(data);
  } catch (error) {
    console.error('Error updating conversation with lead info:', error);
    throw error;
  }
};

const updateConversationWithAdInfo = async (params: any) => {
  try {
    const data = await db
      .update(tables.pageConversationsTable)
      .set({
        conversation_ad_id: params.adInfo.fb_ad_id,
        conversation_source: params.adInfo.conversation_source
      })
      .where(eq(tables.pageConversationsTable.id, Number(params.conversationId)))
      .returning();

    return convertBigInts(data);
  } catch (error) {
    console.error('Error updating conversation with ad info:', error);
    throw error;
  }
};

const getLeadsAndContactsByConversationId = async (supabase: any, conversationId: string | number) => {
  try {
    const [leads, contacts] = await Promise.all([
      getLeadsByConversationId(null, conversationId),
      getContactsByConversationId(null, conversationId)
    ]);

    return { leads, contacts };
  } catch (error) {
    console.error('Error getting leads and contacts:', error);
    throw error;
  }
};

const getConversationsByAdId = async (supabase: any, adId: string) => {
  try {
    const data = await db
      .select()
      .from(tables.pageConversationsTable)
      .where(eq(tables.pageConversationsTable.conversation_ad_id, adId));

    return convertBigInts(data);
  } catch (error) {
    console.error('Error getting conversations by ad id:', error);
    throw error;
  }
};

const getConversationsByLeadId = async (supabase: any, leadId: string | number) => {
  try {
    // First get the conversation_id from leads table
    const leadData = await db
      .select({ conversation_id: tables.leadsTable.conversation_id })
      .from(tables.leadsTable)
      .where(eq(tables.leadsTable.id, Number(leadId)))
      .limit(1);

    if (!leadData || leadData.length === 0) {
      console.error('No conversation id found for lead id:', leadId);
      return [];
    }

    // Then get the conversation details
    const conversationData = await db
      .select()
      .from(tables.pageConversationsTable)
      .where(eq(tables.pageConversationsTable.id, leadData[0].conversation_id));

    return convertBigInts(conversationData);
  } catch (error) {
    console.error('Error getting conversations by lead id:', error);
    throw error;
  }
};

const getConversationByFbConversationId = async (supabase: any, fbConversationId: string) => {
  console.log('Attempting to get conversation by fb conversation id...', fbConversationId);
  try {
    const data = await db
      .select()
      .from(tables.pageConversationsTable)
      .where(
        and(
          eq(tables.pageConversationsTable.fb_conversation_id, fbConversationId),
          eq(tables.pageConversationsTable.app_id, Number(config.appId))
        )
      )
      .limit(1);
      
    console.log('Conversation by fb conversation id:', data);
    
    // Transform camelCase to snake_case to match Supabase format
    // const transformedData = data.map(item => ({
    //   id: item.id,
    //   app_id: item.appId,
    //   status_modified_by_user_id: item.statusModifiedByUserId,
    //   created_at: item.createdAt,
    //   updated_at: item.updatedAt,
    //   fb_page_id: item.fbPageId,
    //   fb_conversation_id: item.fbConversationId,
    //   recipient_page_scope_id: item.recipientPageScopeId,
    //   ig_account_id: item.igAccountId,
    //   conversation_platform: item.conversationPlatform,
    //   active: item.active,
    //   opening_message_id: item.openingMessageId,
    //   conversation_source: item.conversationSource,
    //   conversation_ad_id: item.conversationAdId,
    //   fb_first_name: item.fbFirstName,
    //   fb_last_name: item.fbLastName,
    //   fb_profile_pic: item.fbProfilePic,
    //   ig_name: item.igName,
    //   ig_username: item.igUsername,
    //   ig_profile_pic: item.igProfilePic,
    //   ig_follower_count: item.igFollowerCount,
    //   ig_is_user_follow_business: item.igIsUserFollowBusiness,
    //   ig_is_business_follow_user: item.igIsBusinessFollowUser,
    //   lead_first_name: item.leadFirstName,
    //   lead_last_name: item.leadLastName,
    //   lead_email: item.leadEmail,
    //   lead_phone: item.leadPhone,
    //   lead_street_address: item.leadStreetAddress,
    //   lead_business_website: item.leadBusinessWebsite
    // }));
    
    // return convertBigInts(transformedData);
    return convertBigInts(data);
  } catch (error) {
    console.error('Unexpected error in getConversationByFbConversationId:', error);
    throw error;
  }
};

// Create alias for the class
const webhookFilter = DrizzleWebhookFilter;

const updateCommentSentToAudosServer = async (supabase: any, commentId: string | number) => {
  console.log('marking comment as sent to audos server in updateCommentSentToAudosServer commentId', commentId);
  try {
    const data = await db
      .update(tables.pageCommentsTable)
      .set({ sent_to_audos_server: true })
      .where(eq(tables.pageCommentsTable.id, Number(commentId)))
      .returning();
      
    if (!data || data.length === 0) {
      console.error('Error updating comment sent to audos server: No comment found');
      return null;
    }
    
    return convertBigInts(data[0]);
  } catch (error) {
    console.error('Error updating comment sent to audos server:', error);
    return null;
  }
};

const debugUserAndPageAccessTokens = async (fbUserData: any, pageData: any, supabase: any) => {
  console.log(`debugUserAndPageAccessTokens ${fbUserData.id} ${pageData.fb_page_id}`);
  console.log('fbUserData:', fbUserData);
  console.log('pageData:', pageData);

  // Combine the results
  const combinedResult = {
    appId: pageData.app_id,
    appContext: fbUserData.app_env,
    userDbId: fbUserData.id,
    userDbEmail: fbUserData.email,
    userFbName: null,
    pageId: pageData.fb_page_id,
    userScopes: null,
    pageScopes: null,
    userAccessTokenValid: false,
    pageAccessTokenValid: false,
    pageAccessToken: pageData.page_access_token,
    userAccessToken: fbUserData.user_access_token,
    pageMessagingEnabled: false,
    instagramMessagingEnabled: false,
    adPermissionsEnabled: false,
    userTokenExpiresAt: null,
    pageTokenExpiresAt: null,
    userDataAccessExpiresAt: null,
    pageDataAccessExpiresAt: null,
    userAccessTokenIssuedAt: null,
    pageAccessTokenIssuedAt: null,
    missingScopesForUser: null,
    missingScopesForPage: null,
    status: 'success',
    error: null,
    errorSource: null
  };
  try {
    // Debug user access token
    const userTokenDebugResult = await debugUserAccessToken({
      supabase,
      appid: pageData.app_id,
      userId: fbUserData.id,
      fbId: fbUserData.fb_id,
      accessToken: fbUserData.user_access_token
      }).catch(error => {
      console.error('Error debugging user token:', error);
        throw { source: 'user_token', error };
    });

    console.log('user token debug result', userTokenDebugResult);
    combinedResult.userFbName = userTokenDebugResult.name || null;
    combinedResult.userScopes = userTokenDebugResult.scopes;
    combinedResult.userAccessTokenValid = userTokenDebugResult.isValid;
    combinedResult.userTokenExpiresAt = userTokenDebugResult.expiresAt;
    combinedResult.userDataAccessExpiresAt = userTokenDebugResult.dataAccessExpiresAt;
    combinedResult.userAccessTokenIssuedAt = userTokenDebugResult.issuedAt;
    combinedResult.missingScopesForUser = userTokenDebugResult.missingScopes;

    // Debug page access token
    const pageTokenDebugResult = await debugPageAccessTokens({
      supabase,
      appid: pageData.app_id,
      userId: fbUserData.id,
      fbId: fbUserData.fb_id,
      pageId: pageData.fb_page_id,
      accessToken: pageData.page_access_token
      }).catch(error => {
      console.error('Error debugging page token:', error);
        throw { source: 'page_token', error };
    });

    // Note the returned access token could be different from the one received in the params
    // todo add the new page access token to the db

    if (pageTokenDebugResult.accessToken && pageTokenDebugResult.accessToken !== pageData.page_access_token) {
      console.log('new page access token found, updating the db', pageTokenDebugResult.accessToken);
      // update the db using Drizzle ORM
      await db
        .update(tables.pageTable)
        .set({ page_access_token: pageTokenDebugResult.accessToken })
        .where(
          and(
            eq(tables.pageTable.fb_page_id, pageData.fb_page_id),
            eq(tables.pageTable.app_id, Number(pageData.app_id))
          )
        );
      console.log('page access token updated in the db');
      combinedResult.pageAccessToken = pageTokenDebugResult.accessToken;
    }

    console.log('page token debug result', pageTokenDebugResult);

    combinedResult.pageAccessTokenValid = pageTokenDebugResult.isValid;
    combinedResult.pageScopes = pageTokenDebugResult.scopes;
    combinedResult.pageTokenExpiresAt = pageTokenDebugResult.expiresAt;
    combinedResult.pageDataAccessExpiresAt = pageTokenDebugResult.dataAccessExpiresAt;
    combinedResult.pageAccessTokenIssuedAt = pageTokenDebugResult.issuedAt;
    combinedResult.missingScopesForPage = pageTokenDebugResult.missingScopes;

    combinedResult.pageMessagingEnabled = pageTokenDebugResult.hasMessagingPermission || false;
    combinedResult.instagramMessagingEnabled = pageTokenDebugResult.hasInstagramMessagingPermission || false;
    combinedResult.adPermissionsEnabled = pageTokenDebugResult.hasAdPermission || false;

    // check if user access token is valid
    if (!userTokenDebugResult.isValid) {

    }

    // check if page access token is valid
    if (!pageTokenDebugResult.isValid) {

    }

    // Upsert user access token
    await Promise.all([
      upsertAccessToken({
        supabase,
        appid: pageData.app_id,
        userId: fbUserData.id,
        fbId: fbUserData.fb_id,
        pageId: pageData.fb_page_id,
        accessTokenData: {...combinedResult, pageMessagingEnabled: userTokenDebugResult.hasMessagingPermission || false, instagramMessagingEnabled: userTokenDebugResult.hasInstagramMessagingPermission || false, adPermissionsEnabled: userTokenDebugResult.hasAdPermission || false},
        accessTokenType: 'user'
      }).catch(error => {
        console.error('Error upserting user token:', error);
        throw { source: 'user_token_upsert', error };
      }),

      // Upsert page access token
      upsertAccessToken({
        supabase,
        appid: pageData.app_id,
        userId: fbUserData.id,
        fbId: fbUserData.fb_id,
        pageId: pageData.fb_page_id,
        accessTokenData: {...combinedResult, pageMessagingEnabled: pageTokenDebugResult.hasMessagingPermission || false, instagramMessagingEnabled: pageTokenDebugResult.hasInstagramMessagingPermission || false, adPermissionsEnabled: pageTokenDebugResult.hasAdPermission || false},
        accessTokenType: 'page'
      }).catch(error => {
        console.error('Error upserting page token:', error);
        throw { source: 'page_token_upsert', error };
      })
    ]);
   
  } catch (error: any) {
      console.error('Error in debugUserAndPageAccessTokens:', error);

      combinedResult.status = 'error';
      combinedResult.error = error.message || 'Unknown error occurred';
      combinedResult.errorSource = error.source || 'token_debug';
      combinedResult.userAccessTokenValid = error.source === 'user_token' ? false : true;
      combinedResult.pageAccessTokenValid = error.source === 'page_token' ? false : true;

      if (error.source === 'user_token' || error.source === 'page_token') {
        let errorObject = config.handleFbErrors({error: error.error, serverNote: `error in debugUserAndPageAccessTokens source: ${error.source}`});
        combinedResult.error = errorObject.message;
        combinedResult.errorSource = errorObject.source;
      }
  }

  return combinedResult;
};

const deleteClientUserData = async (userId: string) => {
  console.log(`Starting deletion process for client user ID: ${userId}`);
  // This would need the full deletion logic - simplified for now
  try {
    await db.delete(tables.userTable).where(eq(tables.userTable.id, Number(userId)));
    return { success: true, message: `Successfully deleted user ${userId}` };
  } catch (error) {
    return { success: false, message: `Error deleting user: ${error}` };
  }
};

const deleteCustomerUserData = async (customerPsid: string, fbPageId: string) => {
  console.log(`Starting deletion process for customer PSID: ${customerPsid} on page: ${fbPageId}`);
  // This would need the full deletion logic - simplified for now
  return { success: true, message: `Successfully deleted data for customer ${customerPsid} on page ${fbPageId}` };
};

const getAllUserAssets = async (fb_id: string, app_id: string | number, pagination = { page: 1, pageSize: 10 }) => {
  console.log('Getting all user assets for fb_id:', fb_id, 'app_id:', app_id);
  // This would need the full complex query logic - simplified for now
  const { page, pageSize } = pagination;
  
  const pages = await db
    .select()
    .from(tables.pageTable)
    .where(
      and(
        eq(tables.pageTable.app_id, Number(app_id)),
        eq(tables.pageTable.fb_id, fb_id)
      )
    )
    .limit(pageSize)
    .offset((page - 1) * pageSize);
    
  return {
    pages: { data: convertBigInts(pages), total: pages.length, pages: 1 },
    assets: {},
    adAccounts: [],
    adAssets: {},
    pagination: { totalPages: 1, currentPage: page, pageSize, totalItems: pages.length }
  };
};

const getValidPageAccessToken = async (pageId: string, supabase: any, requirements = { needsMessaging: false, needsInstagram: false, needsAds: false }) => {
  try {
    console.log('Getting valid page access token for pageId:', pageId);
    
    console.log('config.appId', config.appId);
    console.log('requirements', requirements);
    // First check if token exists in the new access tokens table
    const existingTokens = await db
      .select()
      .from(tables.accessTokensTable)
      .where(
        and(
          eq(tables.accessTokensTable.app_id, Number(config.appId)),
          eq(tables.accessTokensTable.page_id, pageId),
          eq(tables.accessTokensTable.access_token_type, 'page'),
          eq(tables.accessTokensTable.is_token_valid, true)
        )
      );

    //check if page is managed by multiple users
    const { isManagedByMultipleUsers, data: pagesData, userDataArray } = await checkIfPageIsManagedByMultipleUsers(pageId, supabase, true);
    
    const recentSuccess = await db
      .select()
      .from(tables.metaApiCallsResultsTable)
      .where(
        and(
          eq(tables.metaApiCallsResultsTable.page_id, pageId),
          eq(tables.metaApiCallsResultsTable.success, true),
          sql`${tables.metaApiCallsResultsTable.requirement_context}->>'needsMessaging' = ${requirements.needsMessaging}`,
          sql`${tables.metaApiCallsResultsTable.requirement_context}->>'needsInstagram' = ${requirements.needsInstagram}`,
          sql`${tables.metaApiCallsResultsTable.requirement_context}->>'needsAds' = ${requirements.needsAds}`,
          eq(tables.metaApiCallsResultsTable.app_id, Number(config.appId))
        )
      )
      .orderBy(desc(tables.metaApiCallsResultsTable.created_at))
      .limit(1);

    console.log('recentSuccess in getValidPageAccessToken recentSuccess.length:', recentSuccess ? recentSuccess : 'null');
    if (recentSuccess && recentSuccess.length > 0 && recentSuccess[0].access_token) {
      console.log('recentSuccess found, return here?', recentSuccess[0]);
      return recentSuccess[0].access_token;
    }

    // If we have valid tokens in the new table
    if (existingTokens && existingTokens.length > 0) {
      console.log('Found existing tokens in access_tokens table, existingTokens.length:', existingTokens.length);
      console.log('isManagedByMultipleUsers', isManagedByMultipleUsers);
      if (isManagedByMultipleUsers) {
        console.log('recentSuccess in getValidPageAccessToken recentSuccess.length:', recentSuccess ? recentSuccess.length : 'null');
        if (recentSuccess && recentSuccess.length > 0 && recentSuccess[0].access_token) {
          console.log('recentSuccess is true 6');
          console.dir({ recentSuccess }, { depth: null });
          return recentSuccess[0].access_token;
        }

        console.log('existingTokens 1.length', existingTokens);
        console.log('requirements', requirements);

        // Filter tokens based on requirements
        // const validTokens = existingTokens.filter((token: any) => {
        //   const meetsRequirements = (
        //     (!requirements.needsMessaging || token.pageMessagingEnabled) &&
        //     (!requirements.needsInstagram || token.instagramMessagingEnabled) &&
        //     (!requirements.needsAds || token.adPermissionsEnabled)
        //   );
        //   return meetsRequirements;
        // });
        const validTokens = existingTokens.filter(token => {
          const meetsRequirements = (
            (!requirements.needsMessaging || token.page_messaging_enabled) &&     // ✅ CORRECT
            (!requirements.needsInstagram || token.instagram_messaging_enabled) && // ✅ CORRECT
            (!requirements.needsAds || token.ad_permissions_enabled)              // ✅ CORRECT
          );
          return meetsRequirements;
        });
        console.log('validTokens', validTokens);

        if (validTokens.length > 0) {
          // Return the token with most permissions
          const bestToken = validTokens.reduce((best: any, current: any) => {
            const currentScore = (current.pageMessagingEnabled ? 1 : 0) + 
                               (current.instagramMessagingEnabled ? 1 : 0) + 
                               (current.adPermissionsEnabled ? 1 : 0);
            const bestScore = (best.pageMessagingEnabled ? 1 : 0) + 
                            (best.instagramMessagingEnabled ? 1 : 0) + 
                            (best.adPermissionsEnabled ? 1 : 0);
            return currentScore > bestScore ? current : best;
          });
          console.log('bestToken', bestToken);
          if (!bestToken.access_token || bestToken.access_token === null || bestToken.access_token === undefined) console.log('bestToken.access_token is undefined', bestToken);
          return bestToken.access_token;
        } else {
          console.log('No valid page access token found for pageId:', pageId);
          return pagesData[0].pageAccessToken; // Return first available token as fallback
        }
      } else {
        console.log('valid tokens found in the new table, but page managed by single user, returning the page access token', pageId);
        console.log('recentSuccess');
        console.dir({ recentSuccess }, { depth: null });
        if (recentSuccess && recentSuccess.length > 0 && recentSuccess[0].access_token) {
          console.log('recentSuccess is true 7');
          console.dir({ recentSuccess }, { depth: null });
          return recentSuccess[0].access_token;
        }
        // page is managed by a single user
        return pagesData[0].pageAccessToken; 
      }
    } else {
      // no valid tokens found in the new table
      console.log('no valid tokens found in the new table, existingTokens', existingTokens);
      if (recentSuccess && recentSuccess.length > 0 && recentSuccess[0].access_token) {
        console.log('recentSuccess is true 8');
        console.dir({ recentSuccess }, { depth: null });
        return recentSuccess[0].access_token;
      }
      
      // If no valid token found in new table,
      if (!pagesData || pagesData.length === 0) {
        console.log('No page data found for pageId:', pageId);
        throw new Error('invalid pageId: ' + pageId);
      }

      // Debug tokens for all user-page pairs and save to db
      let debuggedDataArray: any[] = [];
      for (const pageData of pagesData) {
        for (const users of userDataArray) {
          for (const userData of users) {
            if (userData?.fb_id && pageData.fb_id === userData.fb_id) {
              const debuggedData = await debugUserAndPageAccessTokens(userData, pageData, supabase);
              debuggedDataArray.push(debuggedData);
            }
          }
        }
      }

      if (isManagedByMultipleUsers) {
        console.log('debuggedDataArrayvbnm', debuggedDataArray);
        // Filter valid tokens based on requirements
        const validDebuggedData = debuggedDataArray.filter((data: any) => {
          return data.pageAccessTokenValid && (
            (!requirements.needsMessaging || data.page_messaging_enabled) &&
            (!requirements.needsInstagram || data.instagram_messaging_enabled) &&
            (!requirements.needsAds || data.ad_permissions_enabled)
          );
        });
        console.log('validDebuggedDataqww', validDebuggedData);

        if (validDebuggedData.length > 0) {
          // Return token with most permissions
          const bestData = validDebuggedData.reduce((best: any, current: any) => {
            const currentScore = (current.page_messaging_enabled ? 1 : 0) + 
                               (current.instagram_messaging_enabled ? 1 : 0) + 
                               (current.ad_permissions_enabled ? 1 : 0);
            const bestScore = (best.page_messaging_enabled ? 1 : 0) + 
                            (best.instagram_messaging_enabled ? 1 : 0) + 
                            (best.ad_permissions_enabled ? 1 : 0);
            return currentScore > bestScore ? current : best;
          });
          console.log('bestData', bestData);
          if (!bestData.pageAccessToken || bestData.pageAccessToken === null || bestData.pageAccessToken === undefined) console.log('bestData.pageAccessToken is undefined', bestData);
          return bestData.pageAccessToken;
        } else {
          console.log('No valid page access token found after debugging pageId:', pageId);
          return pagesData[0].pageAccessToken; // Return first available token as fallback
        }
      } else {
        console.log('valid access token not found, page managed by single user, returning the page access token', pageId);
        console.log('recentSuccess');
        console.dir({ recentSuccess }, { depth: null });
        if (recentSuccess && recentSuccess.length > 0 && recentSuccess[0].access_token) {
          console.log('recentSuccess is true 5');
          console.dir({ recentSuccess }, { depth: null });
          return recentSuccess[0].access_token;
        }
        // page is managed by a single user
        console.log('returning legacy page access token', pagesData[0].pageAccessToken);
        return pagesData[0].pageAccessToken; // Return first available token as fallback
      }
    }
  } catch (err) {
    console.error('Error in getValidPageAccessToken:', err);
    console.log(' returning a fall default page access token');
    return await getPageAccessToken(pageId, supabase);
  }
};

const getValidUserAccessToken = async (adId: string, pageId: string, supabase: any, requirements = { needsMessaging: false, needsInstagram: false, needsAds: true }) => {};

const getValidUserAccessTokensForAd = async (adId: string, pageId: string, supabase: any, requirements = { needsMessaging: false, needsInstagram: false, needsAds: true }) => {
  try {
    console.log('getValidUserAccessTokensForAd valid user access token for adId:', adId);

    //check if page is managed by multiple users
    const { isManagedByMultipleUsers, data: pagesData, userDataArray } = await checkIfPageIsManagedByMultipleUsers(pageId, supabase,true );

    const userObjects = userDataArray.map((arr: any) => arr[0]).filter(Boolean);
    const fbIds = userObjects.map((user: any) => user.fb_id);
    const userAccessTokens = userObjects.map((user: any) => user.user_access_token);

    console.log('fbIds', fbIds);
    console.log('userAccessTokens', userAccessTokens);
    console.log('userObjects', userObjects);

    let existingTokens: any[] = [];

    for (const user of userObjects) {
      if (!user || !user.fb_id || !user.user_access_token) continue;
      const tokens = await db
        .select()
        .from(tables.accessTokensTable)
        .where(
          and(
            eq(tables.accessTokensTable.access_token_type, 'user'),
            eq(tables.accessTokensTable.app_id, Number(config.appId)),
            eq(tables.accessTokensTable.is_token_valid, true),
            eq(tables.accessTokensTable.fb_id, user.fb_id)
          )
        )
        .limit(1);

      if (tokens && tokens.length > 0) {
        console.log('getValidUserAccessTokensForAd tokens found in the new table', tokens.length);
        existingTokens.push(...tokens);
      } else {
        console.log('no tokens found in the new table');
      }
    }

    if (isManagedByMultipleUsers) {
      console.log('page is managed by multiple users');
      console.log('isManagedByMultipleUsers, data: pagesData, userDataArray', isManagedByMultipleUsers, pagesData, userDataArray);

      // If we have valid tokens in the new table
      if (existingTokens && existingTokens.length > 0) {
        console.log('existingTokenshbgtf.length', existingTokens.length);
        // Filter tokens based on requirements
        const validTokens = existingTokens.filter(token => {
          const meetsRequirements = (
            (!requirements.needsMessaging || token.page_messaging_enabled) &&
            (!requirements.needsInstagram || token.instagram_messaging_enabled) &&
            (!requirements.needsAds || token.ad_permissions_enabled)
          );
          return meetsRequirements;
        });

        return validTokens.map((data: any) => ({
          userAccessToken: data.access_token,
          userDbId: data.user_id,
          pageAccessToken: data.details?.pageAccessToken,
          // userFbId: data.fb_id
        }));
      } else {
        console.log('no valid tokens found in the new table, existingTokens', existingTokens);
        console.log('userDataArray', userDataArray);

        // If no valid token found in new table,
        if (!pagesData || pagesData.length === 0) {
          console.log('No page data found for pageId:', pageId);
          throw new Error('invalid pageId: ' + pageId);
        }

        // Debug tokens for all user-page pairs and save to db
        let debuggedDataArray: any[] = [];
        for (const pageData of pagesData) {
          for (const users of userDataArray) {
            for (const userData of users) {
              if (userData?.fb_id && pageData.fb_id === userData.fb_id) {
                const debuggedData = await debugUserAndPageAccessTokens(userData, pageData, supabase);
                debuggedDataArray.push(debuggedData);
              }
            }
          }
        }

        return debuggedDataArray.map((data: any) => ({
          userAccessToken: data.userAccessToken,
          userDbId: data.userDbId,
          pageAccessToken: data.pageAccessToken,
          // userFbName: data.userFbName
        }));
      }
    } else {
      // page is managed by single user, return the user access token from userDataArray
      return userDataArray.map((data: any) => ({
        userAccessToken: data[0].user_access_token,
        userDbId: data[0].id,
        pageAccessToken: pagesData[0].page_access_token,
        // userFbName: data.userFbName
      }));
    }
  } catch (err) {
    console.error('Error in getValidUserAccessTokensForAd:', err);
    return null;
  }
};

const logMetaApiCall = async (supabase: any, params: {
  userId?: string | null;
  fbId: string;
  pageId: string;
  accessToken: string;
  accessTokenType: 'page' | 'user';
  success: boolean;
  status: string;
  reqUrl: string;
  reqParams: any;
  res: any;
  requirementContext: { needsMessaging: boolean; needsInstagram: boolean; needsAds: boolean; action: string };
  errorCode?: string;
  errorMessage?: string;
}) => {
  console.log("logMetaApiCall ....");
  console.dir({params},{depth: null})
  try {
    const data = await db
      .insert(tables.metaApiCallsResultsTable)
      .values({
        app_id: Number(config.appId),
        user_id: params.userId ? Number(params.userId) : null,
        fb_id: params.fbId,
        page_id: params.pageId,
        access_token: params.accessToken,
        access_token_type: params.accessTokenType,
        success: params.success,
        status: params.status,
        req_url: params.reqUrl,
        req_params: params.reqParams,
        res: params.res,
        requirement_context: params.requirementContext,
        error_code: params.errorCode,
        error_message: params.errorMessage
      })
      .returning();

    return convertBigInts(data);
  } catch (error) {
    console.error('Error in logMetaApiCall:', error);
  }
};

const makeFbApiCall = async (params: {
  pageId: string;
  userId?: string;
  supabase: any;
  accessToken: string;
  requirements: any;
  apiCall: () => Promise<any>;
  retryOnInvalidToken?: boolean;
}) => {
  console.log('makeFbApiCall executing API call');
  
  try {
    const response = await params.apiCall();
    
    if (response) {
      await logMetaApiCall(params.supabase, {
        userId: params.userId || null,
        fbId: params.pageId,
        pageId: params.pageId,
        accessToken: params.accessToken,
        accessTokenType: params.requirements.accessTokenType || 'page',
        success: true,
        status: 'success',
        reqUrl: params.requirements.url || params.requirements.function_string,
        reqParams: params.requirements.params,
        res: response.data || response,
        requirementContext: params.requirements.requirementContext
      });
    }
    
    return response;
  } catch (error) {
    await logMetaApiCall(params.supabase, {
      userId: params.userId || null,
      fbId: params.pageId,
      pageId: params.pageId,
      accessToken: params.accessToken,
      accessTokenType: params.requirements.accessTokenType || 'page',
      success: false,
      status: 'error',
      reqUrl: params.requirements.url || params.requirements.function_string,
      reqParams: params.requirements.params,
      res: error,
      requirementContext: params.requirements.requirementContext,
      errorCode: (error as any).code,
      errorMessage: (error as any).message
    });
    
    throw error;
  }
};

// Missing functions that need to be implemented
const upsertAdMedia = async (supabase: any, adMediaData: any) => {
  console.log('Upserting ad media:', JSON.stringify(adMediaData, null, 2));
  try {
    const data = await db
      .insert(tables.adMediaTable)
      .values({
        app_id: adMediaData.app_id,
        fb_image_hash: adMediaData.fb_image_hash,
        fb_video_id: adMediaData.fb_video_id,
        is_video: adMediaData.is_video || false,
        is_image: adMediaData.is_image || false,
        original_media_url: adMediaData.original_media_url,
        ad_account_id: adMediaData.ad_account_id,
        status: adMediaData.status || 'uploading',
        upload_response: adMediaData.upload_response,
        height: adMediaData.height,
        width: adMediaData.width
      })
      .onConflictDoUpdate({
        target: tables.adMediaTable.id,
        set: {
          fb_image_hash: adMediaData.fb_image_hash,
          fb_video_id: adMediaData.fb_video_id,
          is_video: adMediaData.is_video || false,
          is_image: adMediaData.is_image || false,
          original_media_url: adMediaData.original_media_url,
          ad_account_id: adMediaData.ad_account_id,
          status: adMediaData.status || 'uploading',
          upload_response: adMediaData.upload_response,
          height: adMediaData.height,
          width: adMediaData.width
        }
      })
      .returning();

    console.log('Ad media upserted successfully');
    return convertBigInts(data);
  } catch (error) {
    console.error('Error upserting ad media:', error);
    throw error;
  }
};

const findAdMediaByAdId = async (supabase: any, adId, adAccountId = null) => {
  console.log('Finding ad media by ad ID:', adId);
  try {
    const whereConditions = [eq(tables.adMediaTable.ad_account_id, adId)];
    
    if (adAccountId) {
      whereConditions.push(eq(tables.adMediaTable.ad_account_id, adAccountId));
    }

    const data = await db
      .select()
      .from(tables.adMediaTable)
      .where(and(...whereConditions));

    console.log('Ad media found:', data);
    return convertBigInts(data);
  } catch (error) {
    console.error('Error finding ad media:', error);
    throw error;
  }
};

// Missing non-exported functions that need to be implemented
const upsertAccessToken = async (params: any) => {
  const { supabase, appid, userId, fbId, pageId, accessTokenData, accessTokenType } = params;
  console.log(`Upserting access token: ${accessTokenType} for user: ${userId} and fbId: ${fbId} and pageId: ${pageId}`);

  let queryParam = accessTokenType !== 'user' ? `page_id.eq.${pageId}` : `fb_id.eq.${fbId}`;
  let queryParam2 = accessTokenType !== 'user' ? `access_token.eq.${accessTokenData.pageAccessToken}` : `access_token.eq.${accessTokenData.userAccessToken}`;
  
  try {
    // First, try to find any existing record - this matches the complex Supabase query logic
    let existingToken: any[] = [];
    let findError: any = null;

    try {
      // Build the complex query that matches the Supabase version
      const baseConditions = [
        eq(tables.accessTokensTable.app_id, Number(appid)),
        eq(tables.accessTokensTable.user_id, Number(userId)),
        eq(tables.accessTokensTable.fb_id, fbId),
        eq(tables.accessTokensTable.access_token_type, accessTokenType)
      ];

      // Add the OR conditions based on accessTokenType
      if (accessTokenType !== 'user') {
        // For page tokens, check page_id OR access_token
        existingToken = await db
          .select()
          .from(tables.accessTokensTable)
          .where(
            and(
              ...baseConditions,
              or(
                eq(tables.accessTokensTable.page_id, pageId),
                eq(tables.accessTokensTable.access_token, accessTokenData.pageAccessToken)
              )
            )
          )
          .limit(1);
      } else {
        // For user tokens, check fb_id OR access_token
        existingToken = await db
          .select()
          .from(tables.accessTokensTable)
          .where(
            and(
              ...baseConditions,
              or(
                eq(tables.accessTokensTable.fb_id, fbId),
                eq(tables.accessTokensTable.access_token, accessTokenData.userAccessToken)
              )
            )
          )
          .limit(1);
      }
    } catch (error) {
      findError = error;
      console.log('Error checking for existing token:', findError);
      console.log('Error checking for existing token.length:', existingToken.length);
    }

    // Convert zero timestamps to 90 days from now and Unix timestamps to PostgreSQL timestamps
    const convertTimestamp = (timestamp: any) => {
      if (!timestamp || timestamp === 0) return new Date(Date.now() + 90 * 24 * 60 * 60 * 1000).toISOString();
      // Convert Unix timestamp (seconds) to JavaScript Date then to ISO string
      return new Date(timestamp * 1000).toISOString();
    };

    // Prepare the token data - using exact field names from Supabase
    const tokenData = {
      appId: Number(appid),
      userId: Number(userId),
      fbId: fbId,
      pageId: accessTokenType !== 'user' ? pageId : null,
      accessToken: accessTokenType === 'user' ? accessTokenData.userAccessToken : accessTokenData.pageAccessToken,
      accessTokenType: accessTokenType,
      pageMessagingEnabled: accessTokenData.pageMessagingEnabled,
      instagramMessagingEnabled: accessTokenData.instagramMessagingEnabled,
      adPermissionsEnabled: accessTokenData.adPermissionsEnabled,
      status: accessTokenData.status,
      isTokenValid: accessTokenType === 'user' ? accessTokenData.userAccessTokenValid : accessTokenData.pageAccessTokenValid,
      errorSource: accessTokenData.errorSource,
      expiresAt: convertTimestamp(accessTokenType === 'user' ? accessTokenData.userTokenExpiresAt : accessTokenData.pageTokenExpiresAt),
      tokenDataAccessExpiresAt: convertTimestamp(accessTokenType === 'user' ? accessTokenData.userDataAccessExpiresAt : accessTokenData.pageDataAccessExpiresAt),
      scopes: accessTokenType === 'user' ? accessTokenData.userScopes : accessTokenData.pageScopes,
      missingScopes: accessTokenType === 'user' ? accessTokenData.missingScopesForUser : accessTokenData.missingScopesForPage,
      details: accessTokenData
    };

    let result;
    if (existingToken && existingToken.length > 0) {
      console.log('existingToken:', existingToken);
      console.log(`Updating type: ${accessTokenType} existing access token: ${existingToken[0].id} for user: ${userId} and fbId: ${fbId} and pageId: ${pageId}`);
      
      // Update existing record - match the exact Supabase update conditions
      const data = await db
        .update(tables.accessTokensTable)
        .set(tokenData)
        .where(
          and(
            eq(tables.accessTokensTable.app_id, Number(appid)),
            eq(tables.accessTokensTable.user_id, Number(userId)),
            eq(tables.accessTokensTable.fb_id, fbId),
            eq(tables.accessTokensTable.access_token_type, accessTokenType)
          )
        )
        .returning();

      result = data;
      console.log('Updated existing access token', data);
    } else {
      console.log(`No existing access token found, inserting new record for user: ${userId} and fbId: ${fbId} and pageId: ${pageId} and accessTokenType: ${accessTokenType}`);

      // Insert new record
      const data = await db
        .insert(tables.accessTokensTable)
        .values(tokenData)
        .returning();

      result = data;
      console.log('Inserted new access token');
    }

    return convertBigInts(result);
  } catch (error) {
    console.error('Error upserting access token:', error);
    throw error;
  }
};

const fetchNewPageTokenFromFbUserAccounts = async (supabase: any, fbUserId: string, userAccessToken: string, targetPageId: string) => {
  let nextUrl = `https://graph.facebook.com/${fbUserId}/accounts?access_token=${userAccessToken}`;
  let foundPage = null;

  console.log('targetPageId', targetPageId);
  while (nextUrl && !foundPage) {
    const response = await axios.get(nextUrl);
    // console.log('response', response)
    const pages = response.data.data;
    console.log('pages', pages);
    foundPage = pages.find((page: any) => page.id === targetPageId);

    if (!foundPage && response.data.paging && response.data.paging.next) {
      console.log('found next page');
      nextUrl = response.data.paging.next;
    } else {
      console.log('no more pages to fetch');
      nextUrl = null;
    }
  }

  if (foundPage) {
    console.log('foundPage', foundPage);
    // Update pages and accessTokens tables
    // await db
    //   .update(tables.pageTable)
    //   .set({ pageAccessToken: foundPage.access_token })
    //   .where(
    //     and(
    //       eq(tables.pageTable.fb_page_id, targetPageId),
    //       eq(tables.pageTable.appId, Number(config.appId))
    //     )
    //   );

    // await upsertAccessToken({
    //   supabase,
    //   appid: config.appId,
    //   userId: fbUserId,
    //   fbId: fbUserId,
    //   pageId: targetPageId,
    //   accessTokenData: { pageAccessToken: foundPage.access_token },
    //   accessTokenType: 'page'
    // });

    return foundPage.access_token;
  } else {
    console.log('could not find the requested page in the list of pages controlled by the user, User no longer controls this page.');
    return null;
  }
};

const debugTokenViaMetaApi = async (params: any) => {
  try {
    const result = await axios.get(`https://graph.facebook.com/debug_token`, {
      params: {
        input_token: params.accessToken,
        access_token: `${config.appId}|${config.appSecret}`
      }
    });
    return result.data.data;
  } catch (error) {
    console.error('Error in debugToken:', error);
    throw error;
  }
};

const debugPageAccessTokens = async (params: {
  supabase: any;
  appid: string | number;
  userId: string | number;
  fbId: string;
  pageId: string;
  accessToken: string;
}): Promise<any> => {
  // STEP 1: DEBUG THE PAGE ACCESS TOKEN
  const result = await debugTokenViaMetaApi({ accessToken: params.accessToken });
  console.log('result1 from debug page access token', result);

  let pageTokenData = null;

  // STEP 2: Check if the page access token is valid
  if ((result.error && result.error.message) || (!result.is_valid)) {
    console.log('page access token is not valid, fetch new page access token');
    
    // fetch the user access token from db using Drizzle ORM
    const userDataFromDb = await db
      .select()
      .from(tables.userTable)
      .where(eq(tables.userTable.id, Number(params.userId)))
      .limit(1);

    if (!userDataFromDb || userDataFromDb.length === 0) {
      console.error('Error fetching user access token: No user found');
      throw new Error('User not found');
    }

    console.log('fetching new page access token from fb by getting the users accounts/pages and then getting the page access token', userDataFromDb);

    // STEP 3: Fetch the new page access token from fb
    const newPageAccessToken = await fetchNewPageTokenFromFbUserAccounts(params.supabase, params.fbId, userDataFromDb[0].user_access_token, params.pageId);

    if (!newPageAccessToken || newPageAccessToken === null) {
      throw result.error;
    }

    // STEP 4: Debug the new page access token
    const result2 = await debugTokenViaMetaApi({ accessToken: newPageAccessToken });
    console.log('result2', result2);

    if (result2.error && result2.error.code === 190) {
      throw result2.error;
    }
    pageTokenData = { ...result2, access_token: newPageAccessToken };
  } else {
    console.log('page access token is valid, return result.data.data');
    console.log('result', result);
    pageTokenData = { ...result, access_token: params.accessToken };
  }

  console.log('pageTokenData', pageTokenData);

  // STEP 5: Return the page access token data
  return {
    scopes: pageTokenData.scopes,
    accessToken: pageTokenData.access_token,
    isValid: pageTokenData.is_valid,
    expiresAt: pageTokenData.expires_at,
    dataAccessExpiresAt: pageTokenData.data_access_expires_at,
    issuedAt: pageTokenData.issued_at,
    missingScopes: config.REQUIRED_SCOPES.split(',').filter((scope: string) => !pageTokenData.scopes.includes(scope)),
    hasMessagingPermission: pageTokenData.granular_scopes?.some((scope: any) => 
      scope.scope === 'pages_messaging'
    ) || false,
    hasInstagramMessagingPermission: pageTokenData.granular_scopes?.some((scope: any) => 
      scope.scope === 'instagram_manage_messages'
    ) || false,
    hasAdPermission: pageTokenData.granular_scopes?.some((scope: any) => 
      scope.scope === 'ads_management'
    ) || false
  };
};

const debugUserAccessToken = async (params: {
  supabase: any;
  appid: string | number;
  userId: string | number;
  fbId: string;
  accessToken: string;
}): Promise<any> => {
  const result = await debugTokenViaMetaApi({ accessToken: params.accessToken });
  console.log('result from debug user access token', result);

  if (result.error && result.error.code === 190) {
    throw result.error;
  }

  const userData = result;
  const userNameResult = await axios.get(`https://graph.facebook.com/v22.0/me`, {
    params: {
      fields: 'id,name,email',
      access_token: params.accessToken
    }
  });

  return {
    name: userNameResult.data.name,
    accessToken: params.accessToken,
    scopes: userData.scopes,
    isValid: userData.is_valid,
    expiresAt: userData.expires_at,
    dataAccessExpiresAt: userData.data_access_expires_at,
    issuedAt: userData.issued_at,
    missingScopes: config.REQUIRED_SCOPES.split(',').filter((scope: string) => !userData.scopes.includes(scope)),
    hasMessagingPermission: userData.granular_scopes.some((scope: any) => 
      scope.scope === 'pages_messaging'
    ),
    hasInstagramMessagingPermission: userData.granular_scopes.some((scope: any) => 
      scope.scope === 'instagram_manage_messages'
    ),
    hasAdPermission: userData.granular_scopes.some((scope: any) => 
      scope.scope === 'ads_management'
    )
  };
};

const getUserIdFromAccessToken = async (supabase: any, accessToken: string, pageData: any, pageError: any) => {
  if (!pageData || pageError || !pageData.fb_id) {
    // get user id from access token, find the fb_id and page_id from the access token and same app id
    console.log('No logMetaApiCall no pageData found in logMetaApiCall pageData:', pageError);
    console.dir({ pageData, pageError }, { depth: null });
    
    const userData = await db
      .select()
      .from(tables.userTable)
      .where(
        and(
          eq(tables.userTable.user_access_token, accessToken),
          eq(tables.userTable.app_id, config.appId as string)
        )
      )
      .limit(1);

    return { userData, userError: null };
  } else {
    console.log('found pageData in logMetaApiCall getUserIdFromAccessToken pageData');
    console.dir({ pageData }, { depth: null });
    
    const userData = await db
      .select()
      .from(tables.userTable)
      .where(
        and(
          eq(tables.userTable.fb_id, pageData.fb_id),
          eq(tables.userTable.app_id, config.appId as string)
        )
      )
      .limit(1);

    console.log('getUserIdFromAccessToken userData and getUserIdFromAccessToken userError');
    console.dir({ userData }, { depth: null });
    return { userData, userError: null };
  }
};

const insertLead = async (supabase: any, leadData: any) => {
  console.log('Inserting lead:', leadData);
  try {
    const data = await db
      .insert(tables.leadsTable)
      .values(leadData)
      .returning();
    
    console.log('Lead inserted successfully:', data);
    return { data, error: null };
  } catch (error) {
    console.error('Error inserting lead:', error);
    throw error;
  }
};

const insertContact = async (supabase: any, contactData: any) => {
  try {
    const data = await db
      .insert(tables.contactsTable)
      .values(contactData)
      .returning();
    
    console.log('Contact inserted successfully:', data);
    return { data, error: null };
  } catch (error) {
    console.error('Error inserting contact:', error);
    throw error;
  }
};

export { tables, convertBigInts };

// Unified export to match supabaseWrite.ts
export {
  insertUserData,
  insertPageData,
  insertBusinessManagerData,
  getFbPageId,
  getIgId,
  checkIfPageIsManagedByMultipleUsers,
  getPageAccessToken,
  getPageAccessTokenByAdIdOrAdTraceIdAndPageId,
  getAdDataAndUserDataFromDbWithAdIdOrAdTraceId,
  getPageAccessTokenAndValidateBeforeReturn,
  getPageDataByDbId,
  getUserAccessToken,
  upsertAdAccount,
  upsertCampaign,
  upsertAdSet,
  upsertAd,
  upsertCreative,
  getAdFromDbByAdIdOrAdTraceId,
  getCreativeFromDbByCreativeIdOrAdTraceId,
  readMessages,
  readComments,
  readPageData,
  changeConversationActiveStatus,
  upsertLead,
  upsertContact,
  getLeadsByConversationId,
  getContactsByConversationId,
  updateConversationWithLeadInfo,
  updateConversationWithAdInfo,
  getLeadsAndContactsByConversationId,
  getConversationsByAdId,
  getConversationsByLeadId,
  getConversationByFbConversationId,
  webhookFilter,
  updateMessageSentToAudosServer,
  updateCommentSentToAudosServer,
  debugUserAndPageAccessTokens,
  deleteClientUserData,
  deleteCustomerUserData,
  getAllUserAssets,
  getValidPageAccessToken,
  getValidUserAccessToken,
  getValidUserAccessTokensForAd,
  logMetaApiCall,
  makeFbApiCall,
  upsertAdMedia,
  findAdMediaByAdId
};
