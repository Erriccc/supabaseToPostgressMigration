"use strict";

import dotenv from 'dotenv';
import crypto from 'crypto';

dotenv.config();

// App context and test mode settings
const APP_CONTEXT = process.env.APP_CONTEXT || '1'; // Default to App 1
const TEST_MODE = process.env.TEST_MODE === 'true'; // Convert string to boolean


// Required environment variables
const ENV_VARS = [
  "PAGE_ID",
  "APP_ID1",
  "APP_ID2",
  "PAGE_ACCESS_TOKEN",
  "APP_SECRET1",
  "APP_SECRET2",
  "VERIFY_TOKEN",
  "APP_URL",
  "FB_APP"
];


import * as schema from './db/migrations/schema';

const config = {
  SENTRY_ORG: process.env.SENTRY_ORG,
  SENTRY_PROJECT: process.env.SENTRY_PROJECT,
  SENTRY_AUTH_TOKEN: process.env.SENTRY_AUTH_TOKEN,
  SENTRY_DSN: process.env.SENTRY_DSN,
  appContext: APP_CONTEXT,
  testMode: TEST_MODE,
  uiModeSignal: process.env.UI_MODE_SIGNAL,
  SUPABASE_URL: process.env.SUPABASE_URL,
  SUPABASE_KEY: process.env.SUPABASE_KEY,
  GEMINI_API_KEY: process.env.GEMINI_API_KEY,
  OPENAI_API_KEY: process.env.OPENAI_API_KEY,
  AUTHORIZED_EMAILS: process.env.AUTHORIZED_EMAILS,
  REQUIRED_SCOPES: process.env.REQUIRED_SCOPES,
  // testMode:process.env.TEST_MODE,
  // Messenger Platform API
  apiDomain: "https://graph.facebook.com",
  apiVersion: "v23.0",
 
  // Page and Application information
  legacyAppId : process.env.APP_ID1,
  legacyAppEnv : 1,
  defaultAdDuration: 14,
  get appId() {
    return this.appContext === '1' ? process.env.APP_ID1 : this.appContext === '2' ? process.env.APP_ID2 : this.appContext === '3' ? process.env.APP_ID3 : process.env.APP_ID4;
  },
  get appSecret() {
    return this.appContext === '1' ? process.env.APP_SECRET1 : this.appContext === '2' ? process.env.APP_SECRET2 : this.appContext === '3' ? process.env.APP_SECRET3 : process.env.APP_SECRET4;
  },
  get configId() {
    return this.appContext === '1' ? process.env.FB_LOGIN_CONFIG_ID_APP1 : this.appContext === '2' ? process.env.FB_LOGIN_CONFIG_ID_APP2 : this.appContext === '3' ? process.env.FB_LOGIN_CONFIG_ID_APP3 : process.env.FB_LOGIN_CONFIG_ID_APP4;
  },
  get businessConfigId() {
    return this.appContext === '1' ? process.env.FB_LOGIN_CONFIG_ID_APP1 : this.appContext === '2' ? process.env.FB_LOGIN_CONFIG_ID_APP2 : this.appContext === '3' ? process.env.FB_LOGIN_CONFIG_ID_APP3 : process.env.FB_LOGIN_BUSINESS_CONFIG_ID_APP4;
  },
  generateUniqueAdTraceId: function(adset_id) {
     return 'audosTraceId'+'-'+Date.now()+'-'+adset_id;
  },
  checkIfDbMessageIsNotNull(dbMessage) {
    if (dbMessage !== null && dbMessage !== undefined) {
      return true;
    }
    return false;
  },
  flattenForLog: function(obj: Record<string, any>) {
    return JSON.stringify(obj).replace(/[{}"]/g, '').replace(/,/g, ' ');
  },

  encodeAdTraceId: function(adTraceId, iceBreakerTitle) {
    return adTraceId + '-' + iceBreakerTitle;
  },
  decodeAdTraceId: function(payload) {
    // audosTraceId-timestamp-adset_id-payload
      //audosTraceId-timestamp-adset_id-payload
  const adTraceIdParts = payload.split('-');
  if (!payload.includes('audosTraceId')) {
    console.log('payload does not include adTraceId');
    return { adTraceId: null, adset_id: null, quickReplyPayloadWithoutAdTraceId: null, timestamp: null, includesAdTraceId: false };
  } 
  console.log('payload includes adTraceId');
  const adTraceId = String(`${adTraceIdParts[0]}-${adTraceIdParts[1]}-${adTraceIdParts[2]}`);
  const timestamp = String(`${adTraceIdParts[1]}`);
  const adset_id = String(`${adTraceIdParts[2]}`);
  const quickReplyPayloadWithoutAdTraceId = String(`${adTraceIdParts[3]}`);
  return { adTraceId, adset_id, quickReplyPayloadWithoutAdTraceId, timestamp, includesAdTraceId: true };
}
,
  verifyToken: process.env.VERIFY_TOKEN,
  schema: 'audos',
  audosCommentWebhookUrl:process.env.AUDOS_COMMENT_WEBHOOK,
  audosMessageWebhookUrl:process.env.AUDOS_MESSAGE_WEBHOOK,
  audosRedictUrl:process.env.AUDOS_REDIRECT_URL,
  audosApiKey: process.env.AUDOS_API_KEY,

  // Business Partnership Configuration
  audosBusinessId: process.env.AUDOS_BUSINESS_ID, // Default from the API examples
  audosBusinessIds: process.env.AUDOS_BUSINESS_IDS , // Default from the API examples
  audosSystemUserId: process.env.AUDOS_SYSTEM_USER_ID, // Default from the API examples // fb_d of  nicholas, andreas and one more
  audosSystemUserAccessToken: process.env.AUDOS_SYSTEM_USER_ACCESS_TOKEN, // for now just use the user access token for the audosSystemUserId user


/** 
 * Supported ad preview formats for Facebook/Instagram (v23.0)
 * See: https://developers.facebook.com/docs/marketing-api/generatepreview/v23.0
 * *also see: https://developers.facebook.com/docs/marketing-api/reference/ad-account/generatepreviews/
 */
 allPermittedTasksForBusinessAgency : [
  'MANAGE',
  'CREATE_CONTENT',
  'MODERATE',
  'MESSAGING',
  'ADVERTISE',
  'ANALYZE',
  'MODERATE_COMMUNITY',
  'MANAGE_JOBS',
  'PAGES_MESSAGING',
  'PAGES_MESSAGING_SUBSCRIPTIONS',
  'READ_PAGE_MAILBOXES',
  'VIEW_MONETIZATION_INSIGHTS',
  'MANAGE_LEADS',
  'PROFILE_PLUS_FULL_CONTROL',
  'PROFILE_PLUS_MANAGE',
  'PROFILE_PLUS_FACEBOOK_ACCESS',
  'PROFILE_PLUS_CREATE_CONTENT',
  'PROFILE_PLUS_MODERATE',
  'PROFILE_PLUS_MODERATE_DELEGATE_COMMUNITY',
  'PROFILE_PLUS_MESSAGING',
  'PROFILE_PLUS_ADVERTISE',
  'PROFILE_PLUS_ANALYZE',
  'PROFILE_PLUS_REVENUE',
  'PROFILE_PLUS_MANAGE_LEADS',
  'CASHIER_ROLE',
  'GLOBAL_STRUCTURE_MANAGEMENT',
  'PROFILE_PLUS_GLOBAL_STRUCTURE_MANAGEMENT'
],

  FACEBOOK_AD_PREVIEW_FORMATS : [ 
    'DESKTOP_FEED_STANDARD',
    'MOBILE_FEED_STANDARD',

    // 'INSTAGRAM_STANDARD',
    // 'INSTAGRAM_STORY',
    // 'INSTAGRAM_EXPLORE_GRID_HOME',
    // 'FACEBOOK_REELS_BANNER',
    // 'INSTAGRAM_REELS',


    // 'INSTAGRAM_SEARCH_CHAIN',
    // 'RIGHT_COLUMN_STANDARD',
    // 'INSTANT_ARTICLE_STANDARD',
    // 'MESSENGER_MOBILE_INBOX_MEDIA',
  ],

   testCustomAssets : {
    instagram: {
      story: 'https://dbid.supabase.co/storage/v1/object/public/randompublicbuckeyt/sweet.avif',
     
      explore:'https://dbid.supabase.co/storage/v1/object/public/randompublicbuckeyt/sign.avif',
     
      reels: 'https://dbid.supabase.co/storage/v1/object/public/randompublicbuckeyt//99D9751D-42C6-4A31-AC0C-C11E5AB52CEF_1000x.avif',
    },
    facebook: {
      story: 'https://dbid.supabase.co/storage/v1/object/public/randompublicbuckeyt/ball.avif',
     
      feed: 'https://dbid.supabase.co/storage/v1/object/public/randompublicbuckeyt//bunnykitchenwineglasslandscape.png'
    }
  },
  testVideoUrl: "https://commondatastorage.googleapis.com/gtv-videos-bucket/sample/BigBuckBunny.mp4",
  testImageUrl: "https://dbid.supabase.co/storage/v1/object/public/randompublicbuckeyt/ball.avif",
  testCustomVideoAssets: {
    "instagram": {
      "story": "https://sample-videos.com/video321/mp4/720/big_buck_bunny_720p_1mb.mp4",
      "explore": "https://commondatastorage.googleapis.com/gtv-videos-bucket/sample/BigBuckBunny.mp4",
      "reels": "https://commondatastorage.googleapis.com/gtv-videos-bucket/sample/ForBiggerBlazes.mp4"
    },
    "facebook": {
      "story": "https://commondatastorage.googleapis.com/gtv-videos-bucket/sample/ForBiggerFun.mp4",
      "feed": "https://sample-videos.com/video321/mp4/720/big_buck_bunny_720p_2mb.mp4"
    }
  },
  testUploadedCustomVideoAssetsIds: {
    "instagram": {
      "story": "3453063528169648",
      "explore": "1280582156800575",
      "reels": "4248753835346629"
    },
    "facebook": {
      "story": "3453063528169648",
      "feed": "4248753835346629"
    }
  },
  testUploadedCustomImageAssetsHashes: {
    "instagram": {
      "story": "92f13fa11197b1fa08ebde0dd453cda6",
      "explore": "d4902c33318fe8c838674b3938718f73",
      "reels": "d50b59f3f6c363cd492b7ebdf4de6cfe"
    },
    "facebook": {
      "story": "92f13fa11197b1fa08ebde0dd453cda6",
      "feed": "d50b59f3f6c363cd492b7ebdf4de6cfe"
    }
  },
  requirements : {
    instagram: {
      profile_feed: { 
        image: { minWidth: 600, minHeight: 400, aspectRatio: [4 / 5, 2.0], maxFileSize: 30 * 1024 * 1024 },
        video: { minWidth: 600, minHeight: 400, aspectRatio: [4 / 5, 2.0], maxFileSize: 4 * 1024 * 1024 * 1024, maxDuration: 241 * 60 }
      },
      feed: { 
        image: { minWidth: 600, minHeight: 600, aspectRatio: [1.0, 1.0], maxFileSize: 30 * 1024 * 1024 },
        video: { minWidth: 600, minHeight: 600, aspectRatio: [1.0, 1.0], maxFileSize: 4 * 1024 * 1024 * 1024, maxDuration: 241 * 60 }
      },
      story: { 
        image: { minWidth: 600, minHeight: 1067, aspectRatio: [ 0.5, 9 / 16], maxFileSize: 30 * 1024 * 1024 },
        video: { minWidth: 600, minHeight: 1067, aspectRatio: [ 0.5, 9 / 16], maxFileSize: 4 * 1024 * 1024 * 1024, maxDuration: 120 }
      },
      reels: { 
        image: { minWidth: 600, minHeight: 1067, aspectRatio: [0.5, 9 / 16], maxFileSize: 30 * 1024 * 1024 },
        video: { minWidth: 600, minHeight: 1067, aspectRatio: [0.5, 9 / 16], maxFileSize: 4 * 1024 * 1024 * 1024, maxDuration: 15 * 60 }
      },
      explore: { 
        image: { minWidth: 600, minHeight: 600, aspectRatio: [4 / 5, 2.0], maxFileSize: 30 * 1024 * 1024 },
        video: { minWidth: 600, minHeight: 600, aspectRatio: [4 / 5, 2.0], maxFileSize: 4 * 1024 * 1024 * 1024, maxDuration: 120 }
      },
      explore_home: { 
        image: { minWidth: 600, minHeight: 600, aspectRatio: [1.0, 1.0], maxFileSize: 30 * 1024 * 1024 },
        video: { minWidth: 600, minHeight: 600, aspectRatio: [1.0, 1.0], maxFileSize: 4 * 1024 * 1024 * 1024, maxDuration: 120 }
      },
    },
    facebook: {
      reels: { 
        image: { minWidth: 600, minHeight: 1067, aspectRatio: [0.5, 9 / 16], maxFileSize: 30 * 1024 * 1024 },
        video: { minWidth: 600, minHeight: 1067, aspectRatio: [0.5, 9 / 16], maxFileSize: 4 * 1024 * 1024 * 1024, maxDuration: 15 * 60 }
      },
      story: { 
        image: { minWidth: 600, minHeight: 1067, aspectRatio: [0.5, 9 / 16], maxFileSize: 30 * 1024 * 1024 },
        video: { minWidth: 600, minHeight: 1067, aspectRatio: [0.5, 9 / 16], maxFileSize: 4 * 1024 * 1024 * 1024, maxDuration: 120 }
      },
      feed: { 
        image: { minWidth: 600, minHeight: 315, aspectRatio: [1.0, 2.0], maxFileSize: 30 * 1024 * 1024 },
        video: { minWidth: 600, minHeight: 315, aspectRatio: [4/5, 2.0], maxFileSize: 4 * 1024 * 1024 * 1024, maxDuration: 241 * 60 }
      },
    },
    // Video-specific global settings
    video: {
      supportedFormats: ['mp4', 'mov', 'gif'],
      supportedCodecs: {
        video: 'h264',
        audio: 'aac'
      },
      minDimensions: { width: 120, height: 120 },
      maxFileSize: 4 * 1024 * 1024 * 1024, // 4GB
      defaultMaxDuration: 241 * 60 // 241 minutes in seconds
    },
    maxFileSize: 30 * 1024 * 1024, // Keep for backward compatibility
  },

  
  SUPABASE_Resource_Names: { 
    userTable: 'hosted_app_config_apps',
    accessTokensTable: 'hosted_app_config_access_tokens',
    metaApiCallsResultsTable: 'hosted_app_config_meta_api_calls_results', 
    pageTable: 'hosted_app_config_app_pages',
    pageMessagesTable: 'hosted_app_config_app_messages',
    pageCommentsTable: 'hosted_app_config_app_comments',
    pageConversationsTable: 'hosted_app_config_app_conversations',
    adAccountsTable: 'hosted_app_config_ad_accounts',
    campaignsTable: 'hosted_app_config_ad_campaigns',
    adSetsTable: 'hosted_app_config_ad_sets',
    adMediaTable: 'hosted_app_config_app_ad_media',
    adCreativesTable: 'hosted_app_config_ad_creatives',
    adsTable: 'hosted_app_config_ads',
    leadsTable: 'hosted_app_config_app_leads',
    contactsTable: 'hosted_app_config_app_contacts',
    businessManagersTable: 'hosted_app_config_app_business_managers',
    businessSystemUsersTable: 'hosted_app_config_app_business_system_users'
  },
  tables : {
    userTable: schema.hosted_app_config_appsInAudos,
    pageTable: schema.hosted_app_config_app_pagesInAudos,
    businessManagersTable: schema.hosted_app_config_app_business_managersInAudos,
    pageMessagesTable: schema.hosted_app_config_app_messagesInAudos,
    pageCommentsTable: schema.hosted_app_config_app_commentsInAudos,
    pageConversationsTable: schema.hosted_app_config_app_conversationsInAudos,
    accessTokensTable: schema.hosted_app_config_access_tokensInAudos,
    metaApiCallsResultsTable: schema.hosted_app_config_meta_api_calls_resultsInAudos,
    adAccountsTable: schema.hosted_app_config_ad_accountsInAudos,
    campaignsTable: schema.hosted_app_config_ad_campaignsInAudos,
    adSetsTable: schema.hosted_app_config_ad_setsInAudos,
    adsTable: schema.hosted_app_config_adsInAudos, 
    adCreativesTable: schema.hosted_app_config_ad_creativesInAudos,
    adMediaTable: schema.hosted_app_config_app_ad_mediaInAudos,
    leadsTable: schema.hosted_app_config_app_leadsInAudos,
    contactsTable: schema.hosted_app_config_app_contactsInAudos,
  },

  persistent_menu : [
    {
      locale: "default",
      call_to_actions: [
        {
          type: "postback",
          title: "Get Started",
          payload: "DEFAULT_GET_STARTED_PAYLOAD"
        },
        {
          type: "postback", 
          title: "Help & Support",
          payload: "DEFAULT_HELP_SUPPORT_PAYLOAD"
        },
        {
          type: "web_url",
          title: "Powered by Audos",
          url: process.env.DOMANIN_URL
        }
      ]
    }
  ],

  subscribeFields : ['messages,messaging_optins,message_echoes,message_deliveries,message_reads,message_edits,messaging_postbacks,messaging_optouts,message_reactions,feed,mention'],
  // subscribeFields : ['messages,messaging_payments,message_edits,messaging_postbacks,messaging_optouts,message_reactions,feed,ads_management,ads_read, *maybe* business_management'],
  // Instagram webhooks are currently working on an app level scope
  // igSubscribeFields : ['comments,live_comments,mentions,story_insights,'],


  // Threads subscribtion fields
  // subscribeFields : ['threads_basic,threads_content_publish,threads_manage_replies,threads_read_replies,threads_manage_insights'],

  // URL of your app domain
  appUrl: process.env.APP_URL,
  fbRedirectUri: `${process.env.APP_URL}/loginredirect`,
  fbBusinessRedirectUri: `${process.env.APP_URL}/businessloginredirect`,
  fbPreviewUri: `${process.env.APP_URL}/preview`,
  oauthUrl: `${process.env.APP_URL}/oauth`,
  linkedinRedirectUri: `${process.env.APP_URL}/linkedinloginredirect`,
  linkedinClientId: process.env.LINKEDIN_CLIENT_ID,
  linkedinClientSecret: process.env.LINKEDIN_CLIENT_SECRET,
  
  // LinkedIn API Configuration
  linkedinApiDomain: "https://api.linkedin.com",
  linkedinApiVersion: "rest",
  linkedinOauthDomain: "https://www.linkedin.com",
  
  // LinkedIn required headers
  get linkedinHeaders() {
    return {
      'LinkedIn-Version': '202507',
      'X-Restli-Protocol-Version': '2.0.0',
      'Content-Type': 'application/json'
    };
  },
  testUser: process.env.TEST_USER,

  // Preferred port (default to 3000)
  port: process.env.PORT || 3000,

  // Base URL for Messenger Platform API calls
  get apiUrl() {
    return `${this.apiDomain}/${this.apiVersion}`;
  },

  // URL of your webhook endpoint
  get webhookUrl() {
    return `${this.appUrl}/webhook`;
  },

  get whitelistedDomains() {
    return [this.appUrl, this.shopUrl];
  },  

  buildLoginUrl: function(clientId, state, configId, customRedirectUri?: string) {
    const redirectUri = customRedirectUri || this.fbRedirectUri;
    return `https://www.facebook.com/v23.0/dialog/oauth?client_id=${clientId}&redirect_uri=${encodeURIComponent(redirectUri)}&state=${state}&response_type=code&config_id=${configId}`;
    // return `https://www.facebook.com/v20.0/dialog/oauth?client_id=${clientId}&redirect_uri=${encodeURIComponent(redirectUri)}&state=${state}&response_type=code&config_id=${configId}`;
  },
  buildBusinessLoginUrl: function(clientId, state, configId, customRedirectUri?: string) {
    const businessRedirectUri = customRedirectUri || this.fbBusinessRedirectUri;
    return `https://www.facebook.com/v23.0/dialog/oauth?client_id=${clientId}&redirect_uri=${encodeURIComponent(businessRedirectUri)}&state=${state}&response_type=code&config_id=${configId}`;
  },
  buildLinkedinLoginUrl: function(clientId, state, customRedirectUri?: string) {
    const redirectUri = customRedirectUri || this.linkedinRedirectUri;

    const params = new URLSearchParams({
      response_type: 'code',
      client_id: clientId,
      redirect_uri: redirectUri,
      state: state, // Your user identifier
      scope: 'openid profile email r_basicprofile r_ads rw_ads r_ads_reporting r_organization_admin rw_organization_admin r_organization_social w_organization_social w_member_social r_1st_connections_size', // LinkedIn Marketing API Development Tier permissions
    });

    return `https://www.linkedin.com/oauth/v2/authorization?${params.toString()}`;
  },

  checkEnvVariables: function () {
    ENV_VARS.forEach(function (key) {
      if (!process.env[key]) {
        console.warn("WARNING: Missing the environment variable " + key);
      } else {
        // Check that urls use https
        if (["APP_URL", "SHOP_URL"].includes(key)) {
          const url = process.env[key];
          if (!url.startsWith("https://")) {
            console.warn(
              "WARNING: Your " + key + ' does not begin with "https://"'
            );
          }
        }
      }
    });
  },
  handleFbErrors : function (params) {
    // handleFbErrors : function (error: any, message: string = 'unknown error') {
    const {error, serverNote} = params;
    // console.error('xhxhxhError:', error);
    let errorObject = {
      message: null,
      error_code: null,
      error_subcode: null,
      error_user_title: null,
      error_user_msg: null,
      www_authenticate: null,
      fbtrace_id: null,
      error_message: null,
      audosFbErrorObject: false,
      success: false,
    };
    if (error?.response) {
      // Normalize Axios-like error shapes safely
      const errResponse = error.response?.data?.error ?? error.response?.data ?? error.response ?? error;
      const message = errResponse?.message ?? error?.message ?? null;
      errorObject = {
        error_message: message,
        error_code: errResponse?.code ?? errResponse?.error_code ?? error?.code ?? null,
        error_subcode: errResponse?.error_subcode ?? null,
        error_user_title: errResponse?.error_user_title ?? null,
        error_user_msg: errResponse?.error_user_msg ?? null,
        www_authenticate: error.response?.headers?.['www-authenticate'] ?? null,
        fbtrace_id: errResponse?.fbtrace_id ?? null,
        audosFbErrorObject: true,
        success: false,
        message: `serverNote: ${serverNote} - message: ${message} - error_code: ${errResponse?.error_code ?? errResponse?.code ?? ''} - error_subcode: ${errResponse?.error_subcode ?? ''} - error_user_title: ${errResponse?.error_user_title ?? ''} - error_user_msg: ${errResponse?.error_user_msg ?? ''} - www_authenticate: ${error.response?.headers?.['www-authenticate'] ?? ''}`
      }
      return errorObject;
    } else if (error?.audosFbErrorObject) {
      return error;
    } else {
      return { success: false, error_message: error?.message ?? String(error), message: `serverNote: ${serverNote} - `, audosFbErrorObject: true };
    }
    // return errorObject;
  },

  /**
   * Generate appsecret_proof for secure Facebook API requests
   * @param accessToken - The access token to generate proof for
   * @param appSecret - Optional app secret (defaults to current app's secret)
   * @returns The SHA256 HMAC hash for the appsecret_proof parameter
   */
  generateAppSecretProof: function(accessToken: string, appSecret?: string): string {
    const secret = appSecret || this.appSecret;
    return crypto.createHmac('sha256', secret).update(accessToken).digest('hex');
  }



};

export default config;
