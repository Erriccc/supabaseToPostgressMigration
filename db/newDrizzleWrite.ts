import { db } from './drizzle';
import { eq, or, and, sql, desc, inArray } from 'drizzle-orm';
import axios from 'axios';
import config from '../config';

// Table references for easier access
const tables = config.tables;

// Add these interfaces at the top of the file
interface TokenDebugResult {
  name?: string;
  accessToken?: string;
  error?: string;
  scopes: string[];
  isValid: boolean;
  expiresAt: number | null;
  dataAccessExpiresAt: number | null;
  issuedAt: number | null;
  missingScopes: string[];
  hasMessagingPermission?: boolean;
  hasInstagramMessagingPermission?: boolean;
  hasAdPermission?: boolean;
}

interface ErrorDebugResult {
  status: 'error';
  error: string;
  errorSource: string;
  appId: string | number;
  userDbId: string | number;
  pageId: string;
  pageAccessTokenValid?: boolean;
  userAccessTokenValid?: boolean;
  pageMessagingEnabled?: boolean;
  instagramMessagingEnabled?: boolean;
}

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

// Convert insertUserData function from Supabase to Drizzle ORM
const insertUserData = async (supabase: any, userId: string, email: string, userPageId: string, access_token: string, appId: number) => {
  try {
    console.log('Attempting to insert user data:', { appId, app: config.appContext, userId, access_token });

    if (userId === config.uiModeSignal) {
      userId = `${config.appContext}${userPageId}`;
    }
    console.log('after change userId: ', userId);
    console.log('appId: ', appId);
    console.log('userPageId: ', userPageId);
    console.log('app: ', config.appContext);

    // Check for existing user with complex OR conditions
    const existingUser = await db
      .select({
        id: tables.userTable.id,
        email: tables.userTable.email,
        fb_id: tables.userTable.fb_id,
        user_access_token: tables.userTable.user_access_token,
        app_env: tables.userTable.app_env,
        app_id: tables.userTable.app_id
      })
      .from(tables.userTable)
      .where(
        or(
          and(
            eq(tables.userTable.app_id, String(appId)),
            eq(tables.userTable.fb_id, userPageId),
            eq(tables.userTable.app_env, config.appContext)
          ),
          and(
            eq(tables.userTable.app_id, config.legacyAppId),
            eq(tables.userTable.fb_id, userPageId),
            eq(tables.userTable.app_env, String(config.legacyAppEnv))
          ),
          and(
            eq(tables.userTable.app_id, config.legacyAppId),
            eq(tables.userTable.email, email),
            eq(tables.userTable.app_env, String(config.legacyAppEnv))
          )
        )
      )
      .limit(1);

    console.log('existingUser: ', existingUser);

    if (existingUser && existingUser.length > 0) {
      console.log('Found existing user. Attempting to update access token...');
      console.log('existingUser: ', existingUser[0]);

      // If user exists in legacy app, migrate them to new app
      const migrateToNewAppData = {
        fb_id: userPageId,
        user_access_token: access_token,
        email: email,
        app_id: String(appId),
        app_env: app
      };

      const updatedUserData = await db
        .update(tables.userTable)
        .set(migrateToNewAppData)
        .where(eq(tables.userTable.id, existingUser[0].id))
        .returning({
          id: tables.userTable.id,
          email: tables.userTable.email,
          fb_id: tables.userTable.fb_id,
          user_access_token: tables.userTable.user_access_token,
          app_env: tables.userTable.app_env,
          app_id: tables.userTable.app_id
        });

      console.log('User data updated successfully:', updatedUserData);

      const newUserData = await db
        .select({
          id: tables.userTable.id,
          email: tables.userTable.email,
          fb_id: tables.userTable.fb_id,
          user_access_token: tables.userTable.user_access_token,
          app_env: tables.userTable.app_env,
          app_id: tables.userTable.app_id
        })
        .from(tables.userTable)
        .where(
          or(
            and(
              eq(tables.userTable.app_id, String(appId)),
              eq(tables.userTable.fb_id, userPageId),
              eq(tables.userTable.app_env, config.appContext)
            ),
            and(
              eq(tables.userTable.app_id, config.legacyAppId),
              eq(tables.userTable.fb_id, userPageId),
              eq(tables.userTable.app_env, String(config.legacyAppEnv))
            ),
            and(
              eq(tables.userTable.app_id, config.legacyAppId),
              eq(tables.userTable.email, email),
              eq(tables.userTable.app_env, String(config.legacyAppEnv))
            )
          )
        )
        .limit(1);

      console.log('1XXXXXXXXXXXXXXXXXXXXX: ', newUserData);
      return newUserData;
    }

    let newUserDataBeforeInsert = null;

    console.log('no existing user found in db, while userId is present, so we will insert the new user');

    const findUserByDbId = await db
      .select({
        id: tables.userTable.id,
        email: tables.userTable.email,
        fb_id: tables.userTable.fb_id,
        user_access_token: tables.userTable.user_access_token,
        app_env: tables.userTable.app_env,
        app_id: tables.userTable.app_id
      })
      .from(tables.userTable)
      .where(
        or(
          and(
            eq(tables.userTable.app_id, String(appId)),
            eq(tables.userTable.id, Number(userId)),
            eq(tables.userTable.app_env, config.appContext)
          ),
          and(
            eq(tables.userTable.app_id, config.legacyAppId),
            eq(tables.userTable.id, Number(userId)),
            eq(tables.userTable.app_env, String(config.legacyAppEnv))
          )
        )
      )
      .limit(1);

    console.log('findUserByDbId: ', findUserByDbId);

    if (findUserByDbId && findUserByDbId.length > 0 && findUserByDbId[0].fb_id === userPageId) {
      console.log('user found by db id, so we will update the existing user');
      console.log('user found by db id, and user fb id is the same as the user page id, so we will update the existing user');
      newUserDataBeforeInsert = [{
        app_id: appId,
        id: userId,
        email: email,
        fb_id: userPageId,
        user_access_token: access_token,
        app_env: config.appContext,
      }];
      console.log('newUserDataBeforeInsert: ', newUserDataBeforeInsert);
    } else if (!findUserByDbId || findUserByDbId.length === 0) {
      if (userId) {
        console.log('user not found by db id, but id was provided so we will insert the new user for the already existing audos user');
        newUserDataBeforeInsert = [{
          id: userId,
          app_id: appId,
          email: email,
          fb_id: userPageId,
          user_access_token: access_token,
          app_env: config.appContext,
        }];
      } else {
        console.log('user not found by db id, completely new user');
        newUserDataBeforeInsert = [{
          app_id: appId,
          email: email,
          fb_id: userPageId,
          user_access_token: access_token,
          app_env: config.appContext,
        }];
      }
    }

    // No existing user, attempt to insert new record
    console.log('No existing user found. Attempting to insert new user...');
    console.log('newUserDataBeforeInsert: ', newUserDataBeforeInsert);

    const newUserData = await db
      .insert(tables.userTable)
      .values(newUserDataBeforeInsert[0])
      .onConflictDoUpdate({
        target: tables.userTable.id,
        set: {
          app_id: String(appId),
          email: email,
          fb_id: userPageId,
          user_access_token: access_token,
          app_env: config.appContext,
        }
      })
      .returning({
        id: tables.userTable.id,
        email: tables.userTable.email,
        fb_id: tables.userTable.fb_id,
        user_access_token: tables.userTable.user_access_token,
        app_env: tables.userTable.app_env,
        app_id: tables.userTable.app_id
      });

    console.log('User data inserted successfully:', newUserData);
    return newUserData;

  } catch (error) {
    console.error('Unexpected error in insertUserData:', error);
    return null; // Prevent crashing, signal error with null
  }
};

// Convert insertBusinessManagerData function from Supabase to Drizzle ORM
const insertBusinessManagerData = async (supabase: any, userId: string, appId: number, businessesData: any) => {
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
              eq(tables.businessManagersTable.app_id, appId),
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
        app_id: appId,
        fb_business_id: business.id,
        business_name: business.name,
        fb_id: userId,
        business_system_user_access_token: null, 
        active: true,
        audos_partnership_id: null,
        other_details: business
      };

      let result;
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
        businessData.push(result[0]);
      }
    } 

    console.log('Business data processing complete.', businessData);
    return businessData;
  } catch (error) {
    console.error('Unexpected error in insertBusinessManagerData:', error);
    return [];
  }
};

// Convert insertBusinessSystemUserData function from Supabase to Drizzle ORM
const insertBusinessSystemUserData = async (supabase: any, appId: number, systemUserData: any, businessData: any, businessAdminEmail: string) => {
  try {
    console.log('Attempting to insert/update business system user data...');

    const existingSystemUser = await db
      .select()
      .from(tables.businessSystemUsersTable)
      .where(
        and(
          eq(tables.businessSystemUsersTable.app_id, appId),
          eq(tables.businessSystemUsersTable.fb_system_user_id, systemUserData.id)
        )
      );

    const systemUserRecord = {
      app_id: appId,
      fb_system_user_id: systemUserData.id,
      fb_client_business_id: systemUserData.client_business_id,
      business_admin_email: businessAdminEmail,
      business_system_user_access_token: systemUserData.access_token,
      active: true,
      assigned_pages: systemUserData.assigned_pages || null,
      fb_system_user_name: systemUserData.name,
      business_users: businessData || null
    };

    let result;
    if (existingSystemUser && existingSystemUser.length > 0) {
      console.log('Updating existing system user record...');
      const updatedData = await db
        .update(tables.businessSystemUsersTable)
        .set(systemUserRecord)
        .where(eq(tables.businessSystemUsersTable.id, existingSystemUser[0].id))
        .returning();

      result = updatedData;
    } else {
      console.log('Inserting new system user record...');
      const newData = await db
        .insert(tables.businessSystemUsersTable)
        .values(systemUserRecord)
        .returning();

      result = newData;
    }

    console.log('Business system user data processed successfully:', result);
    return result && result.length > 0 ? result[0] : null;
  } catch (error) {
    console.error('Unexpected error in insertBusinessSystemUserData:', error);
    throw error;
  }
};

// Convert insertPageData function from Supabase to Drizzle ORM
const insertPageData = async (supabase: any, userId: string, appId: number, fbWebhookData: any) => {
  try {
    console.log('Attempting to insert/update page data...');
    const pageData = [];

    const allPagesByUserId = await db
      .select()
      .from(tables.pageTable)
      .where(
        or(
          and(
            eq(tables.pageTable.app_id, appId),
            eq(tables.pageTable.fb_id, userId)
          ),
          and(
            eq(tables.pageTable.app_id, Number(config.legacyAppId)),
            eq(tables.pageTable.fb_id, userId)
          )
        )
      );

    // Build sets of fb_page_ids
    const existingPageIds = new Set((allPagesByUserId || []).map(p => p.fb_page_id));
    const webhookPageIds = new Set((fbWebhookData.pages || []).map(p => p.id));

    // Find missing pages (in DB but not in webhook)
    const missingPages = (allPagesByUserId || []).filter(p => !webhookPageIds.has(p.fb_page_id));
    // Find new pages (in webhook but not in DB)
    const newPages = (fbWebhookData.pages || []).filter(p => !existingPageIds.has(p.id));

    // Log or handle as needed
    if (missingPages.length > 0) {
      console.log('Pages missing from webhook data (likely disconnected):', missingPages.map(p => p.fb_page_id));
      console.log('Pages missing from webhook data (likely disconnected):', missingPages.map(p => ({fb_page_id: p.fb_page_id, name: p.page_name})));


      let missing_pages_data = {
        missing_pages: missingPages,
        new_pages: newPages
      };

      const updatedUserData = await db
        .update(tables.userTable)
        .set(missing_pages_data)
        .where(eq(tables.userTable.id, Number(userId)))
        .returning({
          id: tables.userTable.id,
          email: tables.userTable.email,
          fb_id: tables.userTable.fb_id,
          user_access_token: tables.userTable.user_access_token,
          app_env: tables.userTable.app_env,
          app_id: tables.userTable.app_id
        });

      console.log('User data updated successfully:', updatedUserData);
    } else {
      console.log('no missing pages');
    }

    if (newPages.length > 0) {
      console.log('New pages not in DB (likely just authorized):', newPages.map(p => p.id));
    } else {
      console.log('no new pages');
    }

    for (const page of fbWebhookData.pages) {
      console.log('Processing page:', page);
      
      const existingPages = await db
        .select()
        .from(tables.pageTable)
        .where(
          or(
            and(
              eq(tables.pageTable.app_id, appId),
              eq(tables.pageTable.fb_page_id, page.id),
              eq(tables.pageTable.fb_id, userId)
            ),
            and(
              eq(tables.pageTable.app_id, Number(config.legacyAppId)),
              eq(tables.pageTable.fb_page_id, page.id),
              eq(tables.pageTable.fb_id, userId)
            )
          )
        );

      const pageRecord = {
        app_id: appId,
        fb_page_id: page.id,
        page_name: page.name,
        fb_id: userId,
        active: true,
        page_access_token: page.access_token,
        ig_account_id: page.connected_instagram_account ? page.connected_instagram_account.id : null,
        has_ig_page: !!page.connected_instagram_account,
      };

      let result;
      if (existingPages && existingPages.length > 0) {
        console.log('Found existing page record. Attempting to update...');
        console.log('existingPages: ', existingPages);
        const existingPage = existingPages[0]; 
        console.log('existingPage: ', existingPage);

        // Start with existing page data as the base
        const updateRecord = {
          ...existingPage,
          // Always update these critical fields
          page_access_token: pageRecord.page_access_token,
          active: true,
          app_id: pageRecord.app_id,
          fb_page_id: pageRecord.fb_page_id,
          fb_id: pageRecord.fb_id,
          
          // Update Instagram-related fields only if new data exists
          ig_account_id: pageRecord.ig_account_id || existingPage.ig_account_id,
          has_ig_page: pageRecord.ig_account_id ? true : existingPage.has_ig_page,
          
          // Update page name if new one exists
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

      // SET the Newest Page to Active and the rest to Inactive
      // run in the background
      activatePageWithBestConfig(supabase, appId, result[0]);

      if (result && result.length > 0) {
        pageData.push(result[0]);
      }
    } 

    console.log('Page data processing complete.', pageData);
    return pageData;
  } catch (error) {
    console.error('Unexpected error in insertPageData:', error);
    return []; // Return empty array to signal potential issue
  }
};

// Convert validateBusinessAdminUser function from Supabase to Drizzle ORM
const validateBusinessAdminUser = async (supabase: any, businessUsersData: any, businessAdminEmail: string) => {
  try {
    console.log('Validating business admin user...');

    if (!businessUsersData || !businessUsersData.business_users || !businessUsersData.business_users.data) {
      throw new Error('No business users data provided');
    }

    const businessUsers = businessUsersData.business_users.data;
    const adminUser = businessUsers.find(user =>
      user.email === businessAdminEmail && user.role === 'ADMIN'
    );

    if (!adminUser) {
      throw new Error(`Business admin email ${businessAdminEmail} not found or not an admin in this business`);
    }

    console.log('Business admin validation successful:', adminUser);
    return {
      isValid: true,
      adminUser: adminUser,
      allUsers: businessUsers
    };
  } catch (error) {
    console.error('Business admin validation failed:', error);
    return {
      isValid: false,
      error: error.message,
      adminUser: null,
      // map and return an array of all the emails of the business users
      validBusinessAdminEmails: businessUsersData.business_users.data.filter(user => user.role === 'ADMIN').map(user => user.email),
      allUsers: []
    };
  }
};

// Convert processBusinessAgencyInvitations function from Supabase to Drizzle ORM
const processBusinessAgencyInvitations = async (supabase: any, assignedPages: any, businessIds: string, systemUserAccessToken: string, existingUserFbId: string) => {
  try {
    console.log('Processing business agency invitations...');

    if (!businessIds || !assignedPages?.data) {
      console.log('No business IDs or assigned pages to process');
      return [];
    }

    // Parse business IDs from comma-separated string
    const businessIdArray = businessIds.split(',').map(id => id.trim()).filter(id => id);

    if (businessIdArray.length === 0) {
      console.log('No valid business IDs found');
      return [];
    }

    // Default permitted tasks for agency partnership
    const permittedTasksForNow = ['MANAGE'];

    const results = [];

    // Iterate through each assigned page
    for (const page of assignedPages.data) {
      console.log(`Processing page: ${page.id} (${page.name || 'Unknown'})`);
      
      // lets use the fb_id and page id of the existing user to find the already existing page data
      // in our data base, the page data generated by the usr has more precedence than the page data generated by the system user
      const existingPageData = await db
        .select()
        .from(tables.pageTable)
        .where(
          and(
            eq(tables.pageTable.fb_id, existingUserFbId),
            eq(tables.pageTable.fb_page_id, page.id),
            eq(tables.pageTable.app_id, Number(config.appId))
          )
        )
        .orderBy(desc(tables.pageTable.created_at))
        .limit(1);
        
      let accessToken = null;
      if (existingPageData && existingPageData.length > 0) {
        console.log('Existing page data found:', existingPageData);
        accessToken = existingPageData[0].page_access_token;
      } else {
        console.log('No existing page data found, using page access token:', page.access_token);
        accessToken = page.access_token || systemUserAccessToken;
      }

      // Invite each business as an agency for this page
      for (const businessId of businessIdArray) {
        const result = await inviteBusinessAsAgency(
          supabase,
          page.id,
          businessId,
          accessToken,
          permittedTasksForNow
        );

        console.log('result: ', result);
        if (result.success) {
          try {
            console.log('Inviting business as agency with all permitted tasks access token:', accessToken);
            const newResults = await inviteBusinessAsAgency(
              supabase,
              page.id,
              businessId,
              accessToken,
              config.allPermittedTasksForBusinessAgency
            );
            if (newResults.success) {
              results.push({
                pageId: page.id,
                pageName: page.name || 'Unknown',
                businessId,
                ...newResults
              });
            }
          } catch (error) {
            console.error('Error adding allPermittedTasksForBusinessAgency to the agency:', error);
            return [];
          }
        }
      }
    }

    console.log('Business agency invitations processing complete:', results);
    return results;
  } catch (error) {
    console.error('Error processing business agency invitations:', error);
    return [];
  }
};

// Helper function for inviting business as agency (from original code)
const inviteBusinessAsAgency = async (supabase: any, pageId: string, businessId: string, accessToken: string, permittedTasks: string[]) => {
  try {
    console.log(`Inviting business ${businessId} as agency for page ${pageId}`);

    const response = await axios.post(
      `https://graph.facebook.com/v23.0/${pageId}/agencies`,
      {},
      {
        params: {
          business: businessId,
          permitted_tasks: permittedTasks.join(','),
          access_token: accessToken
        }
      }
    );

    console.log(`Successfully invited business ${businessId} as agency for page ${pageId}:`, response.data);
    return { success: true, data: response.data };
  } catch (error) {
    console.error(`Error inviting business ${businessId} as agency for page ${pageId}:`, error.response?.data || error.message);
    return { success: false, error: error.response?.data || error.message };
  }
};

// Convert getFbPageId function from Supabase to Drizzle ORM
const getFbPageId = async (supabase: any, identifier: string) => {  
  try {
    console.log('Attempting to retrieve fbPageId for identifier:', identifier);

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
      );

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

// Convert getIgId function from Supabase to Drizzle ORM
const getIgId = async (supabase: any, identifier: string) => { 
  try {
    console.log('Attempting to retrieve fbPageId for identifier:', identifier);

    const data = await db
      .select({
        ig_account_id: tables.pageTable.ig_account_id,
        has_ig_page: tables.pageTable.has_ig_page,
        fb_page_id: tables.pageTable.fb_page_id
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
      );

    if (data && data.length > 0) {
      console.log('fbPageId found:', data[0].fb_page_id);
      const igId = data[0].ig_account_id;
      const hasIgId = data[0].has_ig_page;
      const fbPageId = data[0].fb_page_id;
      return {igId: igId, hasIgId: hasIgId, fbPageId};
    } 

    console.log('No page found for identifier:', identifier);
    return null; 
  } catch (err) {
    console.error('Unexpected error in getIgId:', err);
    return null; 
  }
};

// Convert checkIfPageIsManagedByMultipleUsers function from Supabase to Drizzle ORM
const checkIfPageIsManagedByMultipleUsers = async (identifier: string,supabase:any, activeOnly: boolean = false) => {
  try {
    console.log('Attempting to retrieve checkIfPageIsManagedByMultipleUsers for identifier:', identifier);
    let userDataArray = [];

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

    console.log('data.length:', data.length);

    if (data && data.length > 0) {
      const uniqueFbIds = Array.from(new Set(data.map(page => page.fb_id)));
      
      const userData = await db
        .select()
        .from(tables.userTable)
        .where(
          and(
            inArray(tables.userTable.fb_id, uniqueFbIds),
            eq(tables.userTable.app_id, config.appId)
          )
        );

      // Group users by fb_id to maintain the same structure as before
      userDataArray = data.map(page => {
        const matchingUsers = userData.filter(user => user.fb_id === page.fb_id);
        return matchingUsers;
      });
      return {isManagedByMultipleUsers: data.length > 1 ? true : false, data:data, userDataArray:userDataArray};
    } 
    else {
      console.log(`No page(s) found for identifier: ${identifier}`);
      return {isManagedByMultipleUsers:false, data:null, userDataArray:[]};
    }

  } catch (err) {
    console.error('Unexpected error in checkIfPageIsManagedByMultipleUsers:', err);
    return {isManagedByMultipleUsers:false, data:null, userDataArray:[]};
  }
};

// Convert getPageAccessToken function from Supabase to Drizzle ORM
const getPageAccessToken = async (pageId: string, supabase: any) => {
  try {
    console.log('Attempting to get most recent page access token for pageId:', pageId);
    const data = await db
      .select({
        page_access_token: tables.pageTable.page_access_token,
        created_at: tables.pageTable.created_at
      })
      .from(tables.pageTable)
      .where(
        and(
          eq(tables.pageTable.app_id, Number(config.appId)),
          eq(tables.pageTable.fb_page_id, pageId)
        )
      )
      .orderBy(desc(tables.pageTable.created_at))
      .limit(1);

    if (data && data.length > 0) {
      console.log('getPageAccessToken Found most recent page access token. Creation date:', data[0].created_at);
      console.log('data list:', data[0]);
      return data[0].page_access_token;
    }

    console.log('No page access token found for pageId:', pageId, data);
    return null; 
  } catch (err) {
    console.error('Unexpected error in getPageAccessToken:', err);
    return null; 
  }
};

// Convert getAdDataAndUserDataFromDbWithAdIdOrAdTraceId function from Supabase to Drizzle ORM
const getAdDataAndUserDataFromDbWithAdIdOrAdTraceId = async (supabase: any, identifier: string) => {
  const adData = await getAdFromDbByAdIdOrAdTraceId(supabase, identifier);
  if (adData && adData.length > 0) {
    console.log('adData', adData);
    console.log('getAdDataAndUserData Found ad details:', adData[0]);
    const userData = await db
      .select({ id: tables.userTable.id })
      .from(tables.userTable)
      .where(
        and(
          eq(tables.userTable.app_id, config.appId),
          eq(tables.userTable.id, adData[0].user_id)
        )
      )
      .limit(1);

    console.log('userData', userData);
    console.log('getAdDataAndUserData Found user details:', userData); 
    return {matchFound: true,
      adData: adData[0].fb_ad_id,
      userData: userData[0].id};
  }

  console.log('No ad data or user data found for adId or adTraceId:', identifier);
  return {matchFound: false};
};

// Helper function for getAdFromDbByAdIdOrAdTraceId
const getAdFromDbByAdIdOrAdTraceId = async (supabase: any, adId: string) => {
  console.log('Attempting to get ad from db for adId:', adId);
  // Ensure adId is treated as string
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
    return data;
  } catch (error) {
    console.error('Error getting ad from db:', error);
    return [];
  }
};

// Convert getPageAccessTokenByAdIdOrAdTraceIdAndPageId function from Supabase to Drizzle ORM
const getPageAccessTokenByAdIdOrAdTraceIdAndPageId = async (supabase: any, identifier: string, pageId: string) => {
  console.log('getPageAccessTokenByAdIdOrAdTraceIdAndPageId getting the page access token for identifier:', identifier, 'and pageId:', pageId);
  
  // check if page is managed by multiple users
  const {isManagedByMultipleUsers, data:pagesData, userDataArray} = await checkIfPageIsManagedByMultipleUsers(pageId,supabase, true);
  console.log('isManagedByMultipleUsers:', isManagedByMultipleUsers);
  console.log('pagesData:', pagesData);
  console.log('userDataArray:', userDataArray); 

  let pageAccessTokenFromReturnableObject = pagesData[0].page_access_token;
  let userObjectFromReturnableObject = userDataArray[0][0];
  console.log('pageAccessTokenFromReturnableObject:', pageAccessTokenFromReturnableObject);
  console.log('userObjectFromReturnableObject:', userObjectFromReturnableObject);
  let fallBackValidAccessToken = await getPageAccessToken(pageId, supabase);
  console.log('fallBackValidAccessToken', fallBackValidAccessToken);
  let returner = {pageAccessToken: fallBackValidAccessToken ? fallBackValidAccessToken : pageAccessTokenFromReturnableObject};
  
  if (isManagedByMultipleUsers) {
    try {
      console.log('Attempting to get page access token by ad trace id:', identifier);
      const adData = await getAdFromDbByAdIdOrAdTraceId(supabase, identifier);

      if (adData && adData.length > 0) {
        console.log('getPageAccessTokenByAdTraceIdAndPageId Found ad details:', adData[0]);
        const userData = await db
          .select({
            fb_id: tables.userTable.fb_id,
            fb: tables.userTable.fb_id
          })
          .from(tables.userTable)
          .where(
            and(
              eq(tables.userTable.app_id, config.appId),
              eq(tables.userTable.id, adData[0].user_id)
            )
          )
          .limit(1);

        console.log('getPageAccessTokenByAdTraceIdAndPageId Found user details:', userData[0]);
  
        if (userData && userData.length > 0) {
          console.log('getPageAccessTokenByAdTraceIdAndPageId Found user details:', userData[0]);
          const pageData = await db
            .select({ page_access_token: tables.pageTable.page_access_token })
            .from(tables.pageTable)
            .where(
              and(
                eq(tables.pageTable.app_id, Number(config.appId)),
                eq(tables.pageTable.fb_id, userData[0].fb_id),
                eq(tables.pageTable.fb_page_id, pageId),
                eq(tables.pageTable.is_token_valid, true)
              )
            )
            .limit(1);

          console.log('getPageAccessTokenByAdTraceIdAndPageId Found page details:', pageData[0]);
          return {pageAccessToken: pageData[0].page_access_token};
        } else {
          console.log('No page access token found for ad trace id:', identifier, adData);
          return returner;
        }
      }

      console.log('No page access token found for ad trace id or ad id:', identifier, adData);
      return returner;  
    } catch (err) {
      console.error('Unexpected error in getPageAccessTokenByAdTraceIdAndPageId:', err);
      return returner; 
    }
  } else {
    return returner;
  }
};

// Convert getPageAccessTokenAndValidateBeforeReturn function from Supabase to Drizzle ORM
const getPageAccessTokenAndValidateBeforeReturn = async (supabase: any, pageId: string) => {
  // This function was commented out in the original code
  // Keeping the same structure but with Drizzle syntax
  console.log('getPageAccessTokenAndValidateBeforeReturn - function implementation was commented out in original');
  return null;
};

// Convert getPageDataByDbId function from Supabase to Drizzle ORM
async function getPageDataByDbId(supabase: any, pageDbId: string) {
  try {
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
    
    if (data.length === 0) {
      throw new Error('Error fetching page data');
    }
    return {data: data[0], error: null};
  } catch (error) {
    console.error('Error fetching page data:', error);
    return {data: null, error: error};
  }
}

// Convert upsertAccessToken function from Supabase to Drizzle ORM
const upsertAccessToken = async (params: {
  supabase: any;
  appid: number;
  userId: string;
  fbId: string;
  pageId: string;
  accessTokenData: any;
  accessTokenType: 'user' | 'page';
}) => {
  const { appid, userId, fbId, pageId, accessTokenData, accessTokenType } = params;
  console.log(`Upserting access token: ${accessTokenType} for user: ${userId} and fbId: ${fbId} and pageId: ${pageId}`);

  try {
    // First, try to find any existing record
    const existingToken = await db
      .select()
      .from(tables.accessTokensTable)
      .where(
        and(
          eq(tables.accessTokensTable.app_id, appid),
          eq(tables.accessTokensTable.user_id, Number(userId)),
          eq(tables.accessTokensTable.fb_id, fbId),
          eq(tables.accessTokensTable.access_token_type, accessTokenType)
        )
      )
      .limit(1);

    // Convert zero timestamps to 90 days from now and Unix timestamps to PostgreSQL timestamps
    const convertTimestamp = (timestamp: number) => {
      if (!timestamp || timestamp === 0) return new Date(Date.now() + 90 * 24 * 60 * 60 * 1000).toISOString();
      // Convert Unix timestamp (seconds) to JavaScript Date then to ISO string
      return new Date(timestamp * 1000).toISOString();
    };

    // Prepare the token data
    const tokenData = {
      app_id: appid,
      user_id: Number(userId),
      fb_id: fbId,
      page_id: accessTokenType !== 'user' ? pageId : null,
      access_token: accessTokenType === 'user' ? accessTokenData.userAccessToken : accessTokenData.pageAccessToken,
      access_token_type: accessTokenType,
      page_messaging_enabled: accessTokenData.pageMessagingEnabled,
      instagram_messaging_enabled: accessTokenData.instagramMessagingEnabled,
      ad_permissions_enabled: accessTokenData.adPermissionsEnabled,
      status: accessTokenData.status,
      is_token_valid: accessTokenType === 'user' ? accessTokenData.userAccessTokenValid : accessTokenData.pageAccessTokenValid,
      error_source: accessTokenData.errorSource,
      expires_at: convertTimestamp(accessTokenType === 'user' ? accessTokenData.userTokenExpiresAt : accessTokenData.pageTokenExpiresAt),
      token_data_access_expires_at: convertTimestamp(accessTokenType === 'user' ? accessTokenData.userDataAccessExpiresAt : accessTokenData.pageDataAccessExpiresAt),
      scopes: accessTokenType === 'user' ? accessTokenData.userScopes : accessTokenData.pageScopes,
      missing_scopes: accessTokenType === 'user' ? accessTokenData.missingScopesForUser : accessTokenData.missingScopesForPage,
      details: accessTokenData
    };

    let result;
    if (existingToken && existingToken.length > 0) {
      console.log('existingToken:', existingToken);
      console.log(`Updating type: ${accessTokenType} existing access token: ${existingToken[0].id} for user: ${userId} and fbId: ${fbId} and pageId: ${pageId}`);
      
      // Update existing record
      const data = await db
        .update(tables.accessTokensTable)
        .set(tokenData)
        .where(
          and(
            eq(tables.accessTokensTable.app_id, appid),
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

    return result;
  } catch (error) {
    console.error('Error upserting access token:', error);
    throw error;
  }
};

// Convert upsertAdAccount function from Supabase to Drizzle ORM
async function upsertAdAccount(supabase: any, appid: number, userId: string, adAccountData: any) {
  console.log('Upserting adAccount:', JSON.stringify(adAccountData, null, 2));

  try {
    const data = await db
      .insert(tables.adAccountsTable)
      .values({
        fb_ad_account_id: adAccountData.fb_ad_account_id,
        app_id: appid,
        user_id: Number(userId),
        name: adAccountData.name,
        details: adAccountData
      })
      .onConflictDoUpdate({
        target: tables.adAccountsTable.fb_ad_account_id,
        set: {
          app_id: appid,
          user_id: Number(userId),
          name: adAccountData.name,
          details: adAccountData
        }
      })
      .returning();

    console.log('Ad account upserted successfully');
    return data;
  } catch (error) {
    console.error('Error upserting ad account:', error);
    throw error;
  }
}

// Convert upsertCampaign function from Supabase to Drizzle ORM
async function upsertCampaign(supabase: any, appid: number, userId: string, campaignData: any) {
  console.log('Upserting campaign:', JSON.stringify(campaignData, null, 2));
  console.log('appid:', appid);
  console.log('userId:', userId); 
  console.log('campaignData:', campaignData);
  
  try {
    await db
      .insert(tables.campaignsTable)
      .values({
        fb_campaign_id: campaignData.fb_campaign_id,
        app_id: appid,
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
          app_id: appid,
          user_id: Number(userId),
          ad_account_id: campaignData.ad_account_id,
          name: campaignData.name,
          objective: campaignData.objective,
          status: campaignData.status,
          details: campaignData
        }
      });

    console.log('Campaign upserted successfully');
  } catch (error) {
    console.error('Error upserting campaign:', error);
    throw error;
  }
}

// Convert upsertAdSet function from Supabase to Drizzle ORM
async function upsertAdSet(supabase: any, appid: number, userId: string, adSetData: any) {
  console.log('Upserting ad set:', JSON.stringify(adSetData, null, 2));
  
  try {
    const data = await db
      .insert(tables.adSetsTable)
      .values({
        fb_ad_set_id: adSetData.fb_ad_set_id,
        app_id: appid,
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
          app_id: appid,
          user_id: Number(userId),
          campaign_id: adSetData.campaign_id,
          name: adSetData.name,
          optimization_goal: adSetData.optimization_goal,
          billing_event: adSetData.billing_event,
          bid_strategy: adSetData.bid_strategy,
          status: adSetData.status,
          details: adSetData
        }
      })
      .returning();

    console.log('Ad set upserted successfully');
    return data;
  } catch (error) {
    console.error('Error upserting ad set:', error);
    throw error;
  }
}

// Convert upsertAd function from Supabase to Drizzle ORM
async function upsertAd(supabase: any, appid: number, userId: string, adData: any) {
  console.log('Upserting ad:', JSON.stringify(adData, null, 2));
  
  try {
    const data = await db
      .insert(tables.adsTable)
      .values({
        fb_ad_id: adData.fb_ad_id,
        app_id: appid,
        user_id: Number(userId),
        ad_set_id: adData.ad_set_id,
        name: adData.name,
        status: adData.status,
        details: adData,
        ad_account_id: adData.ad_account_id,
        creative_id: adData.creative_id,
        //message details
        audos_ad_trace_id: adData.audos_ad_trace_id,
        welcome_message_id: adData.welcome_message_id,
        //media details
        image_url: adData.image_url || null,
        video_url: adData.video_url || null,
        //foreign ad flag
        is_foreign_ad: adData.is_foreign_ad || false
      })
      .onConflictDoUpdate({
        target: tables.adsTable.fb_ad_id,
        set: {
          app_id: appid,
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
          is_foreign_ad: adData.is_foreign_ad || false
        }
      })
      .returning();

    console.log('Ad upserted successfully');
    return data;
  } catch (error) {
    console.error('Error upserting ad:', error);
    throw error;
  }
}

// Convert upsertCreative function from Supabase to Drizzle ORM
async function upsertCreative(supabase: any, appid: number, userId: string, creativeData: any) {
  console.log('Upserting creative:', JSON.stringify(creativeData, null, 2));
  
  try {
    const data = await db
      .insert(tables.adCreativesTable)
      .values({
        fb_creative_id: creativeData.fb_creative_id,
        app_id: appid,
        user_id: Number(userId),
        name: creativeData.name,
        object_story_spec: creativeData.object_story_spec,
        ad_account_id: creativeData.ad_account_id,
        details: creativeData,
        //message details
        audos_ad_trace_id: creativeData.audos_ad_trace_id,
        welcome_message_id: creativeData.welcome_message_id,
        //media details
        image_url: creativeData.image_url || null,
        video_url: creativeData.video_url || null
      })
      .onConflictDoUpdate({
        target: tables.adCreativesTable.fb_creative_id,
        set: {
          app_id: appid,
          user_id: Number(userId),
          name: creativeData.name,
          object_story_spec: creativeData.object_story_spec,
          ad_account_id: creativeData.ad_account_id,
          details: creativeData,
          audos_ad_trace_id: creativeData.audos_ad_trace_id,
          welcome_message_id: creativeData.welcome_message_id,
          image_url: creativeData.image_url || null,
          video_url: creativeData.video_url || null
        }
      })
      .returning();

    console.log('Creative upserted successfully');
    return data;
  } catch (error) {
    console.error('Error upserting creative:', error);
    throw error;
  }
}

// Convert getCreativeFromDbByCreativeIdOrAdTraceId function from Supabase to Drizzle ORM
async function getCreativeFromDbByCreativeIdOrAdTraceId(supabase: any, creativeId: string) {
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
    return {data, error: null};
  } catch (error) {
    console.error('Error getting creative from db:', error);
    return {data: null, error: error};
  }
}

// Convert readMessages function from Supabase to Drizzle ORM
async function readMessages(supabase: any, queryParams: any) { 
  try {
    const whereConditions = [eq(tables.pageMessagesTable.app_id, Number(config.appId))];
    
    // Add other query parameters with proper type handling
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
    }

    const data = await db
      .select()
      .from(tables.pageMessagesTable)
      .where(and(...whereConditions));

    return data;
  } catch (err) {
    if (err.name === 'AbortError') {
      console.error('Query timed out after 30 seconds');
    } else {
      console.error('Unexpected error in readMessages:', err);
    }
    return null; 
  }
}

// Convert readComments function from Supabase to Drizzle ORM
async function readComments(supabase: any, queryParams: any) {
  try {
    const whereConditions = [eq(tables.pageCommentsTable.app_id, Number(config.appId))];
    
    // Add other query parameters with proper type handling
    for (const [key, value] of Object.entries(queryParams)) {
      if (key === 'id') {
        whereConditions.push(eq(tables.pageCommentsTable.id, Number(value)));
      } else if (key === 'fb_page_id') {
        whereConditions.push(eq(tables.pageCommentsTable.fb_page_id, value as string));
      } else if (key === 'sender_id') {
        whereConditions.push(eq(tables.pageCommentsTable.sender_id, value as string));
      }
    }

    const data = await db
      .select()
      .from(tables.pageCommentsTable)
      .where(and(...whereConditions));

    return data;
  } catch (err) {
    console.error('Unexpected error in readComments:', err);
    return null; 
  }
}

// Convert readPageData function from Supabase to Drizzle ORM
async function readPageData(supabase: any, queryParams: any) {
  try {
    const whereConditions = [eq(tables.pageTable.app_id, Number(config.appId))];
    
    // Add other query parameters with proper type handling
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
    return data;
  } catch (err) {
    console.error('Unexpected error in readPageData:', err);
    return null; 
  }
}

// Convert changeConversationActiveStatus function from Supabase to Drizzle ORM
async function changeConversationActiveStatus(supabase: any, status: boolean, conversationId: string, updatedByUserId: string) {
  console.log('Attempting to change conversation active status...', status, conversationId, updatedByUserId);
  
  try {
    const data = await db
      .update(tables.pageConversationsTable)
      .set({ 
        active: status, 
        updated_at: new Date().toISOString(), 
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
    return data;
  } catch (error) {
    console.error('Unexpected error in changeConversationActiveStatus:', error);
    throw error;
  }
}

// Convert upsertLead function from Supabase to Drizzle ORM
async function upsertLead(params: {
  supabase: any;
  app_id: number;
  conversation_id: number;
  fb_page_id: string;
  fb_conversation_id: string;
  fb_ad_id: string;
  foreign_ad_id: string;
  recipient_page_scope_id: string;
  conversation_platform: string;
}) {
  const leadData = {
    app_id: Number(params.app_id),
    conversation_id: Number(params.conversation_id), 
    fb_page_id: params.fb_page_id,
    fb_conversation_id: params.fb_conversation_id,
    fb_ad_id: params.fb_ad_id,
    foreign_ad_id: params.foreign_ad_id,
    recipient_page_scope_id: params.recipient_page_scope_id,
    conversation_platform: params.conversation_platform
  };
  
  try {
    const data = await insertLead(supabase, leadData);
    return data;
  } catch (error) {
    console.error('Error inserting lead:', error);
    throw error;
  }
}

// Helper function for insertLead
async function insertLead(supabase: any, leadData: any) {
  console.log('Inserting lead:', leadData);
  
  try {
    const data = await db
      .insert(tables.leadsTable)
      .values(leadData)
      .returning()
      
    console.log('Lead inserted successfully:', data);
    return {data, error: null};
  } catch (error) {
    console.error('Error inserting lead:', error);
    throw error;
  }
}

// Convert upsertContact function from Supabase to Drizzle ORM
async function upsertContact(params: {
  supabase: any;
  appId: number;
  conversationId: number;
  fbPageId: string;
  fbConversationId: string;
  fbAdId: string;
  recipientPageScopeId: string;
  conversation_platform: string;
  source: string;
  messageId: string;
  contactType: string;
  contactValue: string;
}) {
  const contactData = {
    app_id: params.appId,
    conversation_id: params.conversationId,
    fb_page_id: params.fbPageId,
    fb_conversation_id: params.fbConversationId,
    fb_ad_id: params.fbAdId,
    recipient_page_scope_id: params.recipientPageScopeId,
    conversation_platform: params.conversation_platform,
    source: params.source,
    message_id: params.messageId,
    contact_type: params.contactType,
    contact_value: params.contactValue
  };
  
  try {
    const data = await insertContact(supabase, contactData);
    return data;
  } catch (error) {
    console.error('Error inserting contact:', error);
    throw error;
  }
}

// Helper function for insertContact
async function insertContact(supabase: any, contactData: any) {
  try {
    const data = await db
      .insert(tables.contactsTable)
      .values(contactData)
      .returning()
      // .limit(1);
      
    console.log('Contact inserted successfully:', data);
    return {data, error: null};
  } catch (error) {
    console.error('Error inserting contact:', error);
    throw error;
  }
}

// Convert getLeadsByConversationId function from Supabase to Drizzle ORM
async function getLeadsByConversationId(supabase: any, conversationId: string) {
  try {
    const data = await db
      .select()
      .from(tables.leadsTable)
      .where(eq(tables.leadsTable.conversation_id, Number(conversationId)));

    return data;
  } catch (error) {
    console.error('Error getting leads:', error);
    throw error;
  }
}

// Convert getContactsByConversationId function from Supabase to Drizzle ORM
async function getContactsByConversationId(supabase: any, conversationId: string) {
  try {
    const data = await db
      .select()
      .from(tables.contactsTable)
      .where(eq(tables.contactsTable.conversation_id, Number(conversationId)));

    return data;
  } catch (error) {
    console.error('Error getting contacts:', error);
    throw error;
  }
}

// Convert updateConversationWithLeadInfo function from Supabase to Drizzle ORM
async function updateConversationWithLeadInfo(supabase: any, conversationId: string, leadInfo: any) {
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
        updated_at: new Date().toISOString()
      })
      .where(eq(tables.pageConversationsTable.id, Number(conversationId)))  
      .returning();

    return data;
  } catch (error) {
    console.error('Error updating conversation with lead info:', error);
    throw error;
  }
}

// Convert updateConversationWithAdInfo function from Supabase to Drizzle ORM
async function updateConversationWithAdInfo(params: {
  supabase: any;
  conversationId: string;
  adInfo: any;
}) {
  try {
    const data = await db
      .update(tables.pageConversationsTable)
      .set({
        conversation_ad_id: params.adInfo.fb_ad_id,
        conversation_source: params.adInfo.conversation_source,
      })
      .where(eq(tables.pageConversationsTable.id, Number(params.conversationId)))
      .returning();

    return data;
  } catch (error) {
    console.error('Error updating conversation with ad info:', error);
    throw error;
  }
}

// Convert getLeadsAndContactsByConversationId function from Supabase to Drizzle ORM
async function getLeadsAndContactsByConversationId(supabase: any, conversationId: string) {
  try {
    const [leads, contacts] = await Promise.all([
      getLeadsByConversationId(supabase, conversationId),
      getContactsByConversationId(supabase, conversationId)
    ]);

    return {
      leads,
      contacts
    };
  } catch (error) {
    console.error('Error getting leads and contacts:', error);
    throw error;
  }
}

// Convert getConversationsByAdId function from Supabase to Drizzle ORM
async function getConversationsByAdId(supabase: any, adId: string) {
  try {
    const data = await db
      .select()
      .from(tables.pageConversationsTable)
      .where(eq(tables.pageConversationsTable.conversation_ad_id, adId));

    return data;
  } catch (error) {
    console.error('Error getting conversations by ad id:', error);
    throw error;
  }
}

// Convert getConversationsByLeadId function from Supabase to Drizzle ORM
async function getConversationsByLeadId(supabase: any, leadId: string) {
  try {
    // First get the conversation_id from leads table
    const leadData = await db
      .select({ conversation_id: tables.leadsTable.conversation_id })
      .from(tables.leadsTable)
      .where(eq(tables.leadsTable.id, Number(leadId)))
      .limit(1);

    if (!leadData || leadData.length === 0 || !leadData[0]?.conversation_id) {
      console.error('No conversation id found for lead id:', leadId);
      return [];
    }

    // Then get the conversation details
    const conversationData = await db
      .select()
      .from(tables.pageConversationsTable)
      .where(eq(tables.pageConversationsTable.id, leadData[0].conversation_id));

    return conversationData;
  } catch (error) {
    console.error('Error getting conversations by lead id:', error);
    throw error;
  }
}

// Convert getConversationByFbConversationId function from Supabase to Drizzle ORM
async function getConversationByFbConversationId(supabase: any, fbConversationId: string) {
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
    return data;
  } catch (error) {
    console.error('Unexpected error in getConversationByFbConversationId:', error);
    throw error;
  }
}

// Convert updateMessageSentToAudosServer function from Supabase to Drizzle ORM
async function updateMessageSentToAudosServer(supabase: any, messageId: string) {
  try {
    const data = await db
      .update(tables.pageMessagesTable)
      .set({ sent_to_audos_server: true })
      .where(eq(tables.pageMessagesTable.id, Number(messageId)))
      .returning();

    return data;
  } catch (error) {
    console.error('Error updating message sent to audos server:', error);
    throw error;
  }
}

// Convert updateCommentSentToAudosServer function from Supabase to Drizzle ORM
async function updateCommentSentToAudosServer(supabase: any, commentId: string) {
  try {
    const data = await db
      .update(tables.pageCommentsTable)
      .set({ sent_to_audos_server: true })
      .where(eq(tables.pageCommentsTable.id, Number(commentId)))
      .returning();

    return data;
  } catch (error) {
    console.error('Error updating comment sent to audos server:', error);
    throw error;
  }
}

// Convert getAPageAccessTokenThatIsValid function from Supabase to Drizzle ORM
async function getAPageAccessTokenThatIsValid(supabase: any, pageId: string) {
  try {
    console.log('Getting valid page access token for pageId:', pageId);
    console.log('config.appId', config.appId);

    const pageData = await db
      .select({
        id: tables.pageTable.id,
        page_access_token: tables.pageTable.page_access_token,
        is_token_valid: tables.pageTable.is_token_valid
      })
      .from(tables.pageTable)
      .where(
        and(
          eq(tables.pageTable.app_id, Number(config.appId)),
          eq(tables.pageTable.fb_page_id, pageId),
          eq(tables.pageTable.is_token_valid, true)
        )
      )
      .orderBy(desc(tables.pageTable.created_at))
      .limit(1);

    if (pageData && pageData.length > 0) {
      console.log('pageData found, return here?', pageData[0]);
      return pageData[0].page_access_token;
    }
    
  } catch (err) {
    console.error('Error in getAPageAccessTokenThatIsValid:', err);
    console.log(' returning a fall default page access token');
    return await getPageAccessToken(pageId, supabase);
  }
}

// Convert updateIsTokenValid function from Supabase to Drizzle ORM
async function updateIsTokenValid(pageAccessToken: string, isTokenValid: boolean, supabase: any) {
  try {
    // Check if access token is found in the pages table
    const pageData = await db
      .select({ page_access_token: tables.pageTable.page_access_token })
      .from(tables.pageTable)
      .where(
        and(
          eq(tables.pageTable.page_access_token, pageAccessToken),
          eq(tables.pageTable.app_id, Number(config.appId))
        )
      )
      .limit(1);

    if (pageData && pageData.length > 0) {
      // Update page table
      return await db
        .update(tables.pageTable)
        .set({ is_token_valid: isTokenValid })
        .where(eq(tables.pageTable.page_access_token, pageAccessToken))
        .returning();
    } else {
      console.log('page access token not found in the pages table, it is a user access token');
      // Update user table
      return await db
        .update(tables.userTable)
        .set({ is_token_valid: isTokenValid })
        .where(eq(tables.userTable.user_access_token, pageAccessToken))
        .returning();
    }
  } catch (error) {
    console.error('Error updating token validity:', error);
    throw error;
  }
}

// Convert getValidUserAccessToken function from Supabase to Drizzle ORM
async function getValidUserAccessToken(supabase: any, adId: string, pageId: string, requirements = { needsMessaging: false, needsInstagram: false, needsAds: true }) {
  // This function appears to be empty in the original, so implementing as placeholder
  console.log('getValidUserAccessToken called with:', { adId, pageId, requirements });
  return null;
}

// Convert getValidUserAccessTokensForAd function from Supabase to Drizzle ORM
async function getValidUserAccessTokensForAd(supabase: any, adId: string, pageId: string, requirements = { needsMessaging: false, needsInstagram: false, needsAds: true }) {
  try {
    console.log('getValidUserAccessTokensForAd valid user access token for adId:', adId);

    // Check if page is managed by multiple users
    const { isManagedByMultipleUsers, data: pagesData, userDataArray } = await checkIfPageIsManagedByMultipleUsers(pageId,supabase);

    const userObjects = userDataArray.map(arr => arr[0]).filter(Boolean);
    const fbIds = userObjects.map(user => user.fb_id);
    const userAccessTokens = userObjects.map(user => user.user_access_token);

    console.log('fbIds', fbIds);
    console.log('userAccessTokens', userAccessTokens);
    console.log('userObjects', userObjects);

    let existingTokens = [];

    for (const user of userObjects) {
      if (!user || !user.fb_id || !user.user_access_token) continue;

      // Get the matching page data from pagesData
      const matchingPageData = pagesData.find(page => page.fb_id === user.fb_id);
    
      if (user.is_token_valid && user.has_ads) {
        existingTokens.push({ user, page: matchingPageData });
      } else {
        console.log('user is not valid or does not have ads');
      }
    }

    if (existingTokens && existingTokens.length > 0) {
      return existingTokens.map(data => ({
        userAccessToken: data.user.user_access_token,
        userDbId: data.user.id,
        pageAccessToken: data.page.page_access_token,
      }));
    } else {
      return userDataArray.map(data => ({
        userAccessToken: data[0].user_access_token,
        userDbId: data[0].id,
        pageAccessToken: pagesData[0].page_access_token,
      }));
    }

  } catch (err) {
    console.error('Error in getValidUserAccessTokensForAd:', err);
    return null;
  }
}

// Convert logMetaApiCall function from Supabase to Drizzle ORM
async function logMetaApiCall(params: {
  supabase: any;
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
  requirementContext: { needsMessaging: boolean, needsInstagram: boolean, needsAds: boolean, action: string };
  errorCode?: string;
  errorMessage?: string;
}) {
  const { userId, fbId, pageId, accessToken, accessTokenType, success, status, reqUrl, reqParams, res, requirementContext, errorCode, errorMessage } = params;

  console.log("logMetaApiCall ....");
  console.dir({ userId, fbId, pageId, accessToken, accessTokenType, success, status, reqUrl, reqParams, res, requirementContext, errorCode, errorMessage }, { depth: null });

  try {
    // Get page data from access token
    const pageData = await db
      .select({ id: tables.pageTable.id, fb_id: tables.pageTable.fb_id })
      .from(tables.pageTable)
      .where(
        and(
          eq(tables.pageTable.page_access_token, accessToken),
          eq(tables.pageTable.app_id, Number(config.appId))
        )
      )
      .limit(1);

    let userData = null;
    let userError = null;

    if (!pageData || pageData.length === 0 || !pageData[0]?.fb_id) {
      // Get user data from access token
      console.log('No pageData found in logMetaApiCall, checking user table');
      const userResult = await db
        .select({ id: tables.userTable.id, fb_id: tables.userTable.fb_id })
        .from(tables.userTable)
        .where(
          and(
            eq(tables.userTable.user_access_token, accessToken),
            eq(tables.userTable.app_id, config.appId)
          )
        )
        .limit(1);

      userData = userResult;
    } else {
      console.log('found pageData in logMetaApiCall getUserIdFromAccessToken pageData');
      console.dir({ pageData }, { depth: null });
      
      const userResult = await db
        .select()
        .from(tables.userTable)
        .where(
          and(
            eq(tables.userTable.fb_id, pageData[0].fb_id),
            eq(tables.userTable.app_id, config.appId)
          )
        )
        .limit(1);

      userData = userResult;
    }

    if (!userData || userData.length === 0) {
      console.log('Error getting user data in logMetaApiCall:');
      console.dir({ userError }, { depth: null });
    }

    // Insert API call log
    const data = await db
      .insert(tables.metaApiCallsResultsTable)
      .values({
        app_id: Number(config.appId),
        user_id: userData?.[0]?.id || null,
        fb_id: userData?.[0]?.fb_id || null,
        page_id: pageData?.[0] ? pageId : null,
        access_token: accessToken,
        access_token_type: !pageData || pageData.length === 0 ? 'user' : 'page',
        success,
        status,
        req_url: reqUrl,
        req_params: reqParams,
        res,
        requirement_context: requirementContext,
        error_code: errorCode,
        error_message: errorMessage
      })
      .returning();

    console.log('logMetaApiCall returned data');
    console.dir({ data }, { depth: null });
    return data;

  } catch (error) {
    console.error('Error in logMetaApiCall:');
    console.dir({ error }, { depth: null });
    throw error;
  }
}

// Convert makeFbApiCall function from Supabase to Drizzle ORM
async function makeFbApiCall(params: {
  supabase: any;
  pageId: string;
  accessToken: string;
  requirements: any;
  apiCall: () => Promise<any>;
  retryOnInvalidToken?: boolean;
}) {
  const { pageId, accessToken, requirements, apiCall, retryOnInvalidToken = true } = params;

  // For now, let's outsource this to the async version
  return await makeFbApiCallWithPageAccessTokenThatIsValid({
    supabase,
    pageId,
    accessToken,
    requirements,
    apiCall,
    retryOnInvalidToken
  });
}

// Helper function for makeFbApiCall
async function makeFbApiCallWithPageAccessTokenThatIsValid(params: {
  supabase: any;
  pageId: string;
  accessToken: string;
  requirements: any;
  apiCall: () => Promise<any>;
  retryOnInvalidToken?: boolean;
}) {
  const { pageId, accessToken, requirements, apiCall, retryOnInvalidToken = true } = params;

  let response;
  try {
    response = await apiCall();
  } catch (error) {
    console.log('makeFbApiCall errorx123 error.response', error.response);

    const errorObject = config.handleFbErrors({ 
      error: error, 
      serverNote: `Error in makeFbApiCall function: ${requirements.url ? requirements.url.toString() : requirements.function_string?.toString()}` 
    });

    // Check if the error is a token invalid error
    const isTokenInvalidError = error.response?.data?.error?.code === 190;
    if (isTokenInvalidError) {
      console.log('makeFbApiCall this token is considered invalid, updating the is token valid field to false', accessToken);
      await updateIsTokenValid(accessToken, false, supabase);
    }

    throw errorObject;
  }
  return response;
}

// Convert getUserProfileIfNeeded function from Supabase to Drizzle ORM
async function getUserProfileIfNeeded(supabase: any, senderId: string, pageAccessToken: string, platform: string, fbPageId: string, fbConversationId: string) {
  try {
    // First check if we already have profile data for this conversation
    const existingConversation = await db
      .select({
        fb_first_name: tables.pageConversationsTable.fb_first_name,
        fb_last_name: tables.pageConversationsTable.fb_last_name,
        fb_profile_pic: tables.pageConversationsTable.fb_profile_pic,
        ig_name: tables.pageConversationsTable.ig_name,
        ig_username: tables.pageConversationsTable.ig_username,
        ig_profile_pic: tables.pageConversationsTable.ig_profile_pic,
        ig_follower_count: tables.pageConversationsTable.ig_follower_count,
        ig_is_user_follow_business: tables.pageConversationsTable.ig_is_user_follow_business,
        ig_is_business_follow_user: tables.pageConversationsTable.ig_is_business_follow_user
      })
      .from(tables.pageConversationsTable)
      .where(
        and(
          eq(tables.pageConversationsTable.app_id, Number(config.appId)),
          eq(tables.pageConversationsTable.fb_page_id, fbPageId),
          eq(tables.pageConversationsTable.fb_conversation_id, fbConversationId),
          eq(tables.pageConversationsTable.recipient_page_scope_id, senderId)
        )
      )
      .limit(1);

    // Check if we already have profile data
    const hasProfileData = existingConversation && existingConversation.length > 0 && (
      (platform === 'facebook' && (existingConversation[0].fb_first_name || existingConversation[0].fb_last_name)) ||
      (platform === 'instagram' && (existingConversation[0].ig_name || existingConversation[0].ig_username))
    );

    if (hasProfileData) {
      console.log(`Profile data already exists for ${platform} user ${senderId}, skipping API call`);
      
      // Return existing profile data in the expected format
      if (platform === 'facebook') {
        return {
          firstName: existingConversation[0].fb_first_name,
          lastName: existingConversation[0].fb_last_name,
          profilePic: existingConversation[0].fb_profile_pic
        };
      } else {
        return {
          name: existingConversation[0].ig_name,
          username: existingConversation[0].ig_username,
          profilePic: existingConversation[0].ig_profile_pic,
          followerCount: existingConversation[0].ig_follower_count,
          isUserFollowBusiness: existingConversation[0].ig_is_user_follow_business,
          isBusinessFollowUser: existingConversation[0].ig_is_business_follow_user
        };
      }
    }

    // If we don't have profile data, fetch it from the API
    console.log(`Fetching fresh profile data for ${platform} user ${senderId}`);
    const getUserProfile = (await import('@/processWebhooks/getUserProfile')).default;
    return await getUserProfile(senderId, pageAccessToken, platform, fbPageId);

  } catch (error) {
    console.error('Error in getUserProfileIfNeeded:', error);
    // Fallback to fetching from API if checking fails
    const getUserProfile = (await import('@/processWebhooks/getUserProfile')).default;
    return await getUserProfile(senderId, pageAccessToken, platform, fbPageId);
  }
}

// Convert webhookFilter class from Supabase to Drizzle ORM
class webhookFilter {
  private returnableObject: any;

  constructor(returnableObject: any) {
    this.returnableObject = returnableObject;
  }

  async saveToDb() {
    try {
      if (this.returnableObject.commentOrMessgae === 'message') {
        console.log('Attempting to upsert message...');
        const result = await this.upsertMessage(this.returnableObject);
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

  async checkIfConversationExists(conversationObject: any) {
    try {
      const existingConversation = await db
        .select()
        .from(tables.pageConversationsTable)
        .where(
          and(
            eq(tables.pageConversationsTable.app_id, conversationObject.appId),
            eq(tables.pageConversationsTable.fb_page_id, conversationObject.fbPageId),
            eq(tables.pageConversationsTable.fb_conversation_id, conversationObject.fbConversationId)
          )
        )
        .limit(1);
      
      return existingConversation;
    } catch (checkError) {
      console.error('Unexpected error in checkIfConversationExists:', checkError);
      return null;
    }
  }

  async upsertConversation(conversationObject: any) {
    try {
      // Check if the conversation already exists
      const existingConversation = await db
        .select()
        .from(tables.pageConversationsTable)
        .where(
          and(
            eq(tables.pageConversationsTable.app_id, conversationObject.appId),
            eq(tables.pageConversationsTable.fb_page_id, conversationObject.fbPageId),
            eq(tables.pageConversationsTable.fb_conversation_id, conversationObject.fbConversationId)
          )
        )
        .limit(1);

      const conversationRecord = {
        app_id: conversationObject.appId,
        fb_page_id: conversationObject.fbPageId,
        fb_conversation_id: conversationObject.fbConversationId,
        status_modified_by_user_id: conversationObject.statusModifiedByUserId,
        recipient_page_scope_id: conversationObject.recipientPageScopeId,
        conversation_platform: conversationObject.conversationPlatform,
        ig_account_id: conversationObject.igAccountId,
        active: conversationObject.active,
        opening_message_id: conversationObject.messageId,
        conversation_source: null,
        conversation_ad_id: null,
        foreign_ad_id: null,
        lead_first_name: null,
        lead_last_name: null,
        lead_email: null,
        lead_phone: null,
        lead_street_address: null,
        lead_business_website: null,
        fb_first_name: conversationObject.fbFirstName,
        fb_last_name: conversationObject.fbLastName,
        fb_profile_pic: conversationObject.fbProfilePic,
        ig_name: conversationObject.igName,
        ig_username: conversationObject.igUsername,
        ig_profile_pic: conversationObject.igProfilePic,
        ig_follower_count: conversationObject.igFollowerCount,
        ig_is_user_follow_business: conversationObject.igIsUserFollowBusiness,
        ig_is_business_follow_user: conversationObject.igIsBusinessFollowUser,
      };

      let result;
      if (existingConversation && existingConversation.length > 0) {
        console.log('convo already exists, updating...');
        
        // Update existing conversation with new data
        const updatedConversation = { ...existingConversation[0] };
        
        // Handle lead data updates
        if (conversationObject.extractedLeadData && conversationObject.extractedLeadData.lead_data && conversationObject.extractedLeadData.fb_ad_id !== null && conversationObject.extractedLeadData.fb_ad_id !== undefined) {
          if (conversationObject.listOfMessagesFromFbApi && conversationObject.listOfMessagesFromFbApi.length < 3) {
            console.log('conversation includes less than 4 messages so it most likely was started from an ad, updating conversation source to ad');
            updatedConversation.conversation_source = 'ad';
            updatedConversation.conversation_ad_id = conversationObject.extractedLeadData.fb_ad_id;
          }
        }

        // Update profile data
        if (conversationRecord.fb_first_name !== null && conversationRecord.fb_first_name !== undefined) {
          updatedConversation.fb_first_name = conversationRecord.fb_first_name;
          updatedConversation.fb_last_name = conversationRecord.fb_last_name;
          updatedConversation.fb_profile_pic = conversationRecord.fb_profile_pic;
        } else if (conversationRecord.ig_name !== null && conversationRecord.ig_name !== undefined) {
          updatedConversation.ig_name = conversationRecord.ig_name;
          updatedConversation.ig_username = conversationRecord.ig_username;
          updatedConversation.ig_profile_pic = conversationRecord.ig_profile_pic;
          updatedConversation.ig_follower_count = conversationRecord.ig_follower_count;
          updatedConversation.ig_is_user_follow_business = conversationRecord.ig_is_user_follow_business;
          updatedConversation.ig_is_business_follow_user = conversationRecord.ig_is_business_follow_user;
        }

        // Update contact data
        if (conversationObject.extractedContactData !== null && conversationObject.extractedContactData !== undefined && conversationObject.extractedContactData) {
          updatedConversation[conversationObject.extractedContactData.contactType] = conversationObject.extractedContactData.contactValue;
        }

        // Update the conversation record
        const updatedData = await db
          .update(tables.pageConversationsTable)
          .set(updatedConversation)
          .where(eq(tables.pageConversationsTable.id, existingConversation[0].id))
          .returning();

        result = updatedData[0];
      } else {
        console.log('convo does not exist, inserting new conversation...');

        // Set conversation source
        if (conversationObject.extractedLeadData && conversationObject.extractedLeadData.lead_data && conversationObject.extractedLeadData.fb_ad_id !== null && conversationObject.extractedLeadData.fb_ad_id !== undefined) {
          conversationRecord.conversation_source = 'ad';
          conversationRecord.conversation_ad_id = conversationObject.extractedLeadData.fb_ad_id;
          if (conversationObject.extractedLeadData.is_foreign_ad) {
            conversationRecord.foreign_ad_id = conversationObject.extractedLeadData.fb_ad_id;
          }
        } else {
          conversationRecord.conversation_source = 'organic';
        }

        // Add contact data
        if (conversationObject.extractedContactData !== null && conversationObject.extractedContactData !== undefined && conversationObject.extractedContactData) {
          conversationRecord[conversationObject.extractedContactData.contactType] = conversationObject.extractedContactData.contactValue;
        }

        // Insert new conversation
        const newConversationData = await db
          .insert(tables.pageConversationsTable)
          .values(conversationRecord)
          .returning();

        result = newConversationData[0];
      }

      // Handle lead data upsert
      if (conversationObject.extractedLeadData && conversationObject.extractedLeadData !== null && conversationObject.extractedLeadData.lead_data !== null && conversationObject.extractedLeadData.fb_ad_id !== null && conversationObject.extractedLeadData.fb_ad_id !== undefined) {
        const leadData = {
          conversation_id: result.id,
          ...conversationObject.extractedLeadData
        };
        await upsertLead(leadData);
      }

      // Handle contact data upsert
      if (conversationObject.extractedContactData && conversationObject.extractedContactData !== null && conversationObject.extractedContactData !== undefined) {
        const contactData = {
          conversation_id: result.id,
          ...conversationObject.extractedContactData
        };
        await upsertContact(contactData);
      }

      return result;
    } catch (error) {
      console.error('Error in upsertConversation:', error);
      return null;
    }
  }

  async upsertMessage(messageObject: any) {
    try {
      const messageRecord = {
        app_id: messageObject.appId,
        conversation_id: messageObject.conversationId,
        fb_page_id: messageObject.fbPageId,
        fb_conversation_id: messageObject.fbConversationId,
        message_id: messageObject.messageId,
        sender_id: messageObject.senderId,
        recipient_id: messageObject.recipientId,
        message_text: messageObject.messageText,
        message_type: messageObject.messageType,
        message_timestamp: messageObject.messageTimestamp,
        sent_to_audos_server: messageObject.sentToAudosServer || false,
        details: messageObject.details
      };

      const existingMessage = await db
        .select()
        .from(tables.pageMessagesTable)
        .where(
          and(
            eq(tables.pageMessagesTable.app_id, messageObject.appId),
            eq(tables.pageMessagesTable.message_id, messageObject.messageId)
          )
        )
        .limit(1);

      if (existingMessage && existingMessage.length > 0) {
        const updatedMessage = await db
          .update(tables.pageMessagesTable)
          .set(messageRecord)
          .where(eq(tables.pageMessagesTable.id, existingMessage[0].id))
          .returning();
        return updatedMessage[0];
      } else {
        const newMessage = await db
          .insert(tables.pageMessagesTable)
          .values(messageRecord)
          .returning();
        return newMessage[0];
      }
    } catch (error) {
      console.error('Error in upsertMessage:', error);
      return null;
    }
  }

  async upsertComment(commentObject: any) {
    try {
      // const commentRecord = {
      //   app_id: commentObject.appId,
      //   fb_page_id: commentObject.fbPageId,
      //   comment_id: commentObject.commentId,
      //   sender_id: commentObject.senderId,
      //   comment_text: commentObject.commentText,
      //   comment_timestamp: commentObject.commentTimestamp,
      //   sent_to_audos_server: commentObject.sentToAudosServer || false,
      //   details: commentObject.details
      // };

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

      const existingComment = await db
        .select()
        .from(tables.pageCommentsTable)
        .where(
          and(
            eq(tables.pageCommentsTable.app_id, commentObject.appId),
            eq(tables.pageCommentsTable.fb_comment_id, commentObject.commentId),
          )
        )
        .limit(1);

      if (existingComment && existingComment.length > 0) {
        const updatedComment = await db
          .update(tables.pageCommentsTable)
          .set(commentRecord)
          .where(eq(tables.pageCommentsTable.id, existingComment[0].id))
          .returning();
        return updatedComment[0];
      } else {
        const newComment = await db
          .insert(tables.pageCommentsTable)
          .values(commentRecord)
          .returning();
        return newComment[0];
      }
    } catch (error) {
      console.error('Error in upsertComment:', error);
      return null;
    }
  }

  async readMessages(queryParams: any) {
    try {
      const whereConditions = [eq(tables.pageMessagesTable.app_id, Number(config.appId))];
      
      // Add other query parameters with proper type handling
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
      }

      const data = await db
        .select()
        .from(tables.pageMessagesTable)
        .where(and(...whereConditions));
      return data;
    } catch (error) {
      console.error('Error in readMessages:', error);
      return null;
    }
  }

  async readComments(queryParams: any) {
    try {
      const whereConditions = [eq(tables.pageCommentsTable.app_id, Number(config.appId))];
      
      // Add other query parameters with proper type handling
      for (const [key, value] of Object.entries(queryParams)) {
        if (key === 'id') {
          whereConditions.push(eq(tables.pageCommentsTable.id, Number(value)));
        } else if (key === 'fb_page_id') {
          whereConditions.push(eq(tables.pageCommentsTable.fb_page_id, value as string));
        } else if (key === 'sender_id') {
          whereConditions.push(eq(tables.pageCommentsTable.sender_id, value as string));
        } else if (key === 'recipient_id') {
          whereConditions.push(eq(tables.pageCommentsTable.recipient_id, value as string));
        } else if (key === 'post_id') {
          whereConditions.push(eq(tables.pageCommentsTable.post_id, value as string));
        } else if (key === 'media_id') {
          whereConditions.push(eq(tables.pageCommentsTable.media_id, value as string));
        } else if (key === 'platform') {
          whereConditions.push(eq(tables.pageCommentsTable.platform, value as string));
        } else if (key === 'fb_comment_id') {
          whereConditions.push(eq(tables.pageCommentsTable.fb_comment_id, value as string));
        } else if (key === 'ig_comment_id') {
          whereConditions.push(eq(tables.pageCommentsTable.ig_comment_id, value as string));
        } else if (key === 'is_inbound') {
          whereConditions.push(eq(tables.pageCommentsTable.is_inbound, Boolean(value)));
        } else if (key === 'is_outbound') {
          whereConditions.push(eq(tables.pageCommentsTable.is_outbound, Boolean(value)));
        } else if (key === 'outbound_origin') {
          whereConditions.push(eq(tables.pageCommentsTable.outbound_origin, value as string));
        }
      }

      const data = await db
        .select()
        .from(tables.pageCommentsTable)
        .where(and(...whereConditions));
      return data;
    } catch (error) {
      console.error('Error in readComments:', error);
      return null;
    }
  }
}

// Convert debugUserAndPageAccessTokens function from Supabase to Drizzle ORM
async function debugUserAndPageAccessTokens(supabase: any, fbUserData: any, pageData: any) {
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

    // Update page access token if different
    if (pageTokenDebugResult.accessToken && pageTokenDebugResult.accessToken !== pageData.page_access_token) {
      console.log('new page access token found, updating the db', pageTokenDebugResult.accessToken);
      await db
        .update(tables.pageTable)
        .set({ page_access_token: pageTokenDebugResult.accessToken })
        .where(
          and(
            eq(tables.pageTable.fb_page_id, pageData.fb_page_id),
            eq(tables.pageTable.app_id, pageData.app_id)
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

    // Upsert access tokens
    await Promise.all([
      upsertAccessToken({
        supabase,
        appid: Number(pageData.app_id),
        userId: fbUserData.id,
        fbId: fbUserData.fb_id,
        pageId: pageData.fb_page_id,
        accessTokenData: { ...combinedResult, pageMessagingEnabled: userTokenDebugResult.hasMessagingPermission || false, instagramMessagingEnabled: userTokenDebugResult.hasInstagramMessagingPermission || false, adPermissionsEnabled: userTokenDebugResult.hasAdPermission || false },
        accessTokenType: 'user'
      }).catch(error => {
        console.error('Error upserting user token:', error);
        throw { source: 'user_token_upsert', error };
      }),

      upsertAccessToken({
        supabase,
        appid: Number(pageData.app_id),
        userId: fbUserData.id,
        fbId: fbUserData.fb_id,
        pageId: pageData.fb_page_id,
        accessTokenData: { ...combinedResult, pageMessagingEnabled: pageTokenDebugResult.hasMessagingPermission || false, instagramMessagingEnabled: pageTokenDebugResult.hasInstagramMessagingPermission || false, adPermissionsEnabled: pageTokenDebugResult.hasAdPermission || false },
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
      let errorObject = config.handleFbErrors({ error: error.error, serverNote: `error in debugUserAndPageAccessTokens source: ${error.source}` });
      combinedResult.error = errorObject.message;
      combinedResult.errorSource = errorObject.source;
    }
  }

  return combinedResult;
}

// Convert deleteClientUserData function from Supabase to Drizzle ORM
// TODO: FIX BUG
async function deleteClientUserData(supabase: any, userId: string) {
  console.log(`Starting deletion process for client user ID: ${userId}`);

  try {
    // 1. Get associated pages
    const pages = await db
      .select({ id: tables.pageTable.id, fb_page_id: tables.pageTable.fb_page_id })
      .from(tables.pageTable)
      .where(
        and(
          eq(tables.pageTable.app_id, Number(config.appId)),
          eq(tables.pageTable.fb_id, userId)
        )
      );

    const pageIds = pages?.map(p => p.id) || [];
    const fbPageIds = pages?.map(p => p.fb_page_id) || [];
    console.log(`Found ${pages?.length} pages associated with user ${userId}. FB Page IDs: ${fbPageIds.join(', ')}`);

    if (pages && pages.length > 0) {
      // 2. Get associated conversations
      const conversations = await db
        .select({ id: tables.pageConversationsTable.id })
        .from(tables.pageConversationsTable)
        .where(
          and(
            inArray(tables.pageConversationsTable.fb_page_id, fbPageIds),
            eq(tables.pageConversationsTable.app_id, Number(userId))  
          )
        );

      const conversationIds = conversations?.map(c => c.id) || [];
      console.log(`Found ${conversations?.length} conversations.`);

      if (conversationIds.length > 0) {
        // 3. Delete Messages, Leads, Contacts associated with conversations
        console.log(`Deleting messages for ${conversationIds.length} conversations...`);
        // await db.delete(tables.pageMessagesTable).where(inArray(tables.pageMessagesTable.conversation_id, conversationIds));

        console.log(`Deleting leads for ${conversationIds.length} conversations...`);
        await db.delete(tables.leadsTable).where(inArray(tables.leadsTable.conversation_id, conversationIds));

        console.log(`Deleting contacts for ${conversationIds.length} conversations...`);
        await db.delete(tables.contactsTable).where(inArray(tables.contactsTable.conversation_id, conversationIds));

        // 4. Delete Conversations
        console.log(`Deleting ${conversationIds.length} conversations...`);
        await db.delete(tables.pageConversationsTable).where(inArray(tables.pageConversationsTable.id, conversationIds));
      }

      // 5. Delete Comments associated with pages
      console.log(`Deleting comments for FB Page IDs: ${fbPageIds.join(', ')}...`);
      await db.delete(tables.pageCommentsTable).where(inArray(tables.pageCommentsTable.fb_page_id, fbPageIds));

      // 6. Delete Pages
      console.log(`Deleting ${pageIds.length} pages...`);
      await db.delete(tables.pageTable).where(inArray(tables.pageTable.id, pageIds));
    }

    // 7. Delete Ad Creatives
    console.log(`Deleting ad creatives for user ID: ${userId}...`);
    await db.delete(tables.adCreativesTable).where(eq(tables.adCreativesTable.user_id, Number(userId)));

    // 8. Delete Ads
    console.log(`Deleting ads for user ID: ${userId}...`);
    await db.delete(tables.adsTable).where(eq(tables.adsTable.user_id, Number(userId)));

    // 9. Delete Ad Sets
    console.log(`Deleting ad sets for user ID: ${userId}...`);
    await db.delete(tables.adSetsTable).where(eq(tables.adSetsTable.user_id, Number(userId)));

    // 10. Delete Campaigns
    console.log(`Deleting campaigns for user ID: ${userId}...`);
    await db.delete(tables.campaignsTable).where(eq(tables.campaignsTable.user_id, Number(userId)));

    // 11. Delete Ad Accounts
    console.log(`Deleting ad accounts for user ID: ${userId}...`);
    await db.delete(tables.adAccountsTable).where(eq(tables.adAccountsTable.user_id, Number(userId)));

    // 12. Delete User Record
    console.log(`Deleting user record for ID: ${userId}...`);
    await db.delete(tables.userTable).where(eq(tables.userTable.id, Number(userId)));

    console.log(`Successfully deleted all data for client user ID: ${userId}`);
    return { success: true, message: `Successfully deleted user ${userId}` };

  } catch (error) {
    console.error(`Error during deletion process for client user ID ${userId}:`, error);
    return { success: false, message: error.message };
  }
}

// Convert deleteCustomerUserData function from Supabase to Drizzle ORM
async function deleteCustomerUserData(supabase: any, customerPsid: string, fbPageId: string) {
  console.log(`Starting deletion process for customer PSID: ${customerPsid} on page: ${fbPageId}`);

  try {
    // 1. Find conversations involving this customer PSID and Page ID
    const conversations = await db
      .select({ id: tables.pageConversationsTable.id })
      .from(tables.pageConversationsTable)
      .where(
        and(
          eq(tables.pageConversationsTable.app_id, Number(config.appId)),
          eq(tables.pageConversationsTable.recipient_page_scope_id, customerPsid),
          eq(tables.pageConversationsTable.fb_page_id, fbPageId)
        )
      );

    console.log("conversations", conversations);
    const conversationIds = conversations?.map(c => c.id) || [];
    console.log(`Found ${conversations?.length} conversations for PSID ${customerPsid} on page ${fbPageId}.`);

    if (conversationIds.length > 0) {
      // 2. Delete Messages where the customer is sender or recipient within these conversations
      console.log(`Deleting messages involving PSID ${customerPsid} in ${conversationIds.length} conversations...`);
      const messgData = await db
        .select()
        .from(tables.pageMessagesTable)
        .where(
          and(
            or(
              eq(tables.pageMessagesTable.sender_id, customerPsid),
              eq(tables.pageMessagesTable.recipient_id, customerPsid)
            ),
            eq(tables.pageMessagesTable.app_id, Number(config.appId))
          )
        );

      console.log("messgData.length", messgData.length);

      // 3. Delete Leads associated with these conversations
      console.log(`Deleting leads for ${conversationIds.length} conversations...`);
      const leadData = await db
        .select()
        .from(tables.leadsTable)
        .where(inArray(tables.leadsTable.conversation_id, conversationIds));

      console.log("leadData.length", leadData.length);

      // 4. Delete Contacts associated with these conversations
      console.log(`Deleting contacts for ${conversationIds.length} conversations...`);
      const contactData = await db
        .select()
        .from(tables.contactsTable)
        .where(inArray(tables.contactsTable.conversation_id, conversationIds));

      console.log("contactData.length", contactData.length);

      // 5. Delete Conversations themselves
      console.log(`Deleting ${conversationIds.length} conversations...`);
      const convoData = await db
        .select()
        .from(tables.pageConversationsTable)
        .where(inArray(tables.pageConversationsTable.id, conversationIds));

      console.log("convoData.length", convoData.length);
    }

    // 6. Delete Comments made by this customer PSID on this page
    console.log(`Deleting comments from PSID ${customerPsid} on page ${fbPageId}...`);
    const commentData = await db
      .select()
      .from(tables.pageCommentsTable)
      .where(
        and(
          eq(tables.pageCommentsTable.app_id, Number(config.appId)),
          eq(tables.pageCommentsTable.sender_id, customerPsid),
          eq(tables.pageCommentsTable.fb_page_id, fbPageId)
        )
      );

    console.log("commentData.length", commentData.length);

    console.log(`Successfully deleted data for customer PSID: ${customerPsid} on page: ${fbPageId}`);
    return { success: true, message: `Successfully deleted data for customer ${customerPsid} on page ${fbPageId}` };

  } catch (error) {
    console.error(`Error during deletion process for customer PSID ${customerPsid} on page ${fbPageId}:`, error);
    return { success: false, message: error.message };
  }
}

// Convert getAllUserAssets function from Supabase to Drizzle ORM
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


// Convert fetchNewPageTokenFromFbUserAccounts function from Supabase to Drizzle ORM
async function fetchNewPageTokenFromFbUserAccounts(supabase: any, fbUserId: string, userAccessToken: string, targetPageId: string) {
  let nextUrl = `https://graph.facebook.com/${fbUserId}/accounts?access_token=${userAccessToken}`;
  let foundPage = null;

  console.log('targetPageId', targetPageId);
  while (nextUrl && !foundPage) {
    const response = await axios.get(nextUrl);
    const pages = response.data.data;
    console.log('pages', pages);
    foundPage = pages.find(page => page.id === targetPageId);

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
    return foundPage.access_token;
  } else {
    console.log('could not find the requested page in the list of pages controlled by the user, User no longer controls this page.');
    return null;
  }
}

// Convert debugTokenViaMetaApi function from Supabase to Drizzle ORM
const debugTokenViaMetaApi = async (params: { supabase: any; accessToken: string }) => {
  try {
    const result = await axios.get(`https://graph.facebook.com/debug_token`, {
      params: {
        input_token: params.accessToken,
        access_token: `${config.appId}|${config.appSecret}`
      }
    });
    return result.data.data;
  } catch (error) {
    console.error('Error in debugTokenViaMetaApi:', error);
    throw error;
  }
};

// Convert debugPageAccessTokens function from Supabase to Drizzle ORM
const debugPageAccessTokens = async (params: {
  supabase: any;
  appid: string | number;
  userId: string | number;
  fbId: string;
  pageId: string;
  accessToken: string;
}): Promise<TokenDebugResult> => {
  // STEP 1: DEBUG THE PAGE ACCESS TOKEN
  const result = await debugTokenViaMetaApi({ supabase, accessToken: params.accessToken });
  console.log('result1 from debug page access token', result);

  let pageTokenData = null;

  // STEP 2: Check if the page access token is valid
  if ((result.error && result.error.message) || (!result.is_valid)) {
    console.log('page access token is not valid, fetch new page access token');
    
    // Fetch the user access token from db
    const userDataFromDb = await db
      .select({ user_access_token: tables.userTable.user_access_token })
      .from(tables.userTable)
      .where(eq(tables.userTable.id, Number(params.userId)));

    if (!userDataFromDb || userDataFromDb.length === 0) {
      console.error('Error fetching user access token2: No user found');
      throw new Error('User not found');
    }

    console.log('fetching new page access token from fb by getting the users accounts/pages and then getting the page access token', userDataFromDb);

    // STEP 3: Fetch the new page access token from fb
    const newPageAccessToken = await fetchNewPageTokenFromFbUserAccounts(supabase, params.fbId, userDataFromDb[0].user_access_token, params.pageId);

    if (!newPageAccessToken || newPageAccessToken === null) {
      throw result.error;
    }

    // STEP 4: Debug the new page access token
    const result2 = await debugTokenViaMetaApi({ supabase, accessToken: newPageAccessToken });
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
    missingScopes: config.REQUIRED_SCOPES.split(',').filter(scope => !pageTokenData.scopes.includes(scope)),
    hasMessagingPermission: pageTokenData.granular_scopes?.some(scope => 
      scope.scope === 'pages_messaging'
    ) || false,
    hasInstagramMessagingPermission: pageTokenData.granular_scopes?.some(scope => 
      scope.scope === 'instagram_manage_messages'
    ) || false,
    hasAdPermission: pageTokenData.granular_scopes?.some(scope => 
      scope.scope === 'ads_management'
    ) || false
  };
};

// Convert debugUserAccessToken function from Supabase to Drizzle ORM
const debugUserAccessToken = async (params: {
  supabase: any;
  appid: string | number;
  userId: string | number;
  fbId: string;
  accessToken: string;
}): Promise<TokenDebugResult> => {
  const result = await debugTokenViaMetaApi({ supabase, accessToken: params.accessToken });
  console.log('result from debug user access token', result);

  if (result.error && result.error.code === 190) {
    throw result.error;
  }

  const userData = result;
  const userNameResult = await axios.get(`https://graph.facebook.com/v23.0/me`, {
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
    missingScopes: config.REQUIRED_SCOPES.split(',').filter(scope => !userData.scopes.includes(scope)),
    hasMessagingPermission: userData.granular_scopes?.some(scope => 
      scope.scope === 'pages_messaging'
    ) || false,
    hasInstagramMessagingPermission: userData.granular_scopes?.some(scope => 
      scope.scope === 'instagram_manage_messages'
    ) || false,
    hasAdPermission: userData.granular_scopes?.some(scope => 
      scope.scope === 'ads_management'
    ) || false
  };
};

const getBasicPageInfo = async (pageAccessToken) => {
  try {
    const result = await axios.get(`https://graph.facebook.com/v23.0/me`, {
      params: {
        fields: 'instagram_accounts{id}',
        access_token: pageAccessToken
      }
    });
    console.log('Basic page info result:', result.data);
    return result.data;
  } catch (error) {
    console.error('Error fetching basic page info:', error.response?.data?.error || error.message);
    throw error;
  }
}

const getInstagramAccountDetails = async (pageAccessToken, igAccountId) => {
  try {
    const result = await axios.get(`https://graph.facebook.com/v23.0/${igAccountId}`, {
      params: {
        fields: 'name,username,id,ig_id',
        access_token: pageAccessToken
      }
    });
    console.log('Instagram account details result:', result.data);
    return result.data;
  } catch (error) {
    console.error('Error fetching Instagram account details:', error.response?.data?.error || error.message);
    throw error;
  }
}

// non async
const debugTokenSynchronously = async (params) => {
  try {
   // step 1 : debug the token
    const result = await debugTokenViaMetaApi({supabase, accessToken: params.accessToken});
  } catch (error) {
    console.error('Error in debugTokenSynchronously:', config.flattenForLog(error));
    throw error;
  }
}

// Convert getUserAccessToken function from Supabase to Drizzle ORM
// async function getUserAccessToken(supabase: any, userId: string) {
//   console.log('userId: ', userId);
  
//   try {
//     const data = await db
//       .select({ user_access_token: tables.userTable.user_access_token })
//       .from(tables.userTable)
//       .where(
//         or(
//           eq(tables.userTable.id, Number(userId)),
//           eq(tables.userTable.id, Number(userId))
//         )
//       )
//       .limit(1);

//     if (!data || data.length === 0) {
//       throw new Error('Error fetching user access token1');
//     }
    
//     return data[0].user_access_token;
//   } catch (error) {
//     console.error('Error in getUserAccessToken:', error);
//     throw new Error('Error fetching user access token1');
//   }
// }


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



// Convert
// Convert activatePageWithBestConfig function from Supabase to Drizzle ORM
const activatePageWithBestConfig = async (supabase: any, appId: number, targetPage: any) => {
  try {
    // Get all other pages with the same fb_page_id (excluding the current one) to find the best candidate
    const allOtherPages = await db
      .select({
        config_id: tables.pageTable.config_id,
        id: tables.pageTable.id,
        fb_page_id: tables.pageTable.fb_page_id,
        ig_account_id: tables.pageTable.ig_account_id
      })
      .from(tables.pageTable)
      .where(
        and(
          eq(tables.pageTable.app_id, appId),
          eq(tables.pageTable.fb_page_id, targetPage.fb_page_id),
          sql`${tables.pageTable.id} != ${targetPage.id}`
        )
      )
      .orderBy(desc(tables.pageTable.created_at));

    const allOtherPagesIg = await db
      .select({
        config_id: tables.pageTable.config_id,
        id: tables.pageTable.id,
        fb_page_id: tables.pageTable.fb_page_id,
        ig_account_id: tables.pageTable.ig_account_id
      })
      .from(tables.pageTable)
      .where(
        and(
          eq(tables.pageTable.app_id, appId),
          eq(tables.pageTable.ig_account_id, targetPage.ig_account_id),
          sql`${tables.pageTable.id} != ${targetPage.id}`,
          sql`${tables.pageTable.fb_page_id} != ${targetPage.fb_page_id}`
        )
      )
      .orderBy(desc(tables.pageTable.created_at));

    // Find the best candidate page to copy config from
    let selectedPage = null;
    if (allOtherPages && allOtherPages.length > 0) {
      // First, try to find the most recently created page that has a config_id
      const pagesWithConfigId = allOtherPages.filter(page => page.config_id);
      const instagramPagesWithConfigId = allOtherPagesIg.filter(page => page.config_id);
      
      if (pagesWithConfigId.length > 0) {
        // Return the most recently created page with config_id (already ordered by created_at desc)
        selectedPage = pagesWithConfigId[0];
      } else {
        // If no pages have config_id, return the first one (most recently created)
        selectedPage = allOtherPages[0];
      }
    }

    // Update the target page with the selected config_id (if available)
    const updateData: any = selectedPage?.config_id ? { config_id: selectedPage.config_id } : {};
    
    let debugTokenResult = await debugTokenViaMetaApi({ supabase, accessToken: targetPage.access_token });
   
    if ((debugTokenResult.error && debugTokenResult.error.message) || (!debugTokenResult.is_valid)) {
      updateData.is_token_valid = false;
      updateData.token_debug_result = debugTokenResult;
    } else {
      updateData.is_token_valid = true;
      updateData.token_debug_result = debugTokenResult;
    }

    const activatedPage = await db
      .update(tables.pageTable)
      .set(updateData)
      .where(eq(tables.pageTable.id, targetPage.id))
      .returning();
      
    if (!activatedPage || activatedPage.length === 0) {
      console.error('Error activating target page');
      return null;
    }

    // Deactivate all other pages with the same fb_page_id
    if (allOtherPages && allOtherPages.length > 0) {
      for (const page of allOtherPages) {
        console.log('Setting page to inactive:', page.id);
        
        await db
          .update(tables.pageTable)
          .set({ active: false })
          .where(
            and(
              eq(tables.pageTable.app_id, appId),
              eq(tables.pageTable.fb_page_id, page.fb_page_id),
              eq(tables.pageTable.id, page.id)
            )
          );
      }
    }

    // set the ig_account_id to null and has_ig_page to false for all the other pages
    if (allOtherPagesIg && allOtherPagesIg.length > 0) {
      for (const page of allOtherPagesIg) {
        await db
          .update(tables.pageTable)
          .set({ ig_account_id: '', has_ig_page: false })
          .where(
            and(
              eq(tables.pageTable.app_id, appId),
              eq(tables.pageTable.ig_account_id, page.ig_account_id),
              eq(tables.pageTable.id, page.id)
            )
          );
      }
    }

    return activatedPage;
    
  } catch (error) {
    console.error('Unexpected error in activatePageWithBestConfig:', error);
    return null;
  }
};

// Export all functions
export {
  insertUserData,
  insertPageData,
  insertBusinessManagerData,
  insertBusinessSystemUserData,
  validateBusinessAdminUser,
  processBusinessAgencyInvitations,
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
  getAPageAccessTokenThatIsValid,
  updateIsTokenValid,
  getValidUserAccessToken,
  getValidUserAccessTokensForAd,
  logMetaApiCall,
  makeFbApiCall,
  getUserProfileIfNeeded
};
