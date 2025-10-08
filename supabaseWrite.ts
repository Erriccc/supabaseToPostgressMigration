// import * as Sentry from '@sentry/node';
// import * as Sentry from "@sentry/nextjs";
// import '@/instrument.mjs';
import config from '@/config'; 

const app = config.appContext;
const appDbKeys = config.SUPABASE_Resource_Names;
import axios from 'axios';

import { supabase } from '@/lib/supabase_server';
import assert from 'assert';

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
 
// we are planning on migrating from legacy app to a new app, 
// so when fetching and updating user data we need to check for the legacy app id and app env as well as the new app id and app env
// especially during re-auth we need to make sure no new users and pages are created
const insertUserData = async (supabase, userId,email, userPageId, access_token, appId) => {
  try {
    console.log('Attempting to insert user data:', { appId, app, userId, access_token });

    if (userId === config.uiModeSignal) {
      userId = `${app}${userPageId}`
    }
    console.log('after change userId: ', userId) 
    console.log('appId: ', appId)
    console.log('userPageId: ', userPageId)
    console.log('app: ', app) 
    const { data: existingUser, error: fetchError } = await supabase
      .from(appDbKeys.userTable)
      .select('id::text,email,fb_id,user_access_token,app_env,app_id')
      .or(`and(app_id.eq.${appId},fb_id.eq.${userPageId},app_env.eq.${app}),and(app_id.eq.${config.legacyAppId},fb_id.eq.${userPageId},app_env.eq.${config.legacyAppEnv}), and(app_id.eq.${config.legacyAppId},email.eq.${email},app_env.eq.${config.legacyAppEnv})`) 
      .single();

      console.log('existingUser: ', existingUser)

    if (fetchError) {
      if (fetchError.code !== 'PGRST116') {
        console.error('fetchError Error fetching user data:', fetchError);
        console.log('fetchError.code: ', fetchError.code)
        return null; // Indicate failure without crashing
      }
    } else if (existingUser && existingUser !== null) {
      console.log('Found existing user. Attempting to update access token...');
      console.log('existingUser: ', existingUser)

      // If user exists in legacy app, migrate them to new app
      const migrateToNewAppData = {
        fb_id: userPageId,
        user_access_token: access_token,
        email: email,
        app_id: appId,
        app_env: app
      };
      const { data: updatedUserData, error: updateError } = await supabase
        .from(appDbKeys.userTable)
        .update(migrateToNewAppData)
        .eq('id', existingUser.id)
        .select('id::text,email,fb_id,user_access_token,app_env,app_id');

      if (updateError) {
        console.error('Error updating user data:', updateError);
        return null;
      }

      console.log('User data updated successfully:', updatedUserData);

      const { data: newUserData, error: newUserDataError }  = await supabase
      .from(appDbKeys.userTable)
      .select('id::text,email,fb_id,user_access_token,app_env,app_id')
      .or(`and(app_id.eq.${appId},fb_id.eq.${userPageId},app_env.eq.${app}),and(app_id.eq.${config.legacyAppId},fb_id.eq.${userPageId},app_env.eq.${config.legacyAppEnv}), and(app_id.eq.${config.legacyAppId},email.eq.${email},app_env.eq.${config.legacyAppEnv})`)

      .single();

      console.log('1XXXXXXXXXXXXXXXXXXXXX: ', newUserData)
      return newUserData;
    } 


    let newUserDataBeforeInsert = null;


      console.log('no existing user found in db, while userId is present, so we will insert the new user')


      const { data: findUserByDbId, error: findUserByDbIdError }  = await supabase
      .from(appDbKeys.userTable)
      .select('id::text,email,fb_id,user_access_token,app_env,app_id')
      .or(`and(app_id.eq.${appId},id.eq.${userId},app_env.eq.${app}),and(app_id.eq.${config.legacyAppId},id.eq.${userId},app_env.eq.${config.legacyAppEnv})`)
      .single(); 

      if (findUserByDbIdError) {
        console.error('Error finding user by db id:', findUserByDbIdError);
        // return null;
        if (findUserByDbIdError.code !== 'PGRST116' && findUserByDbIdError.code !== 'PGRST117') {
          console.error('findUserByDbIdError Error fetching user data:', findUserByDbIdError);
          return null; // Indicate failure without crashing
        }
      }


      console.log('findUserByDbId: ', findUserByDbId)
      if (findUserByDbId && findUserByDbId.fb_id === userPageId) {
        console.log('user found by db id, so we will update the existing user')
          console.log('user found by db id, and user fb id is the same as the user page id, so we will update the existing user')
          newUserDataBeforeInsert = [{
            app_id: appId,
            id: userId,
            email: email,
            fb_id: userPageId,
            user_access_token: access_token,
            app_env: app,
          }];
          console.log('newUserDataBeforeInsert: ', newUserDataBeforeInsert)
      }else if (!findUserByDbId && userId){
        console.log('user not found by db id, but id was provided so we will insert the new user for the already existing audos user')
        newUserDataBeforeInsert = [{
          id: userId,
          app_id: appId,
          email: email,
          fb_id: userPageId,
          user_access_token: access_token,
          app_env: app,
        }]
      }else{
        console.log('user not found by db id, completely new user')
        newUserDataBeforeInsert = [{
          app_id: appId,
          email: email,
          fb_id: userPageId,
          user_access_token: access_token,
          app_env: app,
        }]
      }

    // } 


    // No existing user, attempt to insert new record
    console.log('No existing user found. Attempting to insert new user...');
    console.log('newUserDataBeforeInsert: ', newUserDataBeforeInsert)
    const { data: newUserData, error: insertError } = await supabase
      .from(appDbKeys.userTable)
      .insert(newUserDataBeforeInsert, {
        onConflict: 'id',
        ignoreDuplicates: false
      })
      .select('id::text,email,fb_id,user_access_token,app_env,app_id');

    if (insertError) {
      console.error('Error inserting user data:', insertError);
      return null;
    }

    console.log('User data inserted successfully:', newUserData);
    return newUserData;

  } catch (error) {
    console.error('Unexpected error in insertUserData:', error);
    return null; // Prevent crashing, signal error with null
  }
};


const insertBusinessManagerData = async (supabase, userId, appId, businessesData) => {
  try {
    console.log('Attempting to insert/update business manager data...');
    const businessData = [];

    if (!businessesData || !businessesData.data || businessesData.data.length === 0) {
      console.log('No businesses data to process');
      return businessData;
    }

    for (const business of businessesData.data) {
      console.log('Processing business:', business);
      
      const { data: existingBusiness, error: fetchError } = await supabase
        .from(appDbKeys.businessManagersTable)
        .select('*')
        .or(`and(app_id.eq.${appId},fb_business_id.eq.${business.id}, fb_id.eq.${userId}),and(app_id.eq.${config.legacyAppId},fb_business_id.eq.${business.id}, fb_id.eq.${userId})`)

      if (fetchError) {
        console.error('Error fetching business data:', fetchError);
        continue;
      }

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

        const { data: updatedBusinessData, error: updateError } = await supabase
          .from(appDbKeys.businessManagersTable)
          .update(updateRecord)
          .eq('id', existingRecord.id)
          .select();

        if (updateError) {
          console.error('Error updating business data:', updateError);
          continue;
        }

        result = updatedBusinessData;
        console.log('Business data updated successfully:', updatedBusinessData);
      } else {
        console.log('No existing business record found. Attempting to insert...');
        const { data: newBusinessData, error: insertError } = await supabase
          .from(appDbKeys.businessManagersTable)
          .insert([businessRecord])
          .select();

        if (insertError) {
          console.error('Error inserting business data:', insertError);
          continue; 
        }

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

const insertBusinessSystemUserData = async (supabase, appId, systemUserData, businessData, businessAdminEmail) => {
  try {
    console.log('Attempting to insert/update business system user data...');

    const { data: existingSystemUser, error: fetchError } = await supabase
      .from('hosted_app_config_app_business_system_users')
      .select('*')
      .eq('app_id', appId)
      .eq('fb_system_user_id', systemUserData.id);

    if (fetchError) {
      console.error('Error fetching existing system user:', fetchError);
      throw fetchError;
    }

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
      const { data: updatedData, error: updateError } = await supabase
        .from('hosted_app_config_app_business_system_users')
        .update(systemUserRecord)
        .eq('id', existingSystemUser[0].id)
        .select();

      if (updateError) {
        console.error('Error updating system user data:', updateError);
        throw updateError;
      }
      result = updatedData;
    } else {
      console.log('Inserting new system user record...');
      const { data: newData, error: insertError } = await supabase
        .from('hosted_app_config_app_business_system_users')
        .insert([systemUserRecord])
        .select();

      if (insertError) {
        console.error('Error inserting system user data:', insertError);
        throw insertError;
      }
      result = newData;
    }

    console.log('Business system user data processed successfully:', result);
    return result && result.length > 0 ? result[0] : null;
  } catch (error) {
    console.error('Unexpected error in insertBusinessSystemUserData:', error);
    throw error;
  }
};

const validateBusinessAdminUser = async (businessUsersData, businessAdminEmail) => {
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

const inviteBusinessAsAgency = async (pageId, businessId, accessToken, permittedTasks) => {
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
 
const processBusinessAgencyInvitations = async (assignedPages, businessIds, systemUserAccessToken, existingUserFbId) => {
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
 // lets use the fb_id and page id of the existing user to find the already exisiting page data
            // in our data base, the page data generated by the usr has more precedence than the page data generated by the system user
      const { data: existingPageData, error: pageFetchError } = await supabase
        .from(appDbKeys.pageTable)
        .select('*')
        .eq('fb_id', existingUserFbId)
        .eq('fb_page_id', page.id)
        .eq('app_id', config.appId)
        .order('created_at', { ascending: false })
        .limit(1);
        
        let accessToken = null;
      if (existingPageData && existingPageData.length > 0) {
        console.log('Existing page data found:', existingPageData);
        accessToken = existingPageData[0].page_access_token;
      }else{
        console.log('No existing page data found, using page access token:', page.access_token);
        accessToken = page.access_token || systemUserAccessToken;
      }
      // Use page access token if available, otherwise use system user token

      // Invite each business as an agency for this page
      for (const businessId of businessIdArray) {
        const result = await inviteBusinessAsAgency(
          page.id,
          businessId,
          accessToken,
          // permittedTasks
          permittedTasksForNow
        );
// if result was sucssesful, lets add allPermittedTasksForBusinessAgency to the angency

        console.log('result: ', result)
        if (result.success) {
          try{
            console.log('Inviting business as agency with all permitted tasks access token:', accessToken);
          const newResults = await inviteBusinessAsAgency(
            page.id,
            businessId,
            accessToken,
            // permittedTasks
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
            console.error('Error adding allPermittedTasksForBusinessAgency to the angency:', error);
            return []
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




const insertPageData = async (supabase, userId, appId, fbWebhookData) => {
  try {
    console.log('Attempting to insert/update page data...');
    const pageData = [];

    const { data: allPagesByUserId, error: fetchError1 } = await supabase
        .from(appDbKeys.pageTable)
        .select('*')
        
        .or(`and(app_id.eq.${appId}, fb_id.eq.${userId}),and(app_id.eq.${config.legacyAppId}, fb_id.eq.${userId})`)

      if (fetchError1) {
        console.error('Error fetching page data:', fetchError1);
        // continue; // Skip to the next page without crashing
      }


      // 2. Build sets of fb_page_ids
    const existingPageIds = new Set((allPagesByUserId || []).map(p => p.fb_page_id));
    const webhookPageIds = new Set((fbWebhookData.pages || []).map(p => p.id));

    // 3. Find missing pages (in DB but not in webhook)
    const missingPages = (allPagesByUserId || []).filter(p => !webhookPageIds.has(p.fb_page_id));
    // 4. Find new pages (in webhook but not in DB)
    const newPages = (fbWebhookData.pages || []).filter(p => !existingPageIds.has(p.id));

    // 5. Log or handle as needed
    if (missingPages.length > 0) {
      console.log('Pages missing from webhook data (likely disconnected):', missingPages.map(p => p.fb_page_id));
      console.log('Pages missing from webhook data (likely disconnected):', missingPages.map(p => ({fb_page_id: p.fb_page_id, name: p.name})));


      let missing_pages_data = {
        missing_pages: missingPages,
        new_pages: newPages
      }


      const { data: updatedUserData, error: updateError } = await supabase
      .from(appDbKeys.userTable)
      .update(missing_pages_data)
      .eq('id', userId)
      .select('id::text,email,fb_id,user_access_token,app_env,app_id');

    if (updateError) {
      console.error('Error updating user data:', updateError);
      return null;
    }

    console.log('User data updated successfully:', updatedUserData);
      
    }else{
      console.log('no missing pages')
    }
    if (newPages.length > 0) {
      console.log('New pages not in DB (likely just authorized):', newPages.map(p => p.id));
    }else{
      console.log('no new pages')
    }


    for (const page of fbWebhookData.pages) {
      console.log('Processing page:', page);
      
      const { data: existingPages, error: fetchError } = await supabase
        .from(appDbKeys.pageTable)
        .select('*')
        
        .or(`and(app_id.eq.${appId},fb_page_id.eq.${page.id}, fb_id.eq.${userId}),and(app_id.eq.${config.legacyAppId},fb_page_id.eq.${page.id}, fb_id.eq.${userId})`)

      if (fetchError) {
        console.error('Error fetching page data:', fetchError);
        continue; // Skip to the next page without crashing
      }

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
        console.log('existingPages: ', existingPages)
        const existingPage = existingPages[0]; 
        console.log('existingPage: ', existingPage)

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

        const { data: updatedPageData, error: updateError } = await supabase
          .from(appDbKeys.pageTable)
          // Note this could be a bug if for some reason the fb page id changes with a new app id
          // .update({...pageRecord,fb_id:existingPage.fb_id})// we are adding the fb_id to make sure we are updating the correct record
          // .update(pageRecord)// we are adding the fb_id to make sure we are updating the correct record
          .update(updateRecord)
          .eq('id', existingPage.id)
          .select();

        if (updateError) {
          console.error('Error updating page data:', updateError);
          continue;
        }

        result = updatedPageData;
        console.log('Page data updated successfully:', updatedPageData);
      } else {
        console.log('No existing page record found. Attempting to insert...');
        const { data: newPageData, error: insertError } = await supabase
          .from(appDbKeys.pageTable)
          .insert([pageRecord])
          .select();

        if (insertError) {
          console.error('Error inserting page data:', insertError);
          continue; 
        }

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

const getIgId = async (identifier, supabase) => { 
  try {
    console.log('Attempting to retrieve fbPageId for identifier:', identifier);

    const { data, error } = await supabase
      .from(appDbKeys.pageTable)
      .select('ig_account_id,has_ig_page,fb_page_id')
      .eq('app_id', config.appId)
      .eq('active', true)
      .or(`ig_account_id.eq.${identifier},fb_page_id.eq.${identifier}`);

    if (error) {
      console.error('Error retrieving fbPageId:', error);
      return null;
    }

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



const getFbPageId = async (identifier, supabase) => {  
  try {
    console.log('Attempting to retrieve fbPageId for identifier:', identifier);

    const { data, error } = await supabase
      .from(appDbKeys.pageTable)
      .select('fb_page_id')
      .eq('app_id', config.appId)
      .eq('active', true)
      .or(`ig_account_id.eq.${identifier},fb_page_id.eq.${identifier}`);

    if (error) {
      console.error('Error retrieving fbPageId:', error);
      return null;
    }

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

// this is used to get the page data by the db id, to account for duplicate page ids and different app ids
async function getPageDataByDbId(pageDbId, supabase) {
  const { data, error } = await supabase
    .from(appDbKeys.pageTable)
    .select('*')
    .eq('app_id', config.appId)
    .eq('id', pageDbId)
    .single();
    if (error) throw new Error('Error fetching page data');
    return {data:data,error:error};
}

 async function getUserAccessToken(userId: string, supabase) {
  console.log('userId: ', userId)
  const { data, error } = await supabase
    .from(appDbKeys.userTable) 
    .select('user_access_token')
    .eq('id', userId)
    .or(`id.eq.${userId},id.eq."${userId}"`)
    .single();

  if (error) throw new Error('Error fetching user access token1');
  return data.user_access_token;
}




const getPageAccessToken = async (pageId, supabase) => {
  try {
    console.log('Attempting to get most recent page access token for pageId:', pageId);
    const { data, error } = await supabase
      .from(appDbKeys.pageTable)
      .select('page_access_token,created_at')
      .eq('app_id', config.appId)
      .eq('fb_page_id', pageId)
      // .eq('ig_account_id', pageId)
      // .or(`fb_page_id.eq.${pageId},ig_account_id.eq.${pageId}`)
      .order('created_at', { ascending: false })
      .limit(1);

    if (error) {
      console.error('Error retrieving page access token:', error);
      return null;
    }

    if (data && data.length > 0) {
      console.log('getPageAccessToken Found most recent page access token. Creation date:', data[0].created_at);
      console.log('data list:', data[0] )
      return data[0].page_access_token;
    }

    console.log('No page access token found for pageId:', pageId, data);
    return null; 
  } catch (err) {
    console.error('Unexpected error in getPageAccessToken:', err);
    return null; 
  }
};


// function to return the ad data and user data given an ad id or ad trace id
async function getAdDataAndUserDataFromDbWithAdIdOrAdTraceId  (supabase, identifier) {
  const { data: adData, error: adError } = await getAdFromDbByAdIdOrAdTraceId(supabase, identifier);
  if (adError) {
    console.error('Error retrieving ad details:', adError);
    return {matchFound: false};
  } 

  if (adData && adData.length > 0) {
    console.log('adData', adData)
    console.log('getAdDataAndUserData Found ad details:', adData[0]);
    const { data: userData, error: userError } = await supabase
      .from(appDbKeys.userTable)  
      .select('id')
      .eq('app_id', config.appId)
      .eq('id', adData[0].user_id)
      .single();

    if (userError) {
      console.error('Error retrieving user details:', userError); 
      return {matchFound: false};
    }

    console.log('userData', userData)
    console.log('getAdDataAndUserData Found user details:', userData); 
    return {matchFound: true,
      adData: adData[0].fb_ad_id,
      userData: userData.id};
  }

  console.log('No ad data or user data found for adId or adTraceId:', identifier);
  return {matchFound: false};
  
}

//

const getPageAccessTokenByAdIdOrAdTraceIdAndPageId = async (identifier, pageId, supabase) => {

  console.log('getPageAccessTokenByAdIdOrAdTraceIdAndPageId getting the page access token for identifier:', identifier, 'and pageId:', pageId);
  // check if page is managed by multiple users


  const {isManagedByMultipleUsers, data:pagesData, userDataArray} = await checkIfPageIsManagedByMultipleUsers(pageId, supabase, true);
      console.log('isManagedByMultipleUsers:', isManagedByMultipleUsers);
      console.log('pagesData:', pagesData);
      console.log('userDataArray:', userDataArray); 

  let pageAccessTokenFromReturnableObject = pagesData[0].page_access_token;
  let userObjectFromReturnableObject = userDataArray[0][0];
  console.log('pageAccessTokenFromReturnableObject:', pageAccessTokenFromReturnableObject);
  console.log('userObjectFromReturnableObject:', userObjectFromReturnableObject);
  let fallBackValidAccessToken = await getPageAccessToken (pageId, supabase);
  console.log('fallBackValidAccessToken', fallBackValidAccessToken)
  let returner = {pageAccessToken: fallBackValidAccessToken ? fallBackValidAccessToken : pageAccessTokenFromReturnableObject};
  if (isManagedByMultipleUsers) {

  try {
    console.log('Attempting to get page access token by ad trace id:', identifier);
    const { data:adData, error:adError } = await getAdFromDbByAdIdOrAdTraceId(supabase, identifier);

    if (adError) {
      console.error('Error retrieving ad details:', adError);
      return returner;
    }

    if (adData && adData.length > 0) {
      console.log('getPageAccessTokenByAdTraceIdAndPageId Found ad details:', adData[0]);
      const { data:userData, error:userError } = await supabase
        .from(appDbKeys.userTable)
        .select('fb_id,fb')
        .eq('app_id', config.appId)
        .eq('id', adData[0].user_id)
        .single();  

      if (userError) {
        console.error('Error retrieving user details:', userError);
        return returner;
      }

      console.log('getPageAccessTokenByAdTraceIdAndPageId Found user details:', userData[0]);
  
      if (userData && userData.length > 0) {
        console.log('getPageAccessTokenByAdTraceIdAndPageId Found user details:', userData[0]);
        const { data:pageData, error:pageError } = await supabase
          .from(appDbKeys.pageTable)
          .select('page_access_token')
          .eq('app_id', config.appId)
          .eq('fb_id', userData[0].fb_id) 
          .eq('fb_page_id', pageId)
          .eq('is_token_valid', true)
          .limit(1);

          // .single();

        if (pageError) {
          console.error('Error retrieving page details:', pageError);
          return returner;
        }

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

const getPageAccessTokenAndValidateBeforeReturn = async (pageId, supabase) => {
  // DO NOT DELETE

  // try {
  //   console.log('Attempting to get most recent page access token for pageId:', pageId);
  //   const { data, error } = await supabase
  //   .from(appDbKeys.pageTable)
  //   .select('id,page_access_token,created_at,fb_page_id')
  //   .eq('app_id', config.appId)
  //   .eq('fb_page_id', pageId)
  //   // .eq('ig_account_id', pageId)
  //   // .or(`fb_page_id.eq.${pageId},ig_account_id.eq.${pageId}`)
  //   .order('created_at', { ascending: false })

  //   if (error) {
  //     console.error('Error retrieving page access token:', error);
  //     return null;
  //   }

  //   if (data && data.length > 0) {
  //     console.log('getPageAccessTokenAndValidateBeforeReturn Found most recent page access token. Creation date:', data[0].created_at);
  //     console.log('data list:', data[0] )
  //      // Get page details from database
  //      const {isManagedByMultipleUsers, data:pagesData, userDataArray} = await checkIfPageIsManagedByMultipleUsers(data[0].fb_page_id, supabase, true);
  //     console.log('isManagedByMultipleUsers:', isManagedByMultipleUsers);
  //     console.log('pagesData:', pagesData);
  //     console.log('userDataArray:', userDataArray); 

  //     if (!isManagedByMultipleUsers && (pagesData.length == 0 || pagesData == null)) {
  //       console.log(`The page with the fb_page_id: ${data[0].fb_page_id} could not be found in the database`);
  //       throw new Error(`The page with the fb_page_id: ${data[0].id} could not be found in the database`);
  //     } else if (!isManagedByMultipleUsers && pagesData.length === 1){

  //       console.log('userDataArray(the page) is managed by a single user');
  //         const singleUserData = userDataArray[0][0];
  //         const singlePageData = pagesData[0];
  //         const debuggedData = await debugUserAndPageAccessTokens(singleUserData, singlePageData, supabase);
  
  //         let debuggedDataArray = [];
  //         debuggedDataArray.push(debuggedData);
  //         const debuggedReturnableObject = {
  //           // appId: config.appId,
  //           // appContext: config.appContext,
  //           ...debuggedData,
  //           workingPageData: debuggedData.pageAccessTokenValid && debuggedData.userAccessTokenValid && debuggedData.pageMessagingEnabled && debuggedData.instagramMessagingEnabled ? debuggedData : null,
  //           teamDebuggedData: debuggedDataArray,
  //           isManagedByMultipleUsers: isManagedByMultipleUsers,
  //           numberOfUsers: userDataArray.length,
  //           numberOfPages: pagesData.length,
  //         }
  //         return debuggedReturnableObject;
  //     }
  //     else{
  //       console.log(`The page with the fb_page_id: ${data[0].fb_page_id} is managed by multiple users, checking the user_id and page_db_id`);
  
  //           // loop through and merge the userDataArray with the right pageDataArray
  //         let debuggedDataArray = [];
  //           for (const singlePageData of pagesData) {
  //             console.log('xxxx singlePageData.id:', singlePageData.id);
  //             console.log('aaaa userDataArray:', userDataArray);
  //             for (const singleUserData of userDataArray) {
  //               console.log('yyyyy singleUserData.id:', singleUserData[0].id);
  //               if (singleUserData[0]?.fb_id && singlePageData.fb_id === singleUserData[0].fb_id) {
  //                 console.log('debugging user:', singleUserData[0].fb_id);
  //                 const debuggedData = await debugUserAndPageAccessTokens(singleUserData[0], singlePageData, supabase);
  //                 debuggedDataArray.push(debuggedData);
  //               }
  //             }
  //           }
  //           console.log('zzzz debuggedDataArray after loop:', debuggedDataArray);
  //           let singlePageData = null;
  //            for (const singleDebuggedData of debuggedDataArray) {
  //             // get the initial pages debugged data using the userDbId from the debuggedData
  //             // console.log(`checking staus report for user: ${singleDebuggedData.userDbId} ?=${user_id}`);   
  //             if (singleDebuggedData.pageAccessTokenValid && singleDebuggedData.userAccessTokenValid && singleDebuggedData.pageMessagingEnabled && singleDebuggedData.instagramMessagingEnabled) {
  //               console.log('found staus report for user:', singleDebuggedData.userDbId);
  //               singlePageData = singleDebuggedData;
  //               break;
  //             }else{
  //               console.log('access token is not valid for user:', singleDebuggedData.userDbId);
  //               // console.log('no staus report for user:', singleDebuggedData.userDbId, 'singleDebuggedData user_id:', user_id);
  //             }
  //           };
  //         const debuggedReturnableObject = {
  //           // appId: config.appId,
  //           // appContext: config.appContext,
  //           ...singlePageData,
  //           workingPageData: singlePageData,
  //           teamDebuggedData: debuggedDataArray,
  //           isManagedByMultipleUsers: isManagedByMultipleUsers,
  //           numberOfUsers: userDataArray.length,
  //           numberOfPages: pagesData.length,
  //         }
  //         return debuggedReturnableObject;
  //     }

  //     // return data[0].page_access_token;
  //   }else{
  //     console.log('No getPageAccessTokenAndValidateBeforeReturn page access token found for pageId:', pageId, data);
      
  //     // return null; 
  //   }

  //   // console.log('No page access token found for pageId:', pageId, data);
  //   return null; 
  // } catch (err) {
  //   console.error('Unexpected error in getPageAccessTokenAndValidateBeforeReturn:', err);
  //   return null; 
  // } 
};

const checkIfPageIsManagedByMultipleUsers = async (identifier, supabase, activeOnly: boolean = false) => {
  try {
    console.log('Attempting to retrieve checkIfPageIsManagedByMultipleUsers for identifier:', identifier);
    let userDataArray = [];

    const { data, error } = await supabase
      .from(appDbKeys.pageTable)
      .select('*')
      .eq('app_id', config.appId)
      .eq('active', activeOnly)
      .or(`ig_account_id.eq.${identifier},fb_page_id.eq.${identifier}`);

    if (error) {
      console.error(`Error retrieving fbPageId for identifier: ${identifier}`, error);
      throw `Error retrieving fbPageId for identifier: ${identifier} and app_id: ${config.appId} ${error}`;
    }

    console.log('data.length:', data.length);

    if (data && data.length > 0) {
      const uniqueFbIds = Array.from(new Set(data.map(page => page.fb_id)));
      
      const { data: userData, error: userError } = await supabase
        .from(appDbKeys.userTable)
        .select('*')
        .in('fb_id', uniqueFbIds)
        .eq('app_id', config.appId);

      if (userError) {
        console.error(`Error fetching users data: ${userError}`);
        throw `Error fetching users data: ${userError}`;
      }

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

const upsertAccessToken = async (params) => {
  const { supabase, appid, userId, fbId, pageId, accessTokenData, accessTokenType } = params;
  console.log(`Upxerting access token: ${accessTokenType} for user: ${userId} and fbId: ${fbId} and pageId: ${pageId}`);

  let queryParam = accessTokenType !== 'user' ? `page_id.eq.${pageId}` : `fb_id.eq.${fbId}`;
  let queryParam2 = accessTokenType !== 'user' ? `access_token.eq.${accessTokenData.pageAccessToken}` : `access_token.eq.${accessTokenData.userAccessToken}`;
  try {
    // First, try to find any existing record
    const { data: existingToken, error: findError } = await supabase
      .from(appDbKeys.accessTokensTable)
      .select('*')
      .eq('app_id', appid)
      .eq('user_id', userId)
      .eq('fb_id', fbId)
      .eq('access_token_type', accessTokenType)
      .or(queryParam2)
      .or(queryParam)
      .limit(1)

    if (findError) {
      console.log('Error checking for existing token:', findError);
      console.log('Error checking for existing token.length:', existingToken.length);
      // throw findError;
    }

     // Convert zero timestamps to 90 days from now and Unix timestamps to PostgreSQL timestamps
     const convertTimestamp = (timestamp) => {
      if (!timestamp || timestamp === 0) return new Date(Date.now() + 90 * 24 * 60 * 60 * 1000).toISOString();
      // Convert Unix timestamp (seconds) to JavaScript Date then to ISO string
      return new Date(timestamp * 1000).toISOString();
    };
    // Prepare the token data
    const tokenData = {
      app_id: appid,
      user_id: userId,
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
      expires_at: convertTimestamp (accessTokenType === 'user' ? accessTokenData.userTokenExpiresAt : accessTokenData.pageTokenExpiresAt),
      token_data_access_expires_at: convertTimestamp (accessTokenType === 'user' ? accessTokenData.userDataAccessExpiresAt : accessTokenData.pageDataAccessExpiresAt),
      scopes: accessTokenType === 'user' ? accessTokenData.userScopes : accessTokenData.pageScopes,
      missing_scopes: accessTokenType === 'user' ? accessTokenData.missingScopesForUser : accessTokenData.missingScopesForPage,
      details: accessTokenData
    };

    let result;
    if (existingToken && existingToken.length > 0) {
      console.log('existingToken:', existingToken);
      console.log(`Updating type: ${accessTokenType} existing access token: ${existingToken.id} for user: ${userId} and fbId: ${fbId} and pageId: ${pageId}`);
      // Update existing record
      const { data, error } = await supabase
    .from(appDbKeys.accessTokensTable)
        .update(tokenData)
    .eq('app_id', appid)
    .eq('user_id', userId)
    .eq('fb_id', fbId)
    .eq('access_token_type', accessTokenType)
        .select();

      if (error) throw error;
      result = data;
      console.log('Updated existing access token', data);
    } else {
      console.log(`No existing access token found, inserting new record for user: ${userId} and fbId: ${fbId} and pageId: ${pageId} and accessTokenType: ${accessTokenType}`);

      // Insert new record
      const { data, error } = await supabase
        .from(appDbKeys.accessTokensTable)
        .insert(tokenData)
        .select();

      if (error) throw error;
      result = data;
      console.log('Inserted new access token');
    }

    return result;
  } catch (error) {
    console.error('Error upserting access token:', error);
    throw error;
  }
};


async function fetchNewPageTokenFromFbUserAccounts(supabase, fbUserId, userAccessToken, targetPageId) {
  let nextUrl = `https://graph.facebook.com/${fbUserId}/accounts?access_token=${userAccessToken}`;
  let foundPage = null;

  console.log('targetPageId', targetPageId)
  while (nextUrl && !foundPage) {
    const response = await axios.get(nextUrl);
    // console.log('response', response)
    const pages = response.data.data;
    console.log('pages', pages)
    foundPage = pages.find(page => page.id === targetPageId);


    if (!foundPage && response.data.paging && response.data.paging.next) {
      console.log('found next page')
      nextUrl = response.data.paging.next;
    } else {
      console.log('no more pages to fetch')
      nextUrl = null;
    }
  }

  if (foundPage) {

     console.log('foundPage', foundPage)
    return foundPage.access_token;
  } else {
    console.log('could not find the requested page in the list of pages controled by the user,User no longer controls this page.')
    return null;
  }
}

const debugTokenViaMetaApi = async (params) => {
  try {
    const result = await axios.get(`https://graph.facebook.com/debug_token`, {
      params: {
        input_token: params.accessToken,
        access_token: `${config.appId}|${config.appSecret}`
      }
    });
    return result.data.data;
  } catch (error) {
    console.error('Error in debugTokenViaMetaApi:', config.flattenForLog(error));
    throw error;
  }
}


const debugPageAccessTokens = async (params: {supabase: any;appid: string | number;userId: string | number;fbId: string;pageId: string; accessToken: string;}): Promise<TokenDebugResult> => {
 
 
  // STEP 1: DEBUG THE PAGE ACCESS TOKEN
  const result = await debugTokenViaMetaApi({accessToken: params.accessToken});
  console.log('result1 from debug page access token', result)

  let pageTokenData = null;

  // STEP 2: Check if the page access token is valid

  
  if((result.error && result.error.message) ||(!result.is_valid)){
    console.log('page access token is not valid, fetch new page access token')
    // fetch the user access token from db
    const { data: userDataFromDb, error: userDataFromDbError } = await supabase
    .from(appDbKeys.userTable).select('user_access_token').eq('id', params.userId)

    if(userDataFromDbError){
      console.error('Error fetching user access token2:', userDataFromDbError);
      throw userDataFromDbError;
    }

    console.log('fetching new page access token from fb by getting the users accounts/pages and then getting the page access token', userDataFromDb)


    // STEP 3: Fetch the new page access token from fb
    // try fetchNewPageTokenFromFbUserAccounts with the user access token
    const newPageAccessToken = await fetchNewPageTokenFromFbUserAccounts(supabase, params.fbId, userDataFromDb[0].user_access_token, params.pageId);

    if(!newPageAccessToken || newPageAccessToken === null){
      throw result.error;
    }

    // STEP 4: Debug the new page access token
    const result2 = await debugTokenViaMetaApi({accessToken: newPageAccessToken});
    console.log('result2', result2)


    if(result2.error && result2.error.code === 190){
      throw result2.error;
    }  
    pageTokenData = {...result2, access_token: newPageAccessToken};
  }else{
    console.log('page access token is valid, return result.data.data')
    console.log('result', result)
    pageTokenData = {...result, access_token: params.accessToken};
  }

  console.log('pageTokenDatacvgjhkl', pageTokenData)
   

  // STEP 5: Return the page access token data
  // Note the returned access token could be different from the one recieved in the params
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

const debugUserAccessToken = async (params: {
  supabase: any;
  appid: string | number;
  userId: string | number;
  fbId: string;
  accessToken: string;
}): Promise<TokenDebugResult> => {
  // Implementation here

  const result = await debugTokenViaMetaApi({accessToken: params.accessToken});
  console.log('result from debug user access token', result)

  // if (result.data && result.data.data){
  //   userData.is_valid
  // }
  if (result.error && result.error.code === 190){
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
    hasMessagingPermission: userData.granular_scopes.some(scope => 
      scope.scope === 'pages_messaging'
    ),
    hasInstagramMessagingPermission: userData.granular_scopes.some(scope => 
      scope.scope === 'instagram_manage_messages'
    ),
    hasAdPermission: userData.granular_scopes.some(scope => 
      scope.scope === 'ads_management'
    )
  };
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
    }) as TokenDebugResult;

    console.log('user token debug result', userTokenDebugResult)
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
    }) as TokenDebugResult;



  // Note the returned access token could be different from the one recieved in the params
  // todo add the new page access token to the db

  if(pageTokenDebugResult.accessToken && pageTokenDebugResult.accessToken !== pageData.page_access_token){
    console.log('new page access token found, updating the db', pageTokenDebugResult.accessToken)
    // update the db
    await supabase.from(appDbKeys.pageTable).update({page_access_token: pageTokenDebugResult.accessToken}).eq('fb_page_id', pageData.fb_page_id).eq('app_id', pageData.app_id);
    console.log('page access token updated in the db')
    combinedResult.pageAccessToken = pageTokenDebugResult.accessToken;
  }

 
    console.log('page token debug result', pageTokenDebugResult)

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
    if(!userTokenDebugResult.isValid){

    }

    // check if page access token is valid
    if(!pageTokenDebugResult.isValid){

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
        await upsertAccessToken({
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
    
    ])
   
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



// we want a new version of this function that uses the page table's is token valid field to check if the token is valid
// return the first page access token that is valid
const getAPageAccessTokenThatIsValid = async (pageId, supabase) => {
  try {
    console.log('Getting valid page access token for pageId:', pageId);
    
    console.log('config.appId', config.appId)


    const { data: pageData, error: pageError } = await supabase
      .from(appDbKeys.pageTable)
      .select('id,page_access_token,is_token_valid')
      .eq('app_id', config.appId)
      .eq('fb_page_id', pageId)
      .eq('is_token_valid', true)
      //order by created_at descending
      .order('created_at', { ascending: false })
      .limit(1); 

    if (pageError) {
      console.error('Error checking page table:', pageError);
      throw pageError;
    }

    if (pageData && pageData.length > 0) {
      console.log('pageData found, return here?', pageData[0])
      return pageData[0].page_access_token;
    }
    
  } catch (err) {
    console.error('Error in getAPageAccessTokenThatIsValid:', err, config.flattenForLog(err));
    console.log(' returning a fall default page access token')
    return await getPageAccessToken(pageId, supabase);
  }
};



// if api call is made and the page access token is not valid we want to find the page db id it belongs to and update the page table's is token valid field to false (makefbapi call to update the is token valid field to false)



const updateIsTokenValid = async (pageAccessToken,isTokenValid, supabase) => {
  // lets handle situation where access token is not found in the pages table ie it is a user access token
  const { data: pageData, error: pageError } = await supabase
    .from(appDbKeys.pageTable)
    .select('page_access_token')
    .eq('page_access_token', pageAccessToken)
    .eq('app_id', config.appId)
    .limit(1);

  if(pageError){
    console.error('Error checking page table:', pageError);
    throw pageError;
  }

  if(pageData && pageData.length > 0){
    return await supabase.from(appDbKeys.pageTable).update({is_token_valid: isTokenValid}).eq('page_access_token', pageAccessToken);
  }else{
    console.log('page access token not found in the pages table, it is a user access token')
    // update the user table's is token valid field to false
    return await supabase.from(appDbKeys.userTable).update({is_token_valid: isTokenValid}).eq('user_access_token', pageAccessToken);
  }
}


const getValidUserAccessTokensForAd = async (adId,pageId, supabase, requirements = { needsMessaging: false, needsInstagram: false, needsAds: true }) => {
  try {
    console.log('getValidUserAccessTokensForAd valid user access token for adId:', adId);

    //check if page is managed by multiple users
    const { isManagedByMultipleUsers, data: pagesData, userDataArray } = await checkIfPageIsManagedByMultipleUsers(pageId, supabase);

    const userObjects = userDataArray.map(arr => arr[0]).filter(Boolean);
    const fbIds = userObjects.map(user => user.fb_id);
    const userAccessTokens = userObjects.map(user => user.user_access_token);

    console.log('fbIds', fbIds)
    console.log('userAccessTokens', userAccessTokens)
    console.log('userObjects', userObjects)



    let existingTokens = [];

    for (const user of userObjects) {
      if (!user || !user.fb_id || !user.user_access_token) continue;

      // get the matching page data from pagesData
      const matchingPageData = pagesData.find(page => page.fb_id === user.fb_id);
    
      if (user.is_token_valid && user.has_ads) {
        existingTokens.push({user,page:matchingPageData});
      }else{
        console.log('user is not valid or does not have ads')
      }
    }

    if(existingTokens && existingTokens.length > 0){
  
      return existingTokens.map(data => ({
        userAccessToken: data.user.user_access_token,
        userDbId: data.user.id,
        pageAccessToken: data.page.page_access_token,
        // userFbName: data.userFbName
      }));

    }else{
        return userDataArray.map(data => ({
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


const getValidUserAccessToken = async (adId,pageId, supabase, requirements = { needsMessaging: false, needsInstagram: false, needsAds: true }) => {};









async function upsertAdAccount(supabase,appid, userId: string, adAccountData: any) {
  console.log('Upserting adAccount:', JSON.stringify(adAccountData, null, 2));

  try {
    const { data, error } = await supabase
      .from(appDbKeys.adAccountsTable)
      .upsert({
        fb_ad_account_id: adAccountData.fb_ad_account_id,
        app_id: appid,
        user_id: userId,
        name: adAccountData.name,
        details: adAccountData
      },
       {
        onConflict: 'fb_ad_account_id',
        returning: 'minimal'
      }
    );

    if (error) throw error;
    console.log('Ad account upserted successfully');
    return data;
  } catch (error) {
    console.error('Error upserting ad account:', error);
    throw error;
  }
}

  async function upsertCampaign(supabase, appid,userId: string, campaignData: any) {
  console.log('Upserting campaign:', JSON.stringify(campaignData, null, 2));
  console.log('appid:', appid);
  console.log('userId:', userId); 
  console.log('campaignData:', campaignData);
  try {
    const {error } = 
    
    await supabase
      .from(appDbKeys.campaignsTable)
      .upsert({
        fb_campaign_id: campaignData.fb_campaign_id,
        app_id: appid,
        user_id: userId,
        ad_account_id: campaignData.ad_account_id,
        name: campaignData.name,
        objective: campaignData.objective,
        status: campaignData.status,
        details: campaignData
      }
     
      
      , {
        onConflict: 'fb_campaign_id',
        returning: 'minimal'
      }
    
    );

    if (error) throw error;
    console.log('Campaign upserted successfully');
    // return data;
  } catch (error) {
    console.error('Error upserting campaign:', error);
    throw error;
  }
}

async function upsertAdSet(supabase,appid, userId: string, adSetData: any) {
  console.log('Upserting ad set:', JSON.stringify(adSetData, null, 2));
  try {
    const { data, error } = await supabase
      .from(appDbKeys.adSetsTable)
      .upsert({
        fb_ad_set_id: adSetData.fb_ad_set_id,
        app_id: appid,
        user_id: userId,
        campaign_id: adSetData.campaign_id,
        name: adSetData.name,
        optimization_goal: adSetData.optimization_goal,
        billing_event: adSetData.billing_event,
        bid_strategy: adSetData.bid_strategy,
        status: adSetData.status,
        details: adSetData
      }, {
        onConflict: 'fb_ad_set_id',
        returning: 'minimal'
      });

    if (error) throw error;
    console.log('Ad set upserted successfully');
    return data;
  } catch (error) {
    console.error('Error upserting ad set:', error);
    throw error;
  }
} 
 async function getAdFromDbByAdIdOrAdTraceId(supabase, adId) {
  console.log('Attempting to get ad from db for adId:', adId);
   // Ensure adId is treated as string
   const adIdString = String(adId);
  try {
    const { data, error } = await supabase
      .from(appDbKeys.adsTable)
      .select('*')
    .eq('app_id', config.appId)
    .or(`fb_ad_id.eq.${adIdString},audos_ad_trace_id.eq.${adIdString}`)
    // .eq('fb_ad_id', adIdString)
    // .single();
  if (error) throw error;
  // console.log('Ad from db:', data);
  return {data, error};
  } catch (error) {
    console.error('Error getting ad from db:', error);
    return {data:null, error:error};
  }
}

async function getCreativeFromDbByCreativeIdOrAdTraceId(supabase, creativeId) {
  console.log('Attempting to get creative from db for creativeId:', creativeId);
  const creativeIdString = String(creativeId);
  try {
    const { data, error } = await supabase
      .from(appDbKeys.adCreativesTable)
      .select('*')
      .or(`fb_creative_id.eq.${creativeIdString},audos_ad_trace_id.eq.${creativeIdString}`)
      // .single();  
    if (error) throw error;
    return {data, error};
  } catch (error) {
    console.error('Error getting creative from db:', error);
    return {data:null, error:error};
  }
}





async function upsertAd(supabase,appid, userId: string, adData: any) {
  console.log('Upserting ad:', JSON.stringify(adData, null, 2));
  try {
    const { data, error } = await supabase
      .from(appDbKeys.adsTable)
      .upsert({
        fb_ad_id: adData.fb_ad_id,
        app_id: appid,
        user_id: userId,
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
      }
      
      , {
        onConflict: 'fb_ad_id',
        returning: 'minimal'
      }
    
    );

    if (error) throw error;
    console.log('Ad upserted successfully');
    return data;
  } catch (error) {
    console.error('Error upserting ad:', error);
    throw error;
  }
}

export async function findAdMediaByAdId(supabase,id,adAccountId = null) {
  console.log('Finding ad media by id:', id);
  try {

    if(adAccountId && adAccountId !== null){
    const { data, error } = await supabase
      .from(appDbKeys.adMediaTable)
      .select('*')
      .eq('app_id', config.appId)
      .eq('ad_account_id', adAccountId)
      .or(`fb_image_hash.eq.${id},fb_video_id.eq.${id},original_media_url.eq.${id}${isNaN(Number(id)) ? '' : `, id.eq.${id}`}`)
      .limit(1); 
      
    if (error) throw error;
    return data;
    }else{
      const { data, error } = await supabase
        .from(appDbKeys.adMediaTable)
        .select('*')
        .eq('app_id', config.appId)
        .or(`fb_image_hash.eq.${id},fb_video_id.eq.${id},original_media_url.eq.${id}${isNaN(Number(id)) ? '' : `, id.eq.${id}`}`)
        .limit(1); 
      if (error) throw error;
      return data;
    }
  } catch (error) {
    console.error('Error finding ad media:', error);
    throw error;
  }
}

 
export async function upsertAdMedia(supabase,adMediaData: any) {
  console.log('Upserting ad media:', JSON.stringify(adMediaData, null, 2));


  const adMedia = await findAdMediaByAdId(supabase,adMediaData.fb_image_hash? adMediaData.fb_image_hash : adMediaData.fb_video_id,adMediaData.ad_account_id);
  if(adMedia && (adMedia.status === 'uploaded'||adMedia.status === 'failed'||adMedia.status === 'uploading' || adMedia.status === 'processing')){
    console.log('Ad media already exists:', adMedia);
  }


  if(adMediaData.id ){

    try{
      const { data, error } = await supabase
        .from(appDbKeys.adMediaTable)
        .update({
          status: adMediaData.status,
          upload_response: adMediaData.upload_response,
          original_media_url: adMediaData.original_media_url,
          height: adMediaData.height,
          width: adMediaData.width
        })
        .eq('id', adMediaData.id)
        .select()
        .single();
      if (error) throw error;
      console.log('Ad media updated successfully');
      return data;
    } catch (error) {
      console.error('Error updating ad media:', error);
      throw error;
    }
  }
  else{
  
  try {

    if(adMediaData.fb_image_hash){
    const { data, error } = await supabase
      .from(appDbKeys.adMediaTable)
      .upsert({
        app_id: config.appId,
        fb_image_hash: adMediaData.fb_image_hash,
        fb_video_id: null,
        is_video: false,
        is_image: true,
        original_media_url: adMediaData.original_media_url,
        ad_account_id: adMediaData.ad_account_id,
        status: adMediaData.status,
        height: adMediaData.height,
        width: adMediaData.width,
        upload_response: null
      })
      .select()
      .single();
      if (error) throw error;
      console.log('Ad Image upserted successfully');
      return data;
    }
    else if(adMediaData.fb_video_id){
      const { data, error } = await supabase
        .from(appDbKeys.adMediaTable)
        .upsert({
          app_id: config.appId,
          fb_image_hash: null,
          fb_video_id: adMediaData.fb_video_id,
          is_video: true,
          is_image: false,
          original_media_url: adMediaData.original_media_url,
          ad_account_id: adMediaData.ad_account_id,
          status: adMediaData.status,
          height: adMediaData.height,
          width: adMediaData.width,
          upload_response: null
        })
        .select()
        .single();
        if (error) throw error;
        console.log('Ad Video upserted successfully');
        return data;
    }
    else{
      console.error('Error upserting ad media: no fb_image_hash or fb_video_id');
      throw new Error('Error upserting ad media: no fb_image_hash or fb_video_id');
    }
  } catch (error) {
    console.error('Error upserting ad media:', error);  
    throw error;
  }
  }
  
}

 async function upsertCreative(supabase,appid, userId: string, creativeData: any) {
  console.log('Upserting creative:', JSON.stringify(creativeData, null, 2));
  try {
    const { data, error } = await supabase
      .from(appDbKeys.adCreativesTable)
      .upsert({
        fb_creative_id: creativeData.fb_creative_id,
        app_id: appid,
        user_id: userId,
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
      }, {
        onConflict: 'fb_creative_id',
        returning: 'minimal'
      });

    if (error) throw error;
    console.log('Creative upserted successfully');
    return data;
  } catch (error) {
    console.error('Error upserting creative:', error);
    throw error;
  }
}
  
async function readMessages(queryParams, supabase) { 

  try {
    const { data, error } = await supabase
      .from(appDbKeys.pageMessagesTable)
      .select('*')
      .match(queryParams)
      .eq('app_id', config.appId)
      // .abortSignal(controller.signal)

    // clearTimeout(timeout);

    if (error) {
      console.error('Error reading messages:', error);
      return null; 
    }

    // console.log('Messages read successfully:', data);
    return data;
  } catch (err) {
    if (err.name === 'AbortError') {
      console.error('Query timed out after 30 seconds');
    } else {
      console.error('Unexpected error in readMessages:', err);
    };
    return null; 
  }
}


async function readComments(queryParams, supabase) {
  try {
    const { data, error } = await supabase
      .from(appDbKeys.pageCommentsTable)
      .select('*')
      .eq('app_id', config.appId)
      .match(queryParams);

    if (error) {
      console.error('Error reading comments:', error);
      return null; 
    }

    // console.log('Comments read successfully:', data);
    return data;
  } catch (err) {
    console.error('Unexpected error in readComments:', err);
    return null; 
  }
}

// NOTE: there could be multiple pages for the same fb_page_id
async function readPageData(queryParams, supabase) {
  try {
    const { data, error } = await supabase
      .from(appDbKeys.pageTable)
      .select('*')
      .eq('app_id', config.appId)
      .match(queryParams);

    if (error) {
      console.error('Error reading page data:', error);
      return null; 
    }

    console.log('Page data read successfully:', data);
    return data;
  } catch (err) {
    console.error('Unexpected error in readPageData:', err);
    return null; 
  }
}

async function changeConversationActiveStatus(supabase, status: boolean, conversationId: string, updatedByUserId: string) {
  console.log('Attempting to change conversation active status...', status, conversationId, updatedByUserId);
  try {
    const { data, error } = await supabase
      .from(appDbKeys.pageConversationsTable)
      .update({ active: status , updated_at: new Date(), last_modified_by_user_id: updatedByUserId})
      .eq('id', conversationId)
      .eq('app_id', config.appId)
      .select()
      // .single();
      .limit(1);
      if (error) {
        console.error('Error updating conversation active status:', error);
        throw error;
      }
      console.log('Conversation active status updated successfully:', data);
      return data;
  } catch (error) {
    console.error('Unexpected error in changeConversationActiveStatus:', error);
    throw error;
  }
}
 
async function getConversationByFbConversationId(supabase, fbConversationId: string) {
  console.log('Attempting to get conversation by fb conversation id...', fbConversationId);
  try {
    const { data, error } = await supabase
      .from(appDbKeys.pageConversationsTable)
      .select('*')
      .eq('fb_conversation_id', fbConversationId)
      .eq('app_id', config.appId)
      .select()
      .limit(1);
      if (error) {
        console.error('Error getting conversation by fb conversation id:', error);
        throw error;
      }
      console.log('Conversation by fb conversation id:', data);
      return data;
  } catch (error) {
    console.error('Unexpected error in getConversationByFbConversationId:', error);
    throw error;
  }
}





async function upsertLead (supabase, params) {
  const leadData = {
    app_id: params.app_id,
    conversation_id: params.conversation_id, 
    fb_page_id: params.fb_page_id,
    fb_conversation_id: params.fb_conversation_id,
    fb_ad_id: params.fb_ad_id,
    foreign_ad_id: params.foreign_ad_id,
    recipient_page_scope_id: params.recipient_page_scope_id,
    conversation_platform: params.conversation_platform
  };
  try {
  const { data, error } = await insertLead(supabase, leadData)
  if (error) throw error;
  return data;
    
  } catch (error) {
    console.error('Error inserting lead:', error);
    throw error;
  }
  
}

async function insertLead (supabase, leadData) {
  console.log('Inserting lead:', leadData);
  try {
    const { data, error } = await supabase
      .from(appDbKeys.leadsTable)
      .insert([leadData])
      .select()
      // .single();
      .limit(1);
    if (error) throw error;
    console.log('Lead inserted successfully:', data);
    return {data, error};
  } catch (error) {
    console.error('Error inserting lead:', error);
    throw error;
  }
};

async function getLeadsByConversationId (supabase, conversationId) {
  try {
    const { data, error } = await supabase
      .from(appDbKeys.leadsTable)
      .select('*')
      .eq('conversation_id', conversationId);

    if (error) throw error;
    return data;
  } catch (error) {
    console.error('Error getting leads:', error);
    throw error;
  }
};


async function upsertContact (supabase, params) {
  // console.log('Upserting contact:', params);
  const contactData = {
    app_id: params.appId,
    conversation_id: params.conversationId,
    fb_page_id: params.fbPageId,
    fb_conversation_id: params.fbConversationId,
    fb_ad_id: params.fbAdId,
    recipient_page_scope_id: params.recipientPageScopeId,
    conversation_platform: params.conversation_platform,
    source: params.source, // or 'message', 'comment', 'form', etc.
    message_id: params.messageId,
    contact_type: params.contactType, // or 'phone', 'address', etc.
    contact_value: params.contactValue
  };
  try {
  const { data, error } = await insertContact(supabase, contactData)
  if (error) throw error;
  return data;
    
  } catch (error) {
    console.error('Error inserting contact:', error);
    throw error;
  }
  
}

async function insertContact (supabase, contactData) {
  try {
    const { data, error } = await supabase
      .from(appDbKeys.contactsTable)
      .insert([contactData])
      .select()
      // .single();
      .limit(1);
    if (error) throw error;
    console.log('Contact inserted successfully:', data);
    return {data, error};
  } catch (error) {
    console.error('Error inserting contact:', error);
    throw error;
  }
};

async function getContactsByConversationId (supabase, conversationId) {
  try {
    const { data, error } = await supabase
      .from(appDbKeys.contactsTable)
      .select('*')
      .eq('conversation_id', conversationId);

    if (error) throw error;
    return data;
  } catch (error) {
    console.error('Error getting contacts:', error);
    throw error;
  }
};

async function updateConversationWithAdInfo (params) {
  try {
    const { data, error } = await params.supabase
      .from(appDbKeys.pageConversationsTable)
      .update({
        conversation_ad_id: params.adInfo.fb_ad_id,
        conversation_source: params.adInfo.conversation_source,
      })
      .eq('id', params.conversationId)
      .select();

    if (error) throw error;
    return data;
  } catch (error) {
    console.error('Error updating conversation with ad info:', error);
    throw error;
  }
};

async function updateConversationWithLeadInfo (supabase, conversationId, leadInfo) {
  try {
    const { data, error } = await supabase
      .from(appDbKeys.pageConversationsTable)
      .update({
        lead_first_name: leadInfo.firstName || null,
        lead_last_name: leadInfo.lastName || null,
        lead_email: leadInfo.email || null,
        lead_phone: leadInfo.phone || null,
        lead_street_address: leadInfo.streetAddress || null,
        lead_business_website: leadInfo.businessWebsite || null,
        updated_at: new Date().toISOString()
      })
      .eq('id', conversationId)
      .select();

    if (error) throw error;
    return data;
  } catch (error) {
    console.error('Error updating conversation with lead info:', error);
    throw error;
  }
};

// Helper function to get all leads and contacts for a conversation
async function getLeadsAndContactsByConversationId (supabase, conversationId) {
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
};
 

// Helper function to get all leads and contacts for a conversation
async function getConversationsByAdId (supabase, adId) {
  try {
    const { data, error } = await supabase
      .from(appDbKeys.pageConversationsTable)
      .select('*')
      .eq('conversation_ad_id', adId)

    if (error) throw error;
    return data;
  } catch (error) {
    console.error('Error getting conversations by ad id:', error);
    throw error;
  }
};

async function getConversationsByLeadId (supabase, leadId) {
  try {
    // First get the conversation_id from leads table
    const { data: leadData, error: leadError } = await supabase
      .from(appDbKeys.leadsTable)
      .select('conversation_id')
      .eq('id', leadId)
      .single();

    if (leadError) {console.error('Error getting conversations by lead id:', leadError); throw leadError;}
    if (!leadData?.conversation_id) {console.error('No conversation id found for lead id:', leadId); return [];}

    // Then get the conversation details
    const { data: conversationData, error: convError } = await supabase
      .from(appDbKeys.pageConversationsTable)
      .select('*')
      .eq('id', leadData.conversation_id);

    if (convError) throw convError;
    return conversationData;
  } catch (error) {
    console.error('Error getting conversations by lead id:', error);
    throw error;
  }
};





class webhookFilter {


  private returnableObject: any;
  private supabase: any;

  
  constructor(returnableObject, supabase) {
    this.returnableObject = returnableObject;
    this.supabase = supabase;
  }
 
  async saveToDb() {
    try {
      if (this.returnableObject.commentOrMessgae === 'message') {
        console.log('Attempting to upsert message...');
        // return await this.upsertMessage(this.returnableObject);
        const result = await this.upsertMessage(this.returnableObject);
        // console.log('Upsert message result:', result);
        return result ? [result] : null;
      } else if (this.returnableObject.commentOrMessgae === 'comment' && this.returnableObject.commentValue) {
        console.log('Attempting to upsert comment...');
        // return await this.upsertComment(this.returnableObject);
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

  // conversation Api table

  async checkIfConversationExists(conversationObject) {
    try {
      // Check if the conversation already exists
      const { data: existingConversation, error: checkError } = await this.supabase
        .from(appDbKeys.pageConversationsTable)
        .select('*')
        .eq('app_id', conversationObject.appId)
        .eq('fb_page_id', conversationObject.fbPageId)
        .eq('fb_conversation_id', conversationObject.fbConversationId)
        // .single();
        .limit(1);
        return existingConversation;
  } catch (checkError) {
    console.error('Unexpected error in checkIfConversationExists:', checkError);
    return null;
  }
  }

  async upsertConversation(conversationObject) {
    // let leadData = conversationObject.extractedLeadData;
    // console.log('conversationObject in upsertConversation', conversationObject); 

    try {
      // Check if the conversation already exists
      const { data: existingConversation, error: checkError } = await this.supabase
        .from(appDbKeys.pageConversationsTable)
        .select('*')
        .eq('app_id', conversationObject.appId)
        .eq('fb_page_id', conversationObject.fbPageId)
        .eq('fb_conversation_id', conversationObject.fbConversationId)
        .single();

      
    const conversationRecord = {
      app_id: conversationObject.appId,
      fb_page_id: conversationObject.fbPageId,
      fb_conversation_id: conversationObject.fbConversationId,
      status_modified_by_user_id: conversationObject.statusModifiedByUserId,
      recipient_page_scope_id: conversationObject.recipientPageScopeId,
      conversation_platform: conversationObject.conversationPlatform,
      ig_account_id: conversationObject.igAccountId,
      active: conversationObject.active,
      //message details
      opening_message_id: conversationObject.messageId,
      //lead details
      conversation_source:null,
      conversation_ad_id:null,
      foreign_ad_id:null,
      lead_first_name: null,
      lead_last_name: null,
      lead_email: null,
      lead_phone: null,
      lead_street_address: null,
      lead_business_website: null,
      //sender details
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
    if (existingConversation) {
      console.log('convo already exists, skipping upsert...');

      // compare the existing conversation record with the new conversation record
      // if the new conversation record has a value, update the existing conversation record
      // if the new conversation record does not have a value, do not update the existingConversation existing conversation record

      // check if the conversation started the same day and if the oldest message was a share type message

      if (conversationObject.extractedLeadData && conversationObject.extractedLeadData.lead_data && conversationObject.extractedLeadData.fb_ad_id !== null && conversationObject.extractedLeadData.fb_ad_id !== undefined) {
      
        console.log("number of messages in this conversationObject", conversationObject.listOfMessagesFromFbApi.length)
       
        if (conversationObject.listOfMessagesFromFbApi &&  conversationObject.listOfMessagesFromFbApi.length < 3) {
         console.log('conversation includes less than 4 messages so it most likely was started from an ad, updating conversation source to ad')

          existingConversation.conversation_source = 'ad';
          existingConversation.conversation_ad_id = conversationObject.extractedLeadData.fb_ad_id;
        }
        else{
         console.log('conversation includes more than 4 messages so it most likely was not started from an ad, leaving conversation source as is')
        }
      }



      if (conversationRecord.fb_first_name !== null && conversationRecord.fb_first_name !== undefined) {
        existingConversation.fb_first_name = conversationRecord.fb_first_name;
        existingConversation.fb_last_name = conversationRecord.fb_last_name;
        existingConversation.fb_profile_pic = conversationRecord.fb_profile_pic;
       
      }else if (conversationRecord.ig_name !== null && conversationRecord.ig_name !== undefined) {
        existingConversation.ig_name = conversationRecord.ig_name;
        existingConversation.ig_username = conversationRecord.ig_username;
        existingConversation.ig_profile_pic = conversationRecord.ig_profile_pic;
        existingConversation.ig_follower_count = conversationRecord.ig_follower_count;
        existingConversation.ig_is_user_follow_business = conversationRecord.ig_is_user_follow_business;
        existingConversation.ig_is_business_follow_user = conversationRecord.ig_is_business_follow_user;
      }


      if (conversationObject.extractedContactData !== null && conversationObject.extractedContactData !== undefined && conversationObject.extractedContactData){
       // update the conversation record with the contact data
       existingConversation[conversationObject.extractedContactData.contactType] = conversationObject.extractedContactData.contactValue;
      }
      

      //update the conversation record with the new details
      const { data: updatedConversationData, error: updatedConversationError } = await this.supabase
        .from(appDbKeys.pageConversationsTable)
        .update(existingConversation)
        .eq('id', existingConversation.id)
        .select()
        .single();
    
      result = updatedConversationData;
    }
    else {
      console.log('convo does not exist, inserting new conversation...');

      if (conversationObject.extractedLeadData && conversationObject.extractedLeadData.lead_data && conversationObject.extractedLeadData.fb_ad_id !== null && conversationObject.extractedLeadData.fb_ad_id !== undefined) {
        conversationRecord.conversation_source = 'ad';
        conversationRecord.conversation_ad_id = conversationObject.extractedLeadData.fb_ad_id;
        if (conversationObject.extractedLeadData.is_foreign_ad) {
          conversationRecord.foreign_ad_id = conversationObject.extractedLeadData.fb_ad_id;
        }
      }else{conversationRecord.conversation_source = 'organic';}
      
      if (conversationObject.extractedContactData !== null && conversationObject.extractedContactData !== undefined && conversationObject.extractedContactData){
        // update the conversation record with the contact data
        conversationRecord[conversationObject.extractedContactData.contactType] = conversationObject.extractedContactData.contactValue;
      }
      
      const { data: newConversationData, error: newConversationError } = await this.supabase
        .from(appDbKeys.pageConversationsTable)
        .insert([conversationRecord])
        .select()
        .single();
        if (newConversationError) {
          console.error('Error inserting new conversation:', newConversationError);
          return null;
        }
        result = newConversationData;
    }
    // console.log('Conversation upserted successfully:', result);

    if (conversationObject.extractedLeadData && conversationObject.extractedLeadData !== null && conversationObject.extractedLeadData.lead_data !== null && conversationObject.extractedLeadData.fb_ad_id !== null && conversationObject.extractedLeadData.fb_ad_id !== undefined) {


      const leadData = {
        conversation_id: result.id,
        ...conversationObject.extractedLeadData
      };

      // upsert the lead ie ads interaction event
      // console.log("upserting lead2", leadData)
      const lead = await upsertLead(this.supabase, leadData);
      // console.log('lead upserted successfully in upsertConversation 2', lead)
      result = {...result, lead_id: lead[0].id, is_message_from_ad: true, welcome_message_flow: conversationObject.extractedLeadData.welcome_message_flow}
      
    }
    if (conversationObject.extractedContactData && conversationObject.extractedContactData !== null && conversationObject.extractedContactData !== undefined && conversationObject.extractedContactData){
          
      // console.log("result", result)
      const contactData = {
        conversationId: result.id,
        appId: conversationObject.appId,
        fbPageId: conversationObject.fbPageId,
        messageId: conversationObject.messageId,
        contactType: conversationObject.extractedContactData.contactType,
        contactValue: conversationObject.extractedContactData.contactValue,
        conversation_platform: conversationObject.conversationPlatform,
        recipientPageScopeId: conversationObject.recipientPageScopeId,
        source: 'messages',
        fbConversationId: conversationObject.fbConversationId,
        fbAdId: result.conversation_ad_id,
      };
      await upsertContact(this.supabase, contactData);
    }
    // await deleteCustomerUserData(this.supabase, conversationObject.recipientPageScopeId, conversationObject.fbPageId);
    return result;
  } catch (checkError) {
    console.error('Unexpected error in upsertConversation:', checkError);
    return null;
  }
  } 

  async upsertMessage(messageObject) {
    try {
      // Check if the message already exists
      const { data: existingMessage, error: checkError } = await this.supabase
        .from(appDbKeys.pageMessagesTable)
        .select('*')
        .eq('message_id', messageObject.messageId)
        .eq('message_type', messageObject.messageType)
        .eq('app_id', messageObject.appId)
        // .limit(1);
        .single();

      if (checkError && checkError.code !== 'PGRST116') {
        console.error('Error checking existing message:', checkError);
        return null;
      }
    
      const messageRecord = {
        app_id: messageObject.appId,
        sender_id: messageObject.senderId,
        recipient_id: messageObject.recipientId,
        message_type: messageObject.messageType,
        message_id: messageObject.messageId,
        message_content: messageObject.messageContent,
        message_attachment_type: messageObject.messageAttachmentsType,
        message_attachment_payload: messageObject.messageAttachmentsPayload,
        is_inbound: messageObject.isInbound,
        is_outbound: messageObject.isOutbound,
        outbound_origin: messageObject.outboundOrigin,
        json_body: messageObject.jsonReqBody
      };

      let result;
      if (existingMessage) {
        console.log('Updating existing message...');
        const { data, error } = await this.supabase
          .from(appDbKeys.pageMessagesTable)
          .update(messageRecord)
          .eq('id', existingMessage.id)
          .select()
          .single();

        if (error) {
          console.error('Error updating message:', error);
          return null;
        }
        result = data;
      } else {
        console.log('Inserting new message...');
        const { data, error } = await this.supabase
          .from(appDbKeys.pageMessagesTable)
          .insert([messageRecord])
          .select()
          .single();

        if (error) {
          console.error('Error inserting message:', error);
          return null;
        }
        result = data;
      }

      console.log('Message upserted successfully:', result);
      return result;
    } catch (error) {
      console.error('Unexpected error in upsertMessage:', error);
      return null;
    }
  }

  async upsertComment(commentObject) {
    try {
      // Check if the comment already exists
      const { data: existingComment, error: checkError } = await this.supabase
        .from(appDbKeys.pageCommentsTable)
        .select('*')
        .eq('app_id', commentObject.appId)
        .eq('fb_comment_id', commentObject.commentId)
        .eq('ig_comment_id', commentObject.igCommentId)
        .single();

      if (checkError && checkError.code !== 'PGRST116') {
        console.error('Error checking existing comment:', checkError);
        return null;
      }

      const commentRecord = {
        app_id: commentObject.appId,
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

      let result;
      if (existingComment) {
        console.log('Updating existing comment...');
        const { data, error } = await this.supabase
          .from(appDbKeys.pageCommentsTable)
          .update(commentRecord)
          .eq('id', existingComment.id)
          .select()
          .single();

        if (error) {
          console.error('Error updating comment:', error);
          return null;
        }
        result = data;
      } else {
        console.log('Inserting new comment...');
        const { data, error } = await this.supabase
          .from(appDbKeys.pageCommentsTable)
          .insert([commentRecord])
          .select()
          .single();

        if (error) {
          console.error('Error inserting comment:', error);
          return null;
        }
        result = data;
      }

      console.log('Comment upserted successfully:', result);
      return result;
    } catch (error) {
      console.error('Unexpected error in upsertComment:', error);
      return null;
    }
  }

  async readMessages(queryParams) {
    try {
      const { data, error } = await this.supabase
        .from(appDbKeys.pageMessagesTable)
        .select('*')
        .match(queryParams)
        .eq('app_id', config.appId)

      if (error) {
        console.error('Error reading messages:', error);
        // canceling statement due to statement timeout'
        return null; 
      }

      console.log('Messages read successfully:', data);
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

  async readComments(queryParams) {
    try {
      const { data, error } = await this.supabase
        .from(appDbKeys.pageCommentsTable)
        .select('*')
        .eq('app_id', config.appId)
        .match(queryParams);

      if (error) {
        console.error('Error reading comments:', error);
        return null; 
      }

      console.log('Comments read successfully:', data);
      return data;
    } catch (err) {
      console.error('Unexpected error in readComments:', err);
      return null; 
    }
  }
}









async function updateMessageSentToAudosServer(supabase, messageId) {
  console.log('marking message as sent to audos server in updateMessageSentToAudosServer messageId', messageId)
  try {
    const { data, error } = await supabase
      .from(appDbKeys.pageMessagesTable)
      .update({ sent_to_audos_server: true })
      .eq('id', messageId)
      .select()
      .single();
    if (error) {
      console.error('Error updating message sent to audos server:', error);
      return null;
    }
    return data;
  } catch (error) {
    console.error('Error updating message sent to audos server:', error);
    return null;
  }
}

async function updateCommentSentToAudosServer(supabase, commentId) {
  console.log('marking comment as sent to audos server in updateCommentSentToAudosServer commentId', commentId)
  try {
    const { data, error } = await supabase
      .from(appDbKeys.pageCommentsTable)
      .update({ sent_to_audos_server: true })
      .eq('id', commentId)
      .select()
      .single();
      
    if (error) {
      console.error('Error updating comment sent to audos server:', error);
      return null;
    }
    return data;
  } catch (error) {
    console.error('Error updating comment sent to audos server:', error);
    return null;
  }
}


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
    const result = await debugTokenViaMetaApi({accessToken: params.accessToken});
  } catch (error) {
    console.error('Error in debugTokenSynchronously:', config.flattenForLog(error));
    throw error;
  }
}
 
async function makeFbApiCallWithPageAccessTokenThatIsValid (params) {  
  // makeFbApiCall : 
  const {
    supabase,
    pageId,
    // userId,
    accessToken,
    requirements,
    apiCall,
    retryOnInvalidToken = true} = params;

  
    
    let response;
    try {

    response = await apiCall();
    // console.log('after api call was made', response)

  } catch (error) {
    console.log('makeFbApiCall errorx123 error.response',config.flattenForLog(error.response))

    const errorObject = config.handleFbErrors({error: error, serverNote: `Error in makeFbApiCall function: ${requirements.url? requirements.url.toString() : requirements.function_string.toString()}`});


    // check if the error is a token invalid error
    const isTokenInvalidError = error.response?.data?.error?.code === 190;
    if (isTokenInvalidError) {   
      console.log('makeFbApiCall this token is considered invalid, updating the is token valid field to false',accessToken)
    await updateIsTokenValid(accessToken, false, supabase);
    }


    // DO NOT DELETE COMMENTED CODE

      // If token invalid and retry enabled, get new token and retry
      // if (retryOnInvalidToken && pageId !== null && pageId !== undefined) {
      //   console.log('makeFbApiCall retryOnInvalidToken')
      //   let actualRequirements = requirements.requirementContext;
      //   const newToken = await getAPageAccessTokenThatIsValid(pageId, supabase);
      //   console.log('makeFbApiCall retryOnInvalidToken newToken', newToken)
      //   if (newToken && newToken !== accessToken) {
      //     console.log('makeFbApiCall retryOnInvalidToken newToken', newToken)
      //     console.log('makeFbApiCall retryOnInvalidToken accessToken', accessToken)

      //     config.flattenForLog(error)

      //     return makeFbApiCallWithPageAccessTokenThatIsValid({
      //       supabase,
      //       // userId,
      //       pageId,
      //       accessToken: newToken,
      //       requirements,
      //       apiCall,
      //       retryOnInvalidToken: false // Prevent infinite retries
      //     });

         

      //   }else{
      //     console.log('makeFbApiCall retryOnInvalidToken newToken and accessToken are the same')
      //     console.log('makeFbApiCall retryOnInvalidToken newToken and accessToken are the same:', JSON.stringify(error, null, 2))
      //     // Sentry.withScope( (scope) => {
      //     //   scope.setExtras({
      //     //     retryOnInvalidTokensTheSame: true,
      //     //   });
      //     // });
      //     config.flattenForLog(error)
      //     throw errorObject;
      //   }
      // }

      throw errorObject;
      }
    return response;
  }



async function makeFbApiCall (params) {  
  // makeFbApiCall : 
  const {
    supabase,
    pageId,
    // userId,
    accessToken,
    requirements,
    apiCall,
    retryOnInvalidToken = true} = params;


    // for now lets outsource this to the async version
    return await makeFbApiCallWithPageAccessTokenThatIsValid({
      // supabase,
      pageId,
      // userId,
      accessToken,
      requirements,
      apiCall,
      retryOnInvalidToken
    });
  }



  

async function logMetaApiCall(
supabase: any,
{
  // appId,
  userId = null,
  fbId,
  pageId,
  accessToken,
  accessTokenType,
  success,
  status,
  reqUrl,
  reqParams,
  res,
  requirementContext,
  errorCode,
  errorMessage
}: {
  // appId: string;
  userId: string;
  fbId: string;
  pageId: string;
  accessToken: string;
  accessTokenType: 'page' | 'user';
  success: boolean;
  status: string;
  reqUrl: string;
  reqParams: any;
  res: any;
  requirementContext: {needsMessaging: boolean, needsInstagram: boolean, needsAds: boolean, action: string};
  errorCode?: string;
  errorMessage?: string;
}
) {
  if (!supabase) {
    console.error('logMetaApiCall was called without a Supabase client instance.');
    config.flattenForLog(new Error('logMetaApiCall received no Supabase client.'))
    // Sentry.captureException(new Error('logMetaApiCall received no Supabase client.'));
    return;
  }
console.log("logMetaApiCall ....")
console.dir({userId, fbId, pageId, accessToken, accessTokenType, success, status, reqUrl, reqParams, res, requirementContext, errorCode, errorMessage},{depth: null})

// console.log('logMetaApiCall config', config)

// get user id from access token, find the fb_id and page_id from the access token and same app id
const { data: pageData, error: pageError } = await supabase
.from(appDbKeys.pageTable)
.select('id,fb_id')
.eq('page_access_token', accessToken)
.eq('app_id', config.appId)
.limit(1);
// .single();



async function getUserIdFromAccessToken(supabase, accessToken, pageData, pageError) {


if (!pageData || pageError || !pageData.fb_id) {
  // get user id from access token, find the fb_id and page_id from the access token and same app id
  console.log('No logMetaApiCall no pageData found in logMetaApiCall pageData:', pageError);
  console.dir({pageData, pageError},{depth: null})
  const { data: userData, error: userError } = await supabase
    .from(appDbKeys.userTable)
    .select('id,fb_id')
    .eq('user_access_token', accessToken)
    .eq('app_id', config.appId)
    .limit(1);
    // .single();

  return {userData, userError};  
}else{
  console.log('found pageData in logMetaApiCall getUserIdFromAccessToken pageData')
  console.dir({pageData},{depth: null})
  const { data: userData, error: userError } = await supabase
  .from(appDbKeys.userTable)
  .select('*')
  .eq('fb_id', pageData.fb_id)
  .eq('app_id', config.appId)
  .limit(1);

  // .single();

  console.log('getUserIdFromAccessToken userData and getUserIdFromAccessToken userError')
  console.dir({userData, userError},{depth: null})
  return {userData, userError};

}
}


const { userData, userError } = await getUserIdFromAccessToken(supabase, accessToken, pageData, pageError)

if (!userData || userError) {
  console.log('Error getting user data in logMetaApiCall:');
  console.dir({userError},{depth: null})
  // Sentry.captureException(userError);
}


try {
  const { data, error } = await supabase
    .from(appDbKeys.metaApiCallsResultsTable)
    .insert({
      app_id: config.appId,
      user_id: userData?.id? userData.id : null,
      fb_id: userData?.fb_id? userData.fb_id : null,
      page_id: pageData ? pageId : null,
      access_token: accessToken,
      access_token_type: !pageData ? 'user' : 'page',
      success,
      status,
      req_url: reqUrl,
      req_params: reqParams,
      res,
      requirement_context: requirementContext,
      error_code: errorCode,
      error_message: errorMessage
    }).select();

  if (error) {
    console.error('Error logging Meta API call:');
    console.dir({error},{depth: null})
  }
  console.log('logMetaApiCall returned data')
  console.dir({data},{depth: null})
  return data;
} catch (error) {
  console.error('Error in logMetaApiCall:');
  console.dir({error},{depth: null});
}
}

// --- END: User Data Deletion Functions ---



/**
 * Deletes all data associated with a client/vendor user.
 * This includes their user record, pages, conversations, messages, comments,
 * leads, contacts, and all ad-related data (accounts, campaigns, ad sets, ads, creatives).
 * @param supabase - Supabase client instance
 * @param userId - The database ID of the user in hosted_app_config_apps
 */
async function deleteClientUserData(supabase, userId: string) {
  console.log(`Starting deletion process for client user ID: ${userId}`);

  try {
    // 1. Get associated pages
    const { data: pages, error: pagesError } = await supabase
      .from(appDbKeys.pageTable)
      .select('id, fb_page_id')
      .eq('app_id', config.appId)
      .eq('fb_id', userId); // Assuming app_id in pages refers to the user's DB ID

    if (pagesError) throw new Error(`Error fetching pages: ${pagesError.message}`);
    const pageIds = pages?.map(p => p.id) || [];
    const fbPageIds = pages?.map(p => p.fb_page_id) || [];
    console.log(`Found ${pages?.length} pages associated with user ${userId}. FB Page IDs: ${fbPageIds.join(', ')}`);

    if (pages && pages.length > 0) {
      // 2. Get associated conversations
      const { data: conversations, error: convosError } = await supabase
        .from(appDbKeys.pageConversationsTable)
        .select('id')
        .in('fb_page_id', fbPageIds)
        .eq('app_id', userId); // Ensure conversations belong to this user's app instance

      if (convosError) throw new Error(`Error fetching conversations: ${convosError.message}`);
      const conversationIds = conversations?.map(c => c.id) || [];
      console.log(`Found ${conversations?.length} conversations.`);

      if (conversationIds.length > 0) {
        // 3. Delete Messages, Leads, Contacts associated with conversations
        console.log(`Deleting messages for ${conversationIds.length} conversations...`);
        const { error: msgError } = await supabase.from(appDbKeys.pageMessagesTable).delete().in('conversation_id', conversationIds);
        if (msgError) console.error(`Error deleting messages: ${msgError.message}`); // Log error but continue

        console.log(`Deleting leads for ${conversationIds.length} conversations...`);
        const { error: leadError } = await supabase.from(appDbKeys.leadsTable).delete().in('conversation_id', conversationIds);
        if (leadError) console.error(`Error deleting leads: ${leadError.message}`); // Log error but continue

        console.log(`Deleting contacts for ${conversationIds.length} conversations...`);
        const { error: contactError } = await supabase.from(appDbKeys.contactsTable).delete().in('conversation_id', conversationIds);
        if (contactError) console.error(`Error deleting contacts: ${contactError.message}`); // Log error but continue

        // 4. Delete Conversations
        console.log(`Deleting ${conversationIds.length} conversations...`);
        const { error: convoDelError } = await supabase.from(appDbKeys.pageConversationsTable).delete().in('id', conversationIds);
        if (convoDelError) console.error(`Error deleting conversations: ${convoDelError.message}`); // Log error but continue
      }

      // 5. Delete Comments associated with pages
      console.log(`Deleting comments for FB Page IDs: ${fbPageIds.join(', ')}...`);
      const { error: commentError } = await supabase.from(appDbKeys.pageCommentsTable).delete().in('fb_page_id', fbPageIds);
      if (commentError) console.error(`Error deleting comments: ${commentError.message}`); // Log error but continue

      // 6. Delete Pages
      console.log(`Deleting ${pageIds.length} pages...`);
      const { error: pageDelError } = await supabase.from(appDbKeys.pageTable).delete().in('id', pageIds);
      if (pageDelError) console.error(`Error deleting pages: ${pageDelError.message}`); // Log error but continue
    }

    // 7. Delete Ad Creatives
    console.log(`Deleting ad creatives for user ID: ${userId}...`);
    const { error: creativeError } = await supabase.from(appDbKeys.adCreativesTable).delete().eq('user_id', userId);
    if (creativeError) console.error(`Error deleting ad creatives: ${creativeError.message}`);

    // 8. Delete Ads
    console.log(`Deleting ads for user ID: ${userId}...`);
    const { error: adError } = await supabase.from(appDbKeys.adsTable).delete().eq('user_id', userId);
    if (adError) console.error(`Error deleting ads: ${adError.message}`);

    // 9. Delete Ad Sets
    console.log(`Deleting ad sets for user ID: ${userId}...`);
    const { error: adSetError } = await supabase.from(appDbKeys.adSetsTable).delete().eq('user_id', userId);
    if (adSetError) console.error(`Error deleting ad sets: ${adSetError.message}`);

    // 10. Delete Campaigns
    console.log(`Deleting campaigns for user ID: ${userId}...`);
    const { error: campaignError } = await supabase.from(appDbKeys.campaignsTable).delete().eq('user_id', userId);
    if (campaignError) console.error(`Error deleting campaigns: ${campaignError.message}`);

    // 11. Delete Ad Accounts
    console.log(`Deleting ad accounts for user ID: ${userId}...`);
    const { error: adAccountError } = await supabase.from(appDbKeys.adAccountsTable).delete().eq('user_id', userId);
    if (adAccountError) console.error(`Error deleting ad accounts: ${adAccountError.message}`);

    // 12. Delete User Record
    console.log(`Deleting user record for ID: ${userId}...`);
    const { error: userError } = await supabase.from(appDbKeys.userTable).delete().eq('id', userId);
    if (userError) throw new Error(`Error deleting user record: ${userError.message}`);

    console.log(`Successfully deleted all data for client user ID: ${userId}`);
    return { success: true, message: `Successfully deleted user ${userId}` };

  } catch (error) {
    console.error(`Error during deletion process for client user ID ${userId}:`, error);
    return { success: false, message: error.message };
  }
}



/**
 * Deletes data associated with a specific customer (identified by PSID) for a specific page.
 * This includes their conversations, messages, comments, leads, and contacts related *only* to that PSID and Page combination.
 * @param supabase - Supabase client instance
 * @param customerPsid - The Page-Scoped ID of the customer.
 * @param fbPageId - The Facebook Page ID the customer interacted with.
 */
async function deleteCustomerUserData(supabase, customerPsid: string, fbPageId: string) {
  console.log(`Starting deletion process for customer PSID: ${customerPsid} on page: ${fbPageId}`);

  try {
    // 1. Find conversations involving this customer PSID and Page ID
    const { data: conversations, error: convosError } = await supabase
      .from(appDbKeys.pageConversationsTable)
      .select('id')
      .eq('app_id', config.appId)
      .eq('recipient_page_scope_id', customerPsid)
      .eq('fb_page_id', fbPageId);

    if (convosError) throw new Error(`Error fetching conversations: ${convosError.message}`);
    console.log("conversations", conversations)
    const conversationIds = conversations?.map(c => c.id) || [];
    console.log(`Found ${conversations?.length} conversations for PSID ${customerPsid} on page ${fbPageId}.`);

    if (conversationIds.length > 0) {
      // 2. Delete Messages where the customer is sender or recipient within these conversations
      console.log(`Deleting messages involving PSID ${customerPsid} in ${conversationIds.length} conversations...`);
      // We delete based on conversation_id to ensure we only remove messages from the relevant threads
      const { data: messgData, error: msgError } = await supabase
        .from(appDbKeys.pageMessagesTable)
        .select('*')
        .or(`sender_id.eq.${customerPsid},recipient_id.eq.${customerPsid}`) // Double-check this logic if needed
        .eq('app_id', config.appId);
        if (msgError) console.error(`Error deleting messages: ${msgError.message}`);

      // console.log("messgData", messgData)
      console.log("messgData.length", messgData.length)

      // 3. Delete Leads associated with these conversations
      console.log(`Deleting leads for ${conversationIds.length} conversations...`);
      const { data: leadData, error: leadError } = await supabase.from(appDbKeys.leadsTable).select('*').in('conversation_id', conversationIds);
      if (leadError) console.error(`Error deleting leads: ${leadError.message}`);

      console.log("leadData.length", leadData.length)

      // 4. Delete Contacts associated with these conversations
      console.log(`Deleting contacts for ${conversationIds.length} conversations...`);
      const { data: contactData, error: contactError } = await supabase.from(appDbKeys.contactsTable).select('*').in('conversation_id', conversationIds);
      if (contactError) console.error(`Error deleting contacts: ${contactError.message}`);

      console.log("contactData.length", contactData.length)

      // 5. Delete Conversations themselves
      console.log(`Deleting ${conversationIds.length} conversations...`); 
      const { data: convoData, error: convoDelError } = await supabase.from(appDbKeys.pageConversationsTable).select('*').in('id', conversationIds);
      if (convoDelError) console.error(`Error deleting conversations: ${convoDelError.message}`);

      console.log("convoData.length", convoData.length)
    }

    // 6. Delete Comments made by this customer PSID on this page
    // Assuming sender_id for comments stores the PSID for FB/IG comments
    console.log(`Deleting comments from PSID ${customerPsid} on page ${fbPageId}...`);
    const { data: commentData, error: commentError } = await supabase
      .from(appDbKeys.pageCommentsTable)
      .select('*')
      .eq('app_id', config.appId)
      .eq('sender_id', customerPsid)
      .eq('fb_page_id', fbPageId); // Ensure comments are for the correct page
    if (commentError) console.error(`Error deleting comments: ${commentError.message}`);

    console.log("commentData.length", commentData.length)

    console.log(`Successfully deleted data for customer PSID: ${customerPsid} on page: ${fbPageId}`);
    return { success: true, message: `Successfully deleted data for customer ${customerPsid} on page ${fbPageId}` };

  } catch (error) {
    console.error(`Error during deletion process for customer PSID ${customerPsid} on page ${fbPageId}:`, error);
    return { success: false, message: error.message };
  }
}



/**
 * Fetch all assets (pages, messages, comments, conversations, ad accounts, campaigns, ad sets, ads, ad creatives, leads, contacts)
 * owned by a user (fb_id) for a given app_id, across all related tables.
 * @param {string} fb_id - The Facebook user ID
 * @param {number} app_id - The App ID
 * @param {Object} pagination - Pagination parameters
 * @param {number} pagination.page - Page number (1-based)
 * @param {number} pagination.pageSize - Number of items per page
 * @returns {Promise<object>} All related assets grouped by type, with pagination info
 */


async function getAllUserAssets(fb_id, app_id, pagination = { page: 1, pageSize: 10 }) {
  const { page, pageSize } = pagination;
  const start = (page - 1) * pageSize;
  const end = start + pageSize - 1;

  try {
    // 1. Find all pages for the user/app
    const { data: pages, error: pagesError } = await supabase
      .from(appDbKeys.pageTable)
      .select('*')
      .eq('app_id', app_id)
      .eq('fb_id', fb_id)
      .range(start, end);

    // Get total count separately
    const { count: pagesCount, error: countError } = await supabase
      .from(appDbKeys.pageTable)
      .select('*', { count: 'exact', head: true })
      .eq('app_id', app_id)
      .eq('fb_id', fb_id);

    console.log("pagesCount", pagesCount)
    console.log("pages.length", pages.length)


    const [
      { data: accessTokens, count: accessTokensCount },
      { data: fbApiCalls, count: fbApiCallsCount }
    ] = await Promise.all([
      supabase.from(appDbKeys.accessTokensTable).select('*', { count: 'exact' }).eq('app_id', app_id).eq('fb_id', fb_id),
      supabase.from(appDbKeys.metaApiCallsResultsTable).select('*', { count: 'exact' }).eq('app_id', app_id).eq('fb_id', fb_id)
    ]);

    if (pagesError || countError) throw pagesError || countError;
    
    if (!pages || pages.length === 0) {
      return { 
        pages: [], 
        assets: {}, 
        adAccounts: [], 
        adAssets: {},
        pagination: {
          totalPages: 0,
          currentPage: page,
          pageSize,
          totalItems: 0
        }
      };
    }

    // 2. For each page, fetch related assets
    const assets = {};
    let count = 0;
    for (const page of pages) {
      const fb_page_id = page.fb_page_id;
      count++;
      console.log("count-", count)
      // Messages (sender or recipient is the page)
      const [
        { data: sentMessages, count: sentCount },
        { data: receivedMessages, count: receivedCount },
        { data: comments, count: commentsCount },
        { data: conversations, count: conversationsCount },
        { data: leads, count: leadsCount },
        { data: contacts, count: contactsCount },
       
      ] = await Promise.all([
        // Sent messages
        supabase
          .from(appDbKeys.pageMessagesTable)
          .select('*', { count: 'exact' })
          .eq('app_id', app_id)
          .eq('sender_id', fb_page_id)
          .range(start, end),

        // Received messages  
        supabase
          .from(appDbKeys.pageMessagesTable)
          .select('*', { count: 'exact' })
          .eq('app_id', app_id)
          .eq('recipient_id', fb_page_id)
          .range(start, end),

        // Comments
        supabase
          .from(appDbKeys.pageCommentsTable)
          .select('*', { count: 'exact' })
          .eq('fb_page_id', fb_page_id)
          .range(start, end),

        // Conversations
        supabase
          .from(appDbKeys.pageConversationsTable)
          .select('*', { count: 'exact' })
          .eq('app_id', app_id)
          .eq('fb_page_id', fb_page_id)
          .range(start, end),

        // Leads
        supabase
          .from(appDbKeys.leadsTable)
          .select('*', { count: 'exact' })
          .eq('fb_page_id', fb_page_id)
          .range(start, end),

        // Contacts
        supabase
          .from(appDbKeys.contactsTable)
          .select('*', { count: 'exact' })
          .eq('fb_page_id', fb_page_id)
          .range(start, end)
      ]);

      assets[fb_page_id] = {
        sentMessages: {
          data: sentMessages.map(m => m.id) || [],
          total: sentCount || 0,
          pages: Math.ceil((sentCount || 0) / pageSize)
        },
        receivedMessages: {
          data: receivedMessages.map(m => m.id) || [],
          total: receivedCount || 0,
          pages: Math.ceil((receivedCount || 0) / pageSize)
        },
        comments: {
          data: comments.map(c => c.id) || [],
          total: commentsCount || 0,
          pages: Math.ceil((commentsCount || 0) / pageSize)
        },
        conversations: {
          data: conversations.map(c => c.id) || [],
          total: conversationsCount || 0,
          pages: Math.ceil((conversationsCount || 0) / pageSize)
        },
        leads: {
          data: leads.map(l => l.id) || [],
          total: leadsCount || 0,
          pages: Math.ceil((leadsCount || 0) / pageSize)
        },
        contacts: {
          data: contacts.map(c => c.id) || [],
          total: contactsCount || 0,
          pages: Math.ceil((contactsCount || 0) / pageSize)
        },
        accessTokens: {
          data: accessTokens.map(a => a.id) || [],
          total: accessTokensCount || 0,
          pages: Math.ceil((accessTokensCount || 0) / pageSize)
        },
        fbApiCalls: {
          data: fbApiCalls.map(f => f.id) || [],
          total: fbApiCallsCount || 0,
          pages: Math.ceil((fbApiCallsCount || 0) / pageSize)
        }
      };
    }

    // 3. Find user's internal id in apps table
    const { data: userApp } = await supabase
      .from(appDbKeys.userTable)
      .select('id')
      .eq('fb_id', fb_id)
      .eq('app_id', app_id)
      .single();

    if (!userApp) {
      return { 
        pages: { 
          data: pages, 
          total: pagesCount, 
          pages: Math.ceil(pagesCount / pageSize) 
        }, 
        assets, 
        adAccounts: [], 
        adAssets: {},
        pagination: {
          totalPages: Math.ceil(pagesCount / pageSize),
          currentPage: page,
          pageSize,
          totalItems: pagesCount
        }
      };
    }

    const user_id = userApp.id;

    // 4. Fetch ad accounts and related assets
    const { data: adAccounts } = await supabase
      .from(appDbKeys.adAccountsTable)
      .select('*')
      .eq('user_id', user_id)
      .range(start, end);

    const { count: adAccountsCount } = await supabase
      .from(appDbKeys.adAccountsTable)
      .select('*', { count: 'exact', head: true })
      .eq('user_id', user_id);

    const adAssets = {};
    let adCount = 0;
    for (const adAccount of adAccounts || []) {
      const ad_account_id = adAccount.fb_ad_account_id;
      adCount++;
      console.log("adCount-", adCount)
      const [
        { data: campaigns, count: campaignsCount },
        { data: adSets, count: adSetsCount },
        { data: ads, count: adsCount },
        { data: adCreatives, count: adCreativesCount }
      ] = await Promise.all([
        // Campaigns
        supabase
          .from(appDbKeys.campaignsTable)
          .select('*', { count: 'exact' })
          .eq('ad_account_id', ad_account_id)
          .range(start, end),

        // Ad Sets
        supabase
          .from(appDbKeys.adSetsTable)
          .select('*', { count: 'exact' })
          .eq('ad_account_id', ad_account_id)
          .range(start, end),

        // Ads
        supabase
          .from(appDbKeys.adsTable)
          .select('*', { count: 'exact' })
          .eq('ad_account_id', ad_account_id)
          .range(start, end),

        // Ad Creatives
        supabase
          .from(appDbKeys.adCreativesTable)
          .select('*', { count: 'exact' })
          .eq('ad_account_id', ad_account_id)
          .range(start, end)
      ]);

      adAssets[ad_account_id] = {
        campaigns: {
          data: campaigns || [],
          total: campaignsCount || 0,
          pages: Math.ceil((campaignsCount || 0) / pageSize)
        },
        adSets: {
          data: adSets || [],
          total: adSetsCount || 0,
          pages: Math.ceil((adSetsCount || 0) / pageSize)
        },
        ads: {
          data: ads || [],
          total: adsCount || 0,
          pages: Math.ceil((adsCount || 0) / pageSize)
        },
        adCreatives: {
          data: adCreatives || [],
          total: adCreativesCount || 0,
          pages: Math.ceil((adCreativesCount || 0) / pageSize)
        }
      };
    }
    const returnableObject = {
      pages: {
        // data: pages,
        total: pagesCount,
        pages: Math.ceil(pagesCount / pageSize)
      },
      assets,
      adAccounts: {
        // data: adAccounts || [],
        total: adAccountsCount || 0,
        pages: Math.ceil((adAccountsCount || 0) / pageSize)
      },
      adAssets,
      pagination: {
        totalPages: Math.ceil(pagesCount / pageSize),
        currentPage: page,
        pageSize,
        totalItems: pagesCount
      }
    };
    console.log("getAllUserAssets returnableObject", returnableObject)
    return returnableObject;
  } catch (error) {
    console.error('Error fetching user assets:', error);
    throw error;
  }
}





// Wrapper to get user profile only if it hasn't been fetched before
async function getUserProfileIfNeeded(senderId: string, pageAccessToken: string, platform: string, fbPageId: string, fbConversationId: string, supabase: any) {
  try {
    // First check if we already have profile data for this conversation
    const { data: existingConversation, error: checkError } = await supabase
      .from(appDbKeys.pageConversationsTable)
      .select('fb_first_name, fb_last_name, fb_profile_pic, ig_name, ig_username, ig_profile_pic, ig_follower_count, ig_is_user_follow_business, ig_is_business_follow_user')
      .eq('app_id', config.appId)
      .eq('fb_page_id', fbPageId)
      .eq('fb_conversation_id', fbConversationId)
      .eq('recipient_page_scope_id', senderId)
      .single();

    if (checkError && checkError.code !== 'PGRST116') {
      console.error('Error checking existing conversation profile:', checkError);
      // Continue to fetch profile if we can't check
    }

    // Check if we already have profile data
    const hasProfileData = existingConversation && (
      (platform === 'facebook' && (existingConversation.fb_first_name || existingConversation.fb_last_name)) ||
      (platform === 'instagram' && (existingConversation.ig_name || existingConversation.ig_username))
    );

    if (hasProfileData) {
      console.log(`Profile data already exists for ${platform} user ${senderId}, skipping API call`);
      
      // Return existing profile data in the expected format
      if (platform === 'facebook') {
        return {
          firstName: existingConversation.fb_first_name,
          lastName: existingConversation.fb_last_name,
          profilePic: existingConversation.fb_profile_pic
        };
      } else {
        return {
          name: existingConversation.ig_name,
          username: existingConversation.ig_username,
          profilePic: existingConversation.ig_profile_pic,
          followerCount: existingConversation.ig_follower_count,
          isUserFollowBusiness: existingConversation.ig_is_user_follow_business,
          isBusinessFollowUser: existingConversation.ig_is_business_follow_user
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

/**
 * Activates a page with the best available configuration and deactivates other pages with the same fb_page_id
 * @param {Object} supabase - Supabase client instance
 * @param {string} appId - Application ID
 * @param {Object} targetPage - The page to be activated
 * @returns {Promise<Array|null>} - Returns the updated page data or null if error
 */
const activatePageWithBestConfig = async (supabase, appId, targetPage) => {
  try {
    // Get all other pages with the same fb_page_id (excluding the current one) to find the best candidate
    const { data: allOtherPages, error: allOtherPagesError } = await supabase
      .from(appDbKeys.pageTable)
      .select('config_id,id,fb_page_id,ig_account_id')
      .eq('app_id', appId)
      .eq('fb_page_id', targetPage.fb_page_id)
      .order('created_at', { ascending: false })
      .neq('id', targetPage.id);

    if (allOtherPagesError) {
      console.error('Error fetching other pages:', allOtherPagesError);
      return null;
    }

    const { data: allOtherPagesIg, error: allOtherPagesIgError } = await supabase
    .from(appDbKeys.pageTable)
    .select('config_id,id,fb_page_id,ig_account_id')
    .eq('app_id', appId)
    .eq('ig_account_id', targetPage.ig_account_id)
    .order('created_at', { ascending: false })
    .neq('id', targetPage.id)
    // .or(`fb_page_id.eq.${targetPage.fb_page_id},ig_account_id.eq.${targetPage.ig_account_id}`)
    .neq('fb_page_id', targetPage.fb_page_id);

  if (allOtherPagesIgError) {
    console.error('Error fetching other pages:', allOtherPagesIgError);
    return null;
  }

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
    
    let debugTokenResult = await debugTokenViaMetaApi({accessToken: targetPage.access_token});
   
       if((debugTokenResult.error && debugTokenResult.error.message) ||(!debugTokenResult.is_valid)){

        updateData.is_token_valid = false;
        updateData.token_debug_result = debugTokenResult;

    }else{
      updateData.is_token_valid = true;
      updateData.token_debug_result = debugTokenResult;
    }
 
    const { data: activatedPage, error: activationError } = await supabase
      .from(appDbKeys.pageTable)
      .update(updateData)
      .eq('id', targetPage.id)
      .select();
      
    if (activationError) {
      console.error('Error activating target page:', activationError);
      return null;
    }

    // Deactivate all other pages with the same fb_page_id
    if (allOtherPages && allOtherPages.length > 0) {
      for (const page of allOtherPages) {
        console.log('Setting page to inactive:', page.id);
        
        const { error: deactivationError } = await supabase
          .from(appDbKeys.pageTable)
          .update({ active: false })
          .eq('app_id', appId)
          .eq('fb_page_id', page.fb_page_id)
          .eq('id', page.id);
      
        if (deactivationError) {
          console.error('Error setting page to inactive:', deactivationError);
        }
      }
    }

    // set the ig_account_id to null and has_ig_page to false for all the other pages
    if (allOtherPagesIg && allOtherPagesIg.length > 0) {
      for (const page of allOtherPagesIg) {
        const { error: deactivationError } = await supabase
          .from(appDbKeys.pageTable)
          .update({ ig_account_id: '', has_ig_page: false })
          .eq('app_id', appId)
          .eq('ig_account_id', page.ig_account_id)
          .eq('id', page.id);
        if (deactivationError) {
          console.error('Error setting page to inactive:', deactivationError);
        }
      }
    }

    
    return activatedPage;
    
  } catch (error) {
    console.error('Unexpected error in activatePageWithBestConfig:', error);
    return null;
  }
};
