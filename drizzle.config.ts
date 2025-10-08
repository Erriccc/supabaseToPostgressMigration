import { defineConfig } from "drizzle-kit";

export default defineConfig({
  dialect: "postgresql",
  schema: "./src/db/migrations/schema.ts",
  out: "./src/db/migrations",
  introspect: {
    casing: "preserve", // This is the key setting for camel case
  },
  dbCredentials: {
    url: process.env.POSTGRES_CONNECTION_STRING!,
  },
  schemaFilter: ["audos"], 
  tablesFilter: [
    "hosted_app_config_apps",
    "hosted_app_config_app_pages",
    "hosted_app_config_app_business_managers",
    "hosted_app_config_app_messages",
    "hosted_app_config_app_comments",
    "hosted_app_config_app_conversations",
    "hosted_app_config_access_tokens",
    "hosted_app_config_meta_api_calls_results",
    "hosted_app_config_ad_accounts",
    "hosted_app_config_ad_campaigns",
    "hosted_app_config_ad_sets",
    "hosted_app_config_ads",
    "hosted_app_config_ad_creatives",
    "hosted_app_config_app_ad_media",
    "hosted_app_config_app_leads",
    "hosted_app_config_app_contacts",
  ],
});
