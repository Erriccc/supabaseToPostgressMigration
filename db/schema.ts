import { pgTable, pgSchema, index, bigint, text, timestamp, boolean, foreignKey, varchar, jsonb, smallint, unique, date } from "drizzle-orm/pg-core"
import { sql } from "drizzle-orm"

export const db = pgSchema("audos");

export const hosted_app_config_app_messagesInAudos = db.table("hosted_app_config_app_messages", {
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	id: bigint({ mode: "number" }).primaryKey().generatedByDefaultAsIdentity({ name: "db.hosted_app_config_app_messages_id_seq", startWith: 1, increment: 1, minValue: 1, maxValue: 9223372036854775807, cache: 1 }),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	app_id: bigint({ mode: "number" }),
	sender_id: text(),
	recipient_id: text(),
	message_type: text(),
	message_id: text(),
	message_content: text(),
	message_attachment_type: text(),
	message_attachment_payload: text(),
	json_body: text(),
	created_at: timestamp({ withTimezone: true, mode: 'string' }).defaultNow().notNull(),
	is_inbound: boolean().default(false),
	is_outbound: boolean().default(false),
	outbound_origin: text(),
	welcome_message_flow: text(),
	welcome_message: text(),
	updated_at: timestamp({ withTimezone: true, mode: 'string' }),
	sent_to_audos_server: boolean(),
}, (table) => [
	index("hosted_app_config_app_messages_app_id_sender_id_recipient_id").using("btree", table.app_id.asc().nullsLast().op("int8_ops"), table.sender_id.asc().nullsLast().op("int8_ops"), table.recipient_id.asc().nullsLast().op("text_ops")),
	index("hosted_app_config_app_messages_message_id_app_id_idx").using("btree", table.message_id.asc().nullsLast().op("text_ops"), table.app_id.asc().nullsLast().op("text_ops")),
	index("hosted_app_config_app_messages_message_id_idx").using("btree", table.message_id.asc().nullsLast().op("text_ops")),
	index("hosted_app_config_app_messages_message_id_message_type_idx").using("btree", table.message_id.asc().nullsLast().op("text_ops"), table.message_type.asc().nullsLast().op("text_ops")),
	index("hosted_app_config_app_messages_sender_id_recipient_id").using("btree", table.sender_id.asc().nullsLast().op("text_ops"), table.recipient_id.asc().nullsLast().op("text_ops")),
	index("idx_app_messages_app_sender_recipient_id").using("btree", table.app_id.asc().nullsLast().op("int8_ops"), table.sender_id.asc().nullsLast().op("int8_ops"), table.recipient_id.asc().nullsLast().op("text_ops"), table.id.asc().nullsLast().op("text_ops")),
	index("idx_app_messages_msgid_type_app").using("btree", table.message_id.asc().nullsLast().op("int8_ops"), table.message_type.asc().nullsLast().op("text_ops"), table.app_id.asc().nullsLast().op("text_ops")),
	index("idx_hosted_app_config_app_messages_id").using("btree", table.id.asc().nullsLast().op("int8_ops")),
]);

export const hosted_app_config_app_commentsInAudos = db.table("hosted_app_config_app_comments", {
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	id: bigint({ mode: "number" }).primaryKey().generatedByDefaultAsIdentity({ name: "db.hosted_app_config_comments_id_seq", startWith: 1, increment: 1, minValue: 1, maxValue: 9223372036854775807, cache: 1 }),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	app_id: bigint({ mode: "number" }),
	sender_id: text(),
	recipient_id: text(),
	post_id: text(),
	media_id: text(),
	comment_value: text(),
	platform: text(),
	fb_page_id: text(),
	json_body: text(),
	created_at: timestamp({ withTimezone: true, mode: 'string' }).defaultNow().notNull(),
	fb_comment_id: text(),
	ig_comment_id: text(),
	is_inbound: boolean().default(false),
	is_outbound: boolean().default(false),
	outbound_origin: text(),
	sent_to_audos_server: boolean(),
}, (table) => [
	index("hosted_app_config_app_comments_app_id_idx").using("btree", table.app_id.asc().nullsLast().op("int8_ops")),
]);

export const hosted_app_config_ad_accountsInAudos = db.table("hosted_app_config_ad_accounts", {
	fb_ad_account_id: varchar({ length: 255 }).primaryKey().notNull(),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	app_id: bigint({ mode: "number" }).notNull(),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	user_id: bigint({ mode: "number" }),
	name: varchar({ length: 255 }).notNull(),
	details: jsonb(),
	created_at: timestamp({ withTimezone: true, mode: 'string' }).default(sql`CURRENT_TIMESTAMP`),
	updated_at: timestamp({ withTimezone: true, mode: 'string' }).default(sql`CURRENT_TIMESTAMP`),
}, (table) => [
	index("hosted_app_config_ad_accounts_user_id_idx").using("btree", table.user_id.asc().nullsLast().op("int8_ops")),
	foreignKey({
			columns: [table.user_id],
			foreignColumns: [hosted_app_config_appsInAudos.id],
			name: "hosted_app_config_ad_accounts_user_id_fkey"
		}).onDelete("cascade"),
]);

export const hosted_app_config_appsInAudos = db.table("hosted_app_config_apps", {
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	id: bigint({ mode: "number" }).primaryKey().generatedByDefaultAsIdentity({ name: "db.hosted_app_config_apps_id_seq", startWith: 1, increment: 1, minValue: 1, maxValue: 9223372036854775807, cache: 1 }),
	config_ids: varchar(),
	app_env: text(),
	fb_id: varchar(),
	user_access_token: text(),
	created_at: timestamp({ withTimezone: true, mode: 'string' }).defaultNow().notNull(),
	app_id: text(),
	email: varchar(),
	ad_config_ids: varchar(),
	new_pages: jsonb(),
	missing_pages: jsonb(),
	is_admin: boolean().default(false).notNull(),
	is_token_valid: boolean(),
	token_debug_result: jsonb(),
	has_ads: boolean(),
	is_synching_user_account: boolean(),
}, (table) => [
	index("hosted_app_config_app_ad_config_ids_idx").using("btree", table.ad_config_ids.asc().nullsLast().op("text_ops")),
	index("hosted_app_config_app_config_ids_idx").using("btree", table.config_ids.asc().nullsLast().op("text_ops")),
]);

export const hosted_app_config_ad_campaignsInAudos = db.table("hosted_app_config_ad_campaigns", {
	fb_campaign_id: varchar({ length: 255 }).primaryKey().notNull(),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	app_id: bigint({ mode: "number" }).notNull(),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	user_id: bigint({ mode: "number" }),
	ad_account_id: varchar({ length: 255 }),
	name: varchar({ length: 255 }).notNull(),
	objective: varchar({ length: 50 }).notNull(),
	status: varchar({ length: 50 }).notNull(),
	details: jsonb(),
	created_at: timestamp({ withTimezone: true, mode: 'string' }).default(sql`CURRENT_TIMESTAMP`),
	updated_at: timestamp({ withTimezone: true, mode: 'string' }).default(sql`CURRENT_TIMESTAMP`),
}, (table) => [
	index("hosted_app_config_ad_campaigns_ad_account_id_idx").using("btree", table.ad_account_id.asc().nullsLast().op("text_ops")),
	index("hosted_app_config_ad_campaigns_user_id_idx").using("btree", table.user_id.asc().nullsLast().op("int8_ops")),
	foreignKey({
			columns: [table.ad_account_id],
			foreignColumns: [hosted_app_config_ad_accountsInAudos.fb_ad_account_id],
			name: "hosted_app_config_ad_campaigns_ad_account_id_fkey"
		}).onDelete("cascade"),
	foreignKey({
			columns: [table.user_id],
			foreignColumns: [hosted_app_config_appsInAudos.id],
			name: "hosted_app_config_ad_campaigns_user_id_fkey"
		}).onDelete("cascade"),
]);

export const hosted_app_config_ad_setsInAudos = db.table("hosted_app_config_ad_sets", {
	fb_ad_set_id: varchar({ length: 255 }).primaryKey().notNull(),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	app_id: bigint({ mode: "number" }).notNull(),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	user_id: bigint({ mode: "number" }),
	campaign_id: varchar({ length: 255 }),
	name: varchar({ length: 255 }).notNull(),
	optimization_goal: varchar({ length: 50 }).notNull(),
	billing_event: varchar({ length: 50 }).notNull(),
	bid_strategy: varchar({ length: 50 }),
	details: jsonb(),
	created_at: timestamp({ withTimezone: true, mode: 'string' }).default(sql`CURRENT_TIMESTAMP`),
	updated_at: timestamp({ withTimezone: true, mode: 'string' }).default(sql`CURRENT_TIMESTAMP`),
	status: varchar(),
	archived: boolean().default(false).notNull(),
}, (table) => [
	index("hosted_app_config_ad_sets_campaign_id_idx").using("btree", table.campaign_id.asc().nullsLast().op("text_ops")),
	index("hosted_app_config_ad_sets_user_id_campaign_id_idx").using("btree", table.user_id.asc().nullsLast().op("int8_ops"), table.campaign_id.asc().nullsLast().op("int8_ops")),
	foreignKey({
			columns: [table.campaign_id],
			foreignColumns: [hosted_app_config_ad_campaignsInAudos.fb_campaign_id],
			name: "hosted_app_config_ad_sets_campaign_id_fkey"
		}).onDelete("cascade"),
	foreignKey({
			columns: [table.user_id],
			foreignColumns: [hosted_app_config_appsInAudos.id],
			name: "hosted_app_config_ad_sets_user_id_fkey"
		}).onDelete("cascade"),
]);

export const hosted_app_config_ad_creativesInAudos = db.table("hosted_app_config_ad_creatives", {
	fb_creative_id: varchar({ length: 255 }).primaryKey().notNull(),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	app_id: bigint({ mode: "number" }).notNull(),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	user_id: bigint({ mode: "number" }),
	ad_account_id: varchar({ length: 255 }),
	name: varchar({ length: 255 }).notNull(),
	object_story_spec: jsonb().notNull(),
	details: jsonb(),
	created_at: timestamp({ withTimezone: true, mode: 'string' }).default(sql`CURRENT_TIMESTAMP`),
	updated_at: timestamp({ withTimezone: true, mode: 'string' }).default(sql`CURRENT_TIMESTAMP`),
	audos_ad_trace_id: text(),
	welcome_message_id: varchar(),
	image_url: text(),
	video_url: text(),
}, (table) => [
	index("hosted_app_config_ad_creatives_ad_account_id_idx").using("btree", table.ad_account_id.asc().nullsLast().op("text_ops")),
	index("hosted_app_config_ad_creatives_user_id_idx").using("btree", table.user_id.asc().nullsLast().op("int8_ops")),
	foreignKey({
			columns: [table.ad_account_id],
			foreignColumns: [hosted_app_config_ad_accountsInAudos.fb_ad_account_id],
			name: "hosted_app_config_ad_creatives_ad_account_id_fkey"
		}).onDelete("cascade"),
	foreignKey({
			columns: [table.user_id],
			foreignColumns: [hosted_app_config_appsInAudos.id],
			name: "hosted_app_config_ad_creatives_user_id_fkey"
		}).onDelete("cascade"),
]);

export const hosted_app_config_app_pagesInAudos = db.table("hosted_app_config_app_pages", {
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	id: bigint({ mode: "number" }).primaryKey().generatedByDefaultAsIdentity({ name: "db.hosted_app_config_app_pages_id_seq", startWith: 1, increment: 1, minValue: 1, maxValue: 9223372036854775807, cache: 1 }),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	app_id: bigint({ mode: "number" }),
	fb_id: varchar(),
	created_at: timestamp({ withTimezone: true, mode: 'string' }).defaultNow().notNull(),
	fb_page_id: varchar(),
	page_name: text(),
	page_access_token: text(),
	ig_account_id: text(),
	has_ig_page: boolean(),
	active: boolean(),
	config_id: varchar(),
	page_username: varchar(),
	updated_at: timestamp({ withTimezone: true, mode: 'string' }),
	is_token_valid: boolean(),
	token_debug_result: jsonb(),
	ig_username: text(),
	token_needs_permission: boolean(),
}, (table) => [
	index("hosted_app_config_app_pages_app_id_fb_page_id_config_id_idx").using("btree", table.app_id.asc().nullsLast().op("int8_ops"), table.fb_page_id.asc().nullsLast().op("text_ops"), table.config_id.asc().nullsLast().op("int8_ops")),
	index("hosted_app_config_app_pages_app_id_idx").using("btree", table.app_id.asc().nullsLast().op("int8_ops")),
	index("hosted_app_config_app_pages_app_id_ig_account_id_config_id_idx").using("btree", table.app_id.asc().nullsLast().op("text_ops"), table.ig_account_id.asc().nullsLast().op("int8_ops"), table.config_id.asc().nullsLast().op("text_ops")),
	index("hosted_app_config_app_pages_config_id_idx").using("btree", table.config_id.asc().nullsLast().op("text_ops")),
	index("hosted_app_config_app_pages_fb_id_has_ig_page_idx").using("btree", table.fb_id.asc().nullsLast().op("bool_ops"), table.has_ig_page.asc().nullsLast().op("bool_ops")),
	index("hosted_app_config_app_pages_fb_id_idx").using("btree", table.fb_id.asc().nullsLast().op("text_ops")),
]);

export const hosted_app_config_app_ad_mediaInAudos = db.table("hosted_app_config_app_ad_media", {
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	id: bigint({ mode: "number" }).primaryKey().generatedByDefaultAsIdentity({ name: "db.hosted_app_config_app_ad_media_id_seq", startWith: 1, increment: 1, minValue: 1, maxValue: 9223372036854775807, cache: 1 }),
	created_at: timestamp({ withTimezone: true, mode: 'string' }).defaultNow().notNull(),
	fb_image_hash: text(),
	fb_video_id: text(),
	is_video: boolean().default(false).notNull(),
	is_image: boolean().default(false).notNull(),
	original_media_url: text(),
	ad_account_id: text(),
	status: text().default('uploading').notNull(),
	upload_response: text(),
	app_id: text().notNull(),
	height: smallint(),
	width: smallint(),
});

export const hosted_app_config_adsInAudos = db.table("hosted_app_config_ads", {
	fb_ad_id: varchar({ length: 255 }).primaryKey().notNull(),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	app_id: bigint({ mode: "number" }).notNull(),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	user_id: bigint({ mode: "number" }),
	ad_account_id: varchar({ length: 255 }),
	ad_set_id: varchar({ length: 255 }),
	creative_id: varchar({ length: 255 }),
	name: varchar({ length: 255 }).notNull(),
	status: varchar({ length: 50 }).notNull(),
	details: jsonb(),
	created_at: timestamp({ withTimezone: true, mode: 'string' }).default(sql`CURRENT_TIMESTAMP`),
	updated_at: timestamp({ withTimezone: true, mode: 'string' }).default(sql`CURRENT_TIMESTAMP`),
	audos_ad_trace_id: text(),
	welcome_message_id: varchar(),
	archived: boolean().default(false).notNull(),
	image_url: text(),
	video_url: text(),
	is_foreign_ad: boolean(),
}, (table) => [
	index("hosted_app_config_ads_ad_account_id_idx").using("btree", table.ad_account_id.asc().nullsLast().op("text_ops")),
	index("hosted_app_config_ads_ad_set_id_idx").using("btree", table.ad_set_id.asc().nullsLast().op("text_ops")),
	index("hosted_app_config_ads_creative_id_idx").using("btree", table.creative_id.asc().nullsLast().op("text_ops")),
	index("hosted_app_config_ads_user_id_idx").using("btree", table.user_id.asc().nullsLast().op("int8_ops")),
	foreignKey({
			columns: [table.ad_account_id],
			foreignColumns: [hosted_app_config_ad_accountsInAudos.fb_ad_account_id],
			name: "hosted_app_config_ads_ad_account_id_fkey"
		}).onDelete("cascade"),
	foreignKey({
			columns: [table.ad_set_id],
			foreignColumns: [hosted_app_config_ad_setsInAudos.fb_ad_set_id],
			name: "hosted_app_config_ads_ad_set_id_fkey"
		}).onDelete("cascade"),
	foreignKey({
			columns: [table.creative_id],
			foreignColumns: [hosted_app_config_ad_creativesInAudos.fb_creative_id],
			name: "hosted_app_config_ads_creative_id_fkey"
		}).onDelete("cascade"),
	foreignKey({
			columns: [table.user_id],
			foreignColumns: [hosted_app_config_appsInAudos.id],
			name: "hosted_app_config_ads_user_id_fkey"
		}).onDelete("cascade"),
]);

export const hosted_app_config_app_business_managersInAudos = db.table("hosted_app_config_app_business_managers", {
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	id: bigint({ mode: "number" }).primaryKey().generatedByDefaultAsIdentity({ name: "db.hosted_app_config_app_business_managers_id_seq", startWith: 1, increment: 1, minValue: 1, maxValue: 9223372036854775807, cache: 1 }),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	app_id: bigint({ mode: "number" }),
	fb_id: varchar(),
	created_at: timestamp({ withTimezone: true, mode: 'string' }).defaultNow().notNull(),
	fb_business_id: varchar(),
	business_name: text(),
	business_system_user_access_token: text(),
	active: boolean(),
	audos_partnership_id: varchar(),
	other_details: jsonb(),
}, (table) => [
	index("hosted_app_config_app_business_managers_app_id_idx").using("btree", table.app_id.asc().nullsLast().op("int8_ops")),
]);

export const hosted_app_config_app_contactsInAudos = db.table("hosted_app_config_app_contacts", {
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	id: bigint({ mode: "number" }).primaryKey().generatedByDefaultAsIdentity({ name: "db.hosted_app_config_app_contacts_id_seq", startWith: 1, increment: 1, minValue: 1, maxValue: 9223372036854775807, cache: 1 }),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	app_id: bigint({ mode: "number" }),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	conversation_id: bigint({ mode: "number" }),
	created_at: timestamp({ withTimezone: true, mode: 'string' }).defaultNow().notNull(),
	fb_page_id: varchar(),
	fb_conversation_id: varchar(),
	fb_ad_id: varchar(),
	recipient_page_scope_id: varchar(),
	conversation_platform: text(),
	source: text(),
	message_id: text(),
	contact_type: text(),
	contact_value: text(),
}, (table) => [
	index("hosted_app_config_app_contacts_conversation_id_idx").using("btree", table.conversation_id.asc().nullsLast().op("int8_ops")),
	foreignKey({
			columns: [table.conversation_id],
			foreignColumns: [hosted_app_config_app_conversationsInAudos.id],
			name: "hosted_app_config_app_contacts_conversation_id_fkey"
		}),
]);

export const hosted_app_config_app_conversationsInAudos = db.table("hosted_app_config_app_conversations", {
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	id: bigint({ mode: "number" }).primaryKey().generatedByDefaultAsIdentity({ name: "db.hosted_app_config_app_conversations_id_seq", startWith: 1, increment: 1, minValue: 1, maxValue: 9223372036854775807, cache: 1 }),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	app_id: bigint({ mode: "number" }),
	status_modified_by_user_id: varchar(),
	created_at: timestamp({ withTimezone: true, mode: 'string' }).defaultNow().notNull(),
	updated_at: timestamp({ withTimezone: true, mode: 'string' }).default(sql`CURRENT_TIMESTAMP`),
	fb_page_id: varchar(),
	fb_conversation_id: varchar(),
	recipient_page_scope_id: varchar(),
	ig_account_id: text(),
	conversation_platform: text(),
	active: boolean(),
	opening_message_id: text(),
	conversation_source: varchar(),
	conversation_ad_id: varchar(),
	fb_first_name: text(),
	fb_last_name: text(),
	fb_profile_pic: text(),
	ig_name: text(),
	ig_username: text(),
	ig_profile_pic: text(),
	ig_follower_count: text(),
	ig_is_user_follow_business: boolean(),
	ig_is_business_follow_user: boolean(),
	lead_first_name: text(),
	lead_last_name: text(),
	lead_email: text(),
	lead_phone: text(),
	lead_street_address: text(),
	lead_business_website: text(),
	foreign_ad_id: varchar(),
}, (table) => [
	index("hosted_app_config_app_conversations_ig_account_id_ig_username").using("btree", table.ig_account_id.asc().nullsLast().op("text_ops"), table.ig_username.asc().nullsLast().op("text_ops")),
	unique("hosted_app_config_app_conversations_unique_conversation").on(table.app_id, table.fb_page_id, table.fb_conversation_id),
]);

export const hosted_app_config_meta_api_calls_resultsInAudos = db.table("hosted_app_config_meta_api_calls_results", {
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	id: bigint({ mode: "number" }).primaryKey().generatedByDefaultAsIdentity({ name: "db.hosted_app_config_meta_api_calls_results_id_seq", startWith: 1, increment: 1, minValue: 1, maxValue: 9223372036854775807, cache: 1 }),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	app_id: bigint({ mode: "number" }),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	user_id: bigint({ mode: "number" }),
	fb_id: varchar(),
	page_id: varchar(),
	access_token: text(),
	access_token_type: text(),
	success: boolean().default(false).notNull(),
	status: text(),
	req_url: text(),
	req_params: jsonb(),
	res: jsonb(),
	requirement_context: jsonb(),
	created_at: timestamp({ withTimezone: true, mode: 'string' }).defaultNow().notNull(),
	error_code: text(),
	error_message: text(),
}, (table) => [
	index("hosted_app_config_meta_api_calls_results_optimized_idx").using("btree", table.page_id.asc().nullsLast().op("timestamptz_ops"), table.success.asc().nullsLast().op("int8_ops"), table.app_id.asc().nullsLast().op("text_ops"), table.created_at.desc().nullsFirst().op("bool_ops"), table.requirement_context.asc().nullsLast().op("bool_ops")),
	index("hosted_app_config_meta_api_calls_results_user_id_idx").using("btree", table.user_id.asc().nullsLast().op("int8_ops")),
	index("idx_hosted_app_config_meta_api_calls_results_composite").using("btree", table.page_id.asc().nullsLast().op("bool_ops"), table.success.asc().nullsLast().op("timestamptz_ops"), table.app_id.asc().nullsLast().op("bool_ops"), table.created_at.asc().nullsLast().op("int8_ops")),
	index("idx_hosted_app_config_meta_api_calls_results_composite_1").using("btree", table.page_id.asc().nullsLast().op("text_ops"), table.success.asc().nullsLast().op("bool_ops"), table.app_id.asc().nullsLast().op("bool_ops")),
	foreignKey({
			columns: [table.user_id],
			foreignColumns: [hosted_app_config_appsInAudos.id],
			name: "hosted_app_config_meta_api_calls_results_user_id_fkey"
		}),
]);

export const hosted_app_config_app_leadsInAudos = db.table("hosted_app_config_app_leads", {
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	id: bigint({ mode: "number" }).primaryKey().generatedByDefaultAsIdentity({ name: "db.hosted_app_config_app_leads_id_seq", startWith: 1, increment: 1, minValue: 1, maxValue: 9223372036854775807, cache: 1 }),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	app_id: bigint({ mode: "number" }),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	conversation_id: bigint({ mode: "number" }),
	created_at: timestamp({ withTimezone: true, mode: 'string' }).defaultNow().notNull(),
	fb_page_id: varchar(),
	fb_conversation_id: varchar(),
	fb_ad_id: varchar(),
	recipient_page_scope_id: varchar(),
	conversation_platform: text(),
	foreign_ad_id: varchar(),
}, (table) => [
	index("hosted_app_config_app_leads_conversation_id_idx").using("btree", table.conversation_id.asc().nullsLast().op("int8_ops")),
	foreignKey({
			columns: [table.conversation_id],
			foreignColumns: [hosted_app_config_app_conversationsInAudos.id],
			name: "hosted_app_config_app_leads_conversation_id_fkey"
		}),
]);

export const hosted_app_config_access_tokensInAudos = db.table("hosted_app_config_access_tokens", {
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	id: bigint({ mode: "number" }).primaryKey().generatedByDefaultAsIdentity({ name: "db.hosted_app_config_access_tokens_id_seq", startWith: 1, increment: 1, minValue: 1, maxValue: 9223372036854775807, cache: 1 }),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	app_id: bigint({ mode: "number" }),
	// You can use { mode: "bigint" } if numbers are exceeding js number limitations
	user_id: bigint({ mode: "number" }),
	fb_id: varchar(),
	page_id: varchar(),
	access_token: text(),
	access_token_type: text(),
	page_messaging_enabled: boolean(),
	instagram_messaging_enabled: boolean(),
	ad_permissions_enabled: boolean(),
	status: text(),
	error_source: text(),
	expires_at: timestamp({ withTimezone: true, mode: 'string' }),
	scopes: text(),
	missing_scopes: text(),
	details: jsonb(),
	is_token_valid: boolean(),
	token_data_access_expires_at: date(),
}, (table) => [
	index("hosted_app_config_access_tokens_user_id_idx").using("btree", table.user_id.asc().nullsLast().op("int8_ops")),
	foreignKey({
			columns: [table.user_id],
			foreignColumns: [hosted_app_config_appsInAudos.id],
			name: "hosted_app_config_access_tokens_user_id_fkey"
		}),
]);
