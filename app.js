const { BigQuery } = require('@google-cloud/bigquery');
const fs = require('fs');

console.clear();

/** Variables */

const sprout_social_url = 'https://api.sproutsocial.com/v1';

/** Edit variables */
const bearer_token = 'xxxxxxxxxxxxxxxxx';
const dataset_id = 'xxxxxxxxxxxxxxxxx';
const table_id = 'xxxxxxxxxxxxxxxxx';   
const key_file_path = './xxxxxxxxxxxxxxxxx.json';
/** End edit variables */

const key_file = JSON.parse(fs.readFileSync(key_file_path));
const bigquery = new BigQuery({
    projectId: key_file.project_id,
    credentials: {
      client_email: key_file.client_email,
      private_key: key_file.private_key
    }
});

/** End Variables */

const syncData = async (url, type, current_page = null) => {
    let data;
    let json;

    if(current_page){
        json = {
            "fields": [
              "post_type",
              "network",
              "title",
              "content_category",
              "guid",
              "created_time",
              "perma_link",
              "text",
              "internal.tags.id",
              "internal.sent_by.id",
              "internal.sent_by.email",
              "internal.sent_by.first_name",
              "internal.sent_by.last_name",
              "customer_profile_id"
            ],
            "filters": [
              "customer_profile_id.eq(6362797)",
              "created_time.in(2024-07-03T00:00:00..2024-07-03T23:59:59)"
            ],
            "metrics": [
              "lifetime.impressions",
              "lifetime.impressions_organic",
              "lifetime.impressions_viral",
              "lifetime.impressions_nonviral",
              "lifetime.impressions_paid",
              "lifetime.impressions_follower",
              "lifetime.impressions_nonfollower",
              "lifetime.impressions_unique",
              "lifetime.impressions_organic_unique",
              "lifetime.impressions_viral_unique",
              "lifetime.impressions_nonviral_unique",
              "lifetime.impressions_paid_unique",
              "lifetime.impressions_follower_unique", 
              "lifetime.reactions",
              "lifetime.likes",
              "lifetime.reactions_love",
              "lifetime.reactions_haha",
              "lifetime.reactions_wow",
              "lifetime.reactions_sad",
              "lifetime.reactions_angry",
              "lifetime.comments_count",
              "lifetime.shares_count",
              "lifetime.question_answers",
              "lifetime.post_content_clicks",
              "lifetime.post_link_clicks",
              "lifetime.post_photo_view_clicks",
              "lifetime.post_video_play_clicks",
              "lifetime.post_content_clicks_other",
              "lifetime.negative_feedback",
              "lifetime.engagements_unique",
              "lifetime.engagements_follower_unique",
              "lifetime.post_content_clicks_unique",
              "lifetime.post_link_clicks_unique",
              "lifetime.post_photo_view_clicks_unique",
              "lifetime.post_video_play_clicks_unique",
              "lifetime.post_other_clicks_unique",
              "lifetime.negative_feedback_unique",
              "video_length",
              "lifetime.video_views",
              "lifetime.video_views_organic",
              "lifetime.video_views_paid",
              "lifetime.video_views_autoplay",
              "lifetime.video_views_click_to_play",
              "lifetime.video_views_sound_on",
              "lifetime.video_views_sound_off",
              "lifetime.video_views_10s",
              "lifetime.video_views_10s_organic",
              "lifetime.video_views_10s_paid",
              "lifetime.video_views_10s_autoplay",
              "lifetime.video_views_10s_click_to_play",
              "lifetime.video_views_10s_sound_on",
              "lifetime.video_views_10s_sound_off",
              "lifetime.video_views_partial",
              "lifetime.video_views_partial_organic",
              "lifetime.video_views_partial_paid",
              "lifetime.video_views_partial_autoplay",
              "lifetime.video_views_partial_click_to_play",
              "lifetime.video_views_30s_complete",
              "lifetime.video_views_30s_complete_organic",
              "lifetime.video_views_30s_complete_paid",
              "lifetime.video_views_30s_complete_autoplay",
              "lifetime.video_views_30s_complete_click_to_play",
              "lifetime.video_views_p95",
              "lifetime.video_views_p95_organic",
              "lifetime.video_views_p95_paid",
              "lifetime.video_views_unique",
              "lifetime.video_views_organic_unique",
              "lifetime.video_views_paid_unique",
              "lifetime.video_views_10s_unique",
              "lifetime.video_views_30s_complete_unique",
              "lifetime.video_views_p95_organic_unique",
              "lifetime.video_views_p95_paid_unique",
              "lifetime.video_view_time_per_view",
              "lifetime.video_view_time",
              "lifetime.video_view_time_organic",
              "lifetime.video_view_time_paid",
              "lifetime.video_ad_break_impressions",
              "lifetime.video_ad_break_earnings",
              "lifetime.video_ad_break_cost_per_impression"
          
            ],
            "page": current_page
          }

    }

    try{

        const get = await fetch(`${sprout_social_url}${url}`, {
            method: type,
            headers: {
                'Authorization': `Bearer ${bearer_token}`,
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(json)
        });

        const showData = await get.json();
        if(showData){
            data = showData;
        }
    }catch(err){
        console.error(err);
    }

    return data;
}

const flattenObject = (obj, parentKey = '', res = {}) => {
    for (let key in obj) {
        if (obj.hasOwnProperty(key)) {
            const newKey = key.replace(/\./g, '_');
            const propName = parentKey ? `${parentKey}_${newKey}` : newKey;
            if (typeof obj[key] === 'object' && !Array.isArray(obj[key])) {
                flattenObject(obj[key], propName, res);
            } else {
                res[propName] = obj[key];
            }
        }
    }
    return res;
};

const clientID = async () => {

    let id;

    try{
        const idJSON = await syncData('/metadata/client', 'GET');
        if(idJSON){
            id = idJSON.data[0].customer_id;
        }
    }catch(err){
        console.error(err);
    }
    return id; 
}

const analyticsPost = async (id) => {

    let post_json = [];

    try{
        let all_pages = 1;
        let current_page = 1;

        do{
            const postJSON = await syncData(`/${id}/analytics/posts`, 'POST', current_page);
            const get_post_data = postJSON.data;
            const get_all_page = postJSON.paging.total_pages;

            if(get_all_page){
                all_pages = get_all_page;
            }

            if(get_post_data){
                get_post_data.forEach(e => {
                    const flattened = flattenObject(e);
                    post_json.push(flattened);
                });
            }

            current_page += 1;
        }while(current_page <= all_pages);

    }catch(err){
        console.error(err);
    }

    return post_json;
}

const insertDataBigQuery = async (data) => {

    try{
        await bigquery.dataset(dataset_id).table(table_id).insert(data);
        console.log('La data ser inserto en con exito');
    }catch(err){
        console.error(err);
    }

}

const getCurrentSchema = async (table) => {
    const [metadata] = await table.getMetadata();
    return metadata.schema.fields;
};

const getNewFields = (currentSchema, newSchema) => {

    if(currentSchema){
        const currentFieldNames = currentSchema.map(field => field.name);
        return newSchema.filter(field => !currentFieldNames.includes(field.name));
    }

    return newSchema;

};

const updateSchema = async (table, newFields) => {
    const [metadata] = await table.getMetadata();
    const newScheme = {
        fields: newFields
    }

    await table.setMetadata({ schema: newScheme }); 
};

const schemaAuto = (rows) => {
    const schema = {};
    rows.forEach(row => {
      Object.keys(row).forEach(key => {
        if (!schema[key]) {
          schema[key] = typeof row[key] === 'object' ? 'STRING' : typeof row[key] === 'boolean' ? 'BOOLEAN' : 'STRING';
        }
      });
    });
  
    return Object.keys(schema).map(key => ({
      name: key,
      type: schema[key].toUpperCase()
    }));
};
  

const main = async () => {

    const dataset = bigquery.dataset(dataset_id);
    const table = dataset.table(table_id);

    const id = await clientID();
    const data = await analyticsPost(id);
    const schema = schemaAuto(data);
    const currentSchema = await getCurrentSchema(table);
    const newFields = getNewFields(currentSchema, schema);

    if(newFields.length > 0){
        await updateSchema(table, newFields);
    }

    await insertDataBigQuery(data);

}

main();