git pCREATE OR REPLACE FUNCTION `aburdenko-project.cheminformatics.add_fake_user`(user_id int64, corp_id STRING) RETURNS STRING
REMOTE WITH CONNECTION `aburdenko-project.us.gcf-conn`
    OPTIONS (
        -- change this to reflect the Trigger URL of your cloud function (look for the TRIGGER tab)
        endpoint = 'https://us-east1-aburdenko-project.cloudfunctions.net/add-fake-user2'
    )