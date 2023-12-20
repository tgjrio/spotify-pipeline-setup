Before executing the scripts, you'll need to create a [GCP account](https://cloud.google.com/docs/get-started) using your Google email.  After setup, you will need to create a new project if you haven't already.  Now that you have your project created, it's time to enable [Dataform](https://cloud.google.com/dataform?hl=en).

<p>Once Dataform is enabled, you will have to give the service account permission to perform tasks in BigQuery. Below is a demonstration on how that's done:</p>

* Head over to [IAM & Admin](https://console.cloud.google.com/iam-admin)
* At the top right, find and check a box that says: `Include Google-provided role grants`. 
* You should see a principal account ending with ***dataform.iam.gserviceaccount.com***.  Click the edit button on the right to make changes on access.
* Under the dropdown named `Role` search for `"BigQuery Admin"`, select this role and click save.  This will give Dataforms service account permission to run jobs in Big Query.

**After the GCP setup, you're ready to start your Spotify pipeline!**

* Clone this repository to your local drive and open folder in your favorite IDE (VS-Code is what I recommend)
* Open your terminal and create a virtual environment
    *  INSERT COMMAND
* In your terminal, install packages in [requirements.txt](requirements.txt)
    * INSERT COMMAND
* [Install GCloud CLI to your local](https://cloud.google.com/sdk/docs/install)
* [Authenticate into your Google Account](https://cloud.google.com/docs/authentication/provide-credentials-adc) This will provide access to your project(s) on your local environment.
* Create a [developer account](https://developer.spotify.com/) with Spotify and copy your Client ID and Client Secret:
    * At the top right you can click the dropdown and select dashboard
    * Create an app
    * Give your app a name, brief description and put `http://localhost` for redirect URI.
    * Check Web API box
    * Agree to terms and then click save
    * Click your new app to see the dashboard for it.
    * Select settings at the top right and that will bring you to the Client ID and Client Secret
* Create a new file called .env in the root directory of the project and set the variables using your Spotify credentials:
    * client_id = ""
    * client_secret = ""   
* Go into the [initial_load.py](initial_load.py) file and input your values for:
    * `project_id` = Name of the project you created in GCP
    * `location` = Whatever location you set for you GCP project. Safe default can be “us-central1”
    * `user_name` = Your name or alias
    * `user_name` = Your GMail
* Change the `defaultdatabase` value in [dataform.json](dataform_logic/dataform.json) to your GCP `project_id` name

*I recommend keeping Drake and Taylor swift as the artists for first initial load.  During the API pull, some artists don't have the same number of genres listed and could cause a loading error when data is modeled in Dataform. Doing this will set the tone so when the incremental script is ran, the logic will be able to handle a lower or higher number of genres.*

After you setup the variables in `initial_load.py`, you can run the script!
Once it prompts you that it's done, hop into your Dataform UI and check the execution logs in your repo and the tables in Big Query.

* DEMONSTRATION VIDEO


If you're happy and ready to expand, open [`incremental_load.py`](initial_load.py) file and update the same variables that you did for initial_load.py. Please input a list of artists that you want to load next and then execute the script! Highly recommend using 5 at a time to avoid hitting rate limit. Repeat steps (Checking dataform execution logs and bigquery) after script is completed to QA. Run incremental_load.py anytime you want to update your Spotify Database!

<p>I encourage you all to explore the operators we use to pull in data and refactor them to pull from different endpoints that Spotify offers!</p>

If you have any questions, please feel free to reach out to me on [LinkedIn](https://www.linkedin.com/in/tg2/)

Stay tuned for my next project!

Useful resources:
[Dataform Overview and examples](https://cloud.google.com/dataform/docs/overview)
[IAM Roles in GCP](https://cloud.google.com/iam/docs/overview)
[Spotify Developer Documentation](https://developer.spotify.com/documentation/web-api)

