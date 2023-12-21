## WELCOME THE WORLD OF TGJR.IO
This repository is setup for novice engineers who are looking to get their hands on a simple working pipeline.  I don't have a video tutorial for this but there is commentary throughout this entire repo explaining what each process is doing.  I'm also assuming you know your way around Github and you've been exposed to Python since you're destined to be an engineer.  It's a plus if you're familir with GCP but navigating their products are pretty straightfoward.  Now let me explain what you're getting into.

Click this [link](https://www.kaggle.com/datasets/tonygordonjr/spotify-dataset-2023) if you want to see the final result of what you could pontentially have.  I uploaded a version of my final reporting table to Kaggle.

From a high level, the [initial_load.py](initial_load.py) pipeline script is:
1. Pulling data from Spotify at 5 different endpoints
2. Processing data via Python to make the load into BigQuery happy
3. Creating the required datasets and tables in BigQuery while updating schema
4. Creating Dataform repository & workspace to model & update production tables
5. Commiting & Pulling files from [dataform_logic](dataform_logic) into your newly created Dataform repository
6. Installing the required packages you need to run Dataform
7. Upload data into their respective tables in the staging dataset
8. Invoking the Dataform workflow to process data into their respective tables in the production dataset

After the inital load is complete, the [incremental.py](incremenal_load.py) script with only do steps 1, 2, 7 and 8 as it loads in new records for artists you choose. 

Whether you're new to Python or not, I encourage you to take a look at [gcp_operators.py](gcp_operators.py) and [operators.py](operators.py) to scope out the functions that handle all of the processes in the pipelines.  Each function comes with an explanation on what's happening and the required arguments.

I'm assuming most people coming to this project is new to Dataform and to be honest, there's a lot to talk about there but not enough time.  I provided a link at the bottom of this ReadMe to it's documnentation page and you can learn more about how it works there.  

To explain it simply, Dataform is a service catered for analysts who want to model data into tables they can report from.  It's a very structured way to keep your code organized and it uses version control to manage everything.  You can even link any Git type of repo service to your Dataform repository!

Now that we got the introduction out of the way, let's get started on our very own Spotify pipeline!


### THE FUN BEGINS  
Before executing the scripts, you'll need to create a [GCP account](https://cloud.google.com/docs/get-started) using your Google email.  After setup, you will need to create a new project if you haven't already.  Now that you have your project created, it's time to enable [Dataform](https://cloud.google.com/dataform?hl=en).

<p>Once Dataform is enabled, you will have to give the service account permission to perform tasks in BigQuery. Below is a demonstration on how that's done:</p>

* Head over to [IAM & Admin](https://console.cloud.google.com/iam-admin)
* At the top right, find and check a box that says: `Include Google-provided role grants`. 
* You should see a principal account ending with ***dataform.iam.gserviceaccount.com***.  Click the edit button on the right to make changes on access.
* Under the dropdown named `Role` search for `"BigQuery Admin"`, select this role and click save.  This will give Dataforms service account permission to run jobs in Big Query.

**After the GCP setup, you're ready to start your Spotify pipeline!**

* Clone or Fork this repository and then open folder in your favorite IDE (VS-Code is what I recommend)
* Open your terminal and make sure you're in the root directory of this project. Create a virtual environment and then you'll activate it:
    *  `python -m venv [env_name]` env_name should be the name you want for your virtual environment and you can remove the [ ]
    * To activate environment on a Windows: `myenv\Scripts\activate`
    * To activate environment on a Mac: `source myenv/bin/activate`
* In your terminal, install packages in [requirements.txt](requirements.txt)
    * `pip install -r requirements.txt`
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
* Create a new file called `.env` in the root directory of the project and set the variables using your Spotify credentials:
    * client_id = ""
    * client_secret = ""   
* Go into the [initial_load.py](initial_load.py) file and input your values for:
    * `project_id` = Name of the project you created in GCP
    * `location` = Whatever location you set for you GCP project. Safe default can be “us-central1”
    * `user_name` = Your name or alias
    * `user_name` = Your GMail
* Change the `defaultdatabase` value in [dataform.json](dataform_logic/dataform.json) to your GCP `project_id` name

After you setup the variables in `initial_load.py`, you can run the script!
Once it prompts you that it's done, hop into your Dataform UI and check the execution logs in your repo and the tables in Big Query.

*I highly recommend keeping Drake and Taylor swift as the artists for first initial load.  During the API pull, some artists don't have the same number of genres listed and could cause a loading error when data is modeled in Dataform. Doing this will set the tone so when the incremental script is ran, the logic will be able to handle a lower or higher number of genres.*

If you're happy and ready to expand, open the [`incremental_load.py`](initial_load.py) file and update the `project_id` & `location` variables with the same values you put for initial_load.py. Please input a list of artists that you want to load next and then execute the script! Highly recommend using 5 at a time to avoid hitting rate limit. Repeat steps (Checking dataform execution logs and Bigquery) after script is completed to QA. Run incremental_load.py anytime you want to update your Spotify Database!

That's it! 

I'm extremely thankful that you took some time to work through this exercise and I hope you come out of this with a clear understanding of what a simple pipeline looks like.  There's so much to learn in the world of Data Engineering and the best advice I can give to a rookie like me: 

*Since there's more than one way to solve a problem in this field, don't get caught up on trying to find the best solution everytime.  Really think about solving the problem first and after you do that, you'll have the flexibility to expand on that project to make it more efficient or implement different tools you've learned about.*

If you have any questions, please feel free to reach out to me on [LinkedIn](https://www.linkedin.com/in/tg2/)

Stay tuned for my next project!

Useful resources:\
[Dataform Overview and examples](https://cloud.google.com/dataform/docs/overview)\
[IAM Roles in GCP](https://cloud.google.com/iam/docs/overview)\
[Spotify Developer Documentation](https://developer.spotify.com/documentation/web-api)

