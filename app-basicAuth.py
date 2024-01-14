# Import dependencies
from flask import Flask, request, abort
from flask_httpauth import HTTPBasicAuth
from flask_cors import CORS
from dotenv import load_dotenv
from datetime import datetime
import logic
import json
import os


# Create Flask application
app = Flask(__name__)
CORS(app)


# HTTPBasicAuth class from flask_httpauth is used to handle Basic Authentication
auth = HTTPBasicAuth()


# Load environment variables from .env file
load_dotenv()


# User credentials stored in a dictionary
users = {
     os.getenv("BASIC_AUTH_USR") : os.getenv("BASIC_AUTH_PWD"),
}


# Used to check whether the username and password provided under Basic Auth of the API call is valid
@auth.verify_password
def verify_password(username, password):
    return (username in users and users[username] == password)


# Endpoint which accepts a search request
# Validates the API call for username and password provided under Basic Auth
# And if the authentication is valid, customer data is returned
@app.route("/get-customer-data", methods=["POST"])
@auth.login_required
def get_customer_data():
    if(auth.current_user()):
        # Gets and extracts the value of user request from the JSON object
        request_usr = request.get_json()
        request_date = request_usr['date']

        try:
            datetime.strptime(request_date, '%Y-%m-%d')
            
            # Runs the SQL query against the data warehouse and gets a response
            # Returns this response in JSON format
            json_response = logic.get_customer_data_from_sourceDB(request_date)
            return json.loads(json_response), 200
        except ValueError:
            # Raise 400 Bad Request if an invalid date is passed
            abort(400, description="Invalid date.")
        


# Run Flask application
if __name__ == "__main__":
    app.run(host='127.0.0.1', port=5000, debug=True)