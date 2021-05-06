#!/usr/bin/env python

from flask import Flask, render_template, request, redirect, url_for, flash, session, send_file, jsonify
from flask_bcrypt import Bcrypt
from flask_login import LoginManager, login_required, login_user, logout_user, current_user
from models.Users import User
from models.Users import db
from gridfs import GridFS
from bson.binary import Binary
from pymongo import MongoClient
from bson.objectid import ObjectId
import requests, json, zipfile
import re
import os
from bson.json_util import dumps, loads

# setup the app
app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = os.path.dirname(os.path.realpath(__file__)) + "/static/uploads"
app.config["ALLOWED_EXTENSIONS"] = ["zip"]
app.config['DEBUG'] = True
app.config['SECRET_KEY'] = "SuperSecretKey"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///database.db'

db_url = "mongodb://apurva:user123@cluster0-shard-00-00.p4xv2.mongodb.net:27017,cluster0-shard-00-01.p4xv2.mongodb.net:27017,cluster0-shard-00-02.p4xv2.mongodb.net:27017/IAS_test_1?ssl=true&replicaSet=atlas-auz41v-shard-0&authSource=admin&retryWrites=true&w=majority"
db_name = "IAS_test_1"
collection_name = "test_zip_file_upload"
# file_name = "./Final.zip"

db.init_app(app)
bcrypt = Bcrypt(app)

# setup the login manager
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

# create the db structure
with app.app_context():
    db.create_all()


####  setup routes  ####
@app.route('/')
@login_required
def index():
    return render_template('index.html', user=current_user)


@app.route("/login", methods=["GET", "POST"])
def login():

    # clear the inital flash message
    session.clear()
    if request.method == 'GET':
        return render_template('login.html')

    # get the form data
    username = request.form['username'] 
    password = request.form['password']

    remember_me = False
    if 'remember_me' in request.form:
        remember_me = True

    # query the user
    registered_user = User.query.filter_by(username=username).first()

    # check the passwords
    if registered_user is None and bcrypt.check_password_hash(registered_user.password, password) == False:
        flash('Invalid Username/Password')
        return render_template('login.html')

    # login the user
    login_user(registered_user, remember=remember_me)

    return redirect(request.args.get('next') or url_for('index'))

@app.route('/register', methods=["GET", "POST"])
def register():
    if request.method == 'GET':
        session.clear()
        return render_template('register.html')

    # get the data from our form
    password = request.form['password']
    conf_password = request.form['confirm-password']
    username = request.form['username']
    email = request.form['email']
    role = request.form['role']

    # make sure the password match
    if conf_password != password:
        flash("Passwords do not match")
        return render_template('register.html')

    # check if it meets the right complexity
    check_password = password_check(password)

    # generate error messages if it doesnt pass
    if True in check_password.values():
        for k,v in check_password.items():
            if str(v) == "True":
                flash(k)

        return render_template('register.html')

    # hash the password for storage
    pw_hash = bcrypt.generate_password_hash(password)

    # create a user, and check if its unique
    user = User(username, pw_hash, email, role)
    u_unique = user.unique()

    # add the user
    if u_unique == 0:
        db.session.add(user)
        db.session.commit()
        flash("Account Created")
        return redirect(url_for('login'))

    # else error check what the problem is
    elif u_unique == -1:
        flash("Email address already in use.")
        return render_template('register.html')

    elif u_unique == -2:
        flash("Username already in use.")
        return render_template('register.html')

    else:
        flash("Username and Email already in use.")
        return render_template('register.html')


@app.route('/logout')
def logout():
    logout_user()
    return redirect(url_for('index'))


@app.route('/charts')
def charts():
    return render_template('charts.html', user=current_user)


@app.route('/tables')
def tables():
    return render_template('tables.html', user=current_user)


@app.route('/forms')
def forms():
    return render_template('forms.html', user=current_user)


@app.route('/bootstrap-elements')
def bootstrap_elements():
    return render_template('bootstrap-elements.html', user=current_user)


@app.route('/bootstrap-grid')
def bootstrap_grid():
    return render_template('bootstrap-grid.html', user=current_user)

def collection_to_json(col):
	cursor = col.find()
	list_cur = list(cursor)
	json_data = dumps(list_cur)
	return json_data

def fetch_place_id():
    place_details_collection = "place_collection"
    cluster = MongoClient(db_url)
    db = cluster[db_name]
    collection = db[place_details_collection]
    result = collection_to_json(collection)
    result = json.loads(result)
    place_id_list = []
    for row in result:
        place_id_list.append(row['place_id'])
    place_id_list.append("default")

    print(place_id_list)
    return place_id_list

@app.route('/schedule-application')
def blank_page():
    place_id_list = fetch_place_id()
    return render_template('schedule-application.html', user=current_user, place_id=place_id_list)


@app.route('/profile')
def profile():
    return render_template('profile.html', user=current_user)


@app.route('/settings')
def settings():
    return render_template('settings.html', user=current_user)

@app.route('/download', methods=["GET","POST"])
@login_required
def download():
    # Appending app path to upload folder path within app root folder
    filepath = os.path.join(app.config['UPLOAD_FOLDER'],filename)
    filename = "template.json"
    # Returning file from appended path
    return send_file(filepath, as_attachment=True)

@app.route('/scheduling_request_upload/', methods=["POST"])
@login_required
def scheduling_request_upload():
    uploaded_file = request.files['file']

    if uploaded_file.filename != '':
        fpath = os.path.join('static/uploads/', uploaded_file.filename)
        uploaded_file.save(fpath)

    with open(fpath, "r") as f:
        data = json.loads(f.read())
    requests.post('http://scheduler:13337/schedule_request',json=data)

    return render_template('index.html', user=current_user)

@app.route('/scheduling_request/', methods=["POST"])
@login_required
def scheduling_request():
    application_name = str(request.form['application_name'])
    location = str(request.form['location'])

    startTimes = request.form['startTime'].split(',')
    durations = request.form['duration'].split(',')
    # all_sensor = request.form['all_sensor']
    message = request.form['message']

    isScheduled = request.form['isScheduled']

    if(isScheduled == False):
        startTimes = None
        durations = None
    # startTimeDummy = "12:01:15,09:34:21"
    # durationDummy = "01:01:10,00:02:15"
    # locationDummy = "23.45:32.21,54.12:32"

    email = request.form['email']
    mobile = request.form['mobile']

    days = list()
    
    if request.form.get("mon"):
        days.append("Monday")
    if request.form.get("tue"):
        days.append("Tuesday")
    if request.form.get("wed"):
        days.append("Wednesday")
    if request.form.get("thu"):
        days.append("Thursday")
    if request.form.get("fri"):
        days.append("Friday")
    if request.form.get("sat"):
        days.append("Saturday")
    if request.form.get("sun"):
        days.append("Sunday")

    # print(days)

    data = {    
        application_name : {
            "user_id" : current_user.username,
            "application_name" : application_name,
            "algorithms" : {
                "algorithm1" : {
                    "isScheduled" : isScheduled,
                    "schedule" : {
                        "time" : {
                            "startTimes" : startTimes,
                            "durations" : durations
                        },
                        "days" : days
                    },
                    "action" : {
                        "user_display" : message,
                        "sensor_manager" : [{"sensor_id1" : "command1"}],
                        "notify_user" : [email,mobile]
                    },
                    "place_id": location
                    }
                }
            }
        }

    requests.post('http://scheduler:13337/schedule_request',json=data)
    return render_template('index.html', user=current_user)

def validateJSON(jsonData):
    try:
        json.loads(jsonData)
    except ValueError as err:
        return False
    return True

def check_format(uploaded_file, app_path, app_name, file_path):
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(app_path)
    
    # check src file, exists or not.
    src_path = app_path+"/"+app_name+"/src"
    if not os.path.exists(src_path):
        return jsonify({"status" : "src folder missing"})


    # check app_config.
    config_filepath = app_path+"/"+app_name + "/app_config.json"
    if not os.path.exists(config_filepath):
        return jsonify({"status" : "app_config.json missing"})
    
    # validate json
    if not validateJSON(config_filepath):
        return jsonify({"status" : "Invalid JSON file"})
    
    return True
    
def create_connection_mongo_cloud():
    cluster = MongoClient(db_url)
    db = cluster[db_name]
    coll = db[collection_name]
    db2 = cluster.gridfs_example
    fs = GridFS(db2)
    return fs, coll

@app.route('/uploads/', methods=["POST"])
@login_required
def upload_file():

    uploaded_file = request.files['file']
    app_path = "./static/uploads/"
    app_name = uploaded_file.filename

    if uploaded_file.filename != '':
        file_path = os.path.join(app_path, app_name)
        uploaded_file.save(file_path)
    
    format_status = check_format(uploaded_file, app_path, app_name, file_path)
    if format_status == True:
        pass
    else:
        return format_status

    fs, coll = create_connection_mongo_cloud()

    with open(file_path, "rb") as fp:
        encoded = Binary(fp.read())
    flink = fs.put(encoded, filename = app_name)

    coll.insert_one({"filename": app_name, "file": flink })

    os.remove(file_path)

    return json.dumps({'status': 'Zip uploaded successfully'}), 200

####  end routes  ####


# required function for loading the right user
@login_manager.user_loader
def load_user(id):
    return User.query.get(int(id))

# check password complexity
def password_check(password):
    """
    Verify the strength of 'password'
    Returns a dict indicating the wrong criteria
    A password is considered strong if:
        8 characters length or more
        1 digit or more
        1 symbol or more
        1 uppercase letter or more
        1 lowercase letter or more
        credit to: ePi272314
        https://stackoverflow.com/questions/16709638/checking-the-strength-of-a-password-how-to-check-conditions
    """

    # calculating the length
    length_error = len(password) <= 8

    # searching for digits
    digit_error = re.search(r"\d", password) is None

    # searching for uppercase
    uppercase_error = re.search(r"[A-Z]", password) is None

    # searching for lowercase
    lowercase_error = re.search(r"[a-z]", password) is None

    # searching for symbols
    symbol_error = re.search(r"[ !@#$%&'()*+,-./[\\\]^_`{|}~"+r'"]', password) is None

    ret = {
        'Password is less than 8 characters' : length_error,
        'Password does not contain a number' : digit_error,
        'Password does not contain a uppercase character' : uppercase_error,
        'Password does not contain a lowercase character' : lowercase_error,
        'Password does not contain a special character' : symbol_error,
    }

    return ret


if __name__ == "__main__":
	# change to app.run(host="0.0.0.0"), if you want other machines to be able to reach the webserver.
    # db.create_all()
    # call apurva's api and 
    app.run(port=9999, threaded=True, host='0.0.0.0')
    # app.run(host="localhost",port=5005)