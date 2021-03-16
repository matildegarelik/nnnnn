from datetime import timedelta
import csv
from werkzeug.utils import secure_filename
import os
import ssl
from pymongo import MongoClient
from flask import Flask, render_template, redirect, url_for, request, session, flash, Response, stream_with_context
import requests
from jinja2 import Environment
from jinja2.loaders import FileSystemLoader

mongo_url = "mongodb://gc_f_user:9SjYdJbCuMm1qg5L@cluster0-shard-00-00.mhkp0.mongodb.net:27017,cluster0-shard-00-01.mhkp0.mongodb.net:27017,cluster0-shard-00-02.mhkp0.mongodb.net:27017/<dbname>?ssl=true&replicaSet=atlas-enfcuj-shard-0&authSource=admin&retryWrites=true&w=majority"

def manual_insert(intersection, mongo_url:str = mongo_url, mode: str = 'FR', deduplicate:bool = True):
    client_mdb = MongoClient(mongo_url, ssl_cert_reqs=ssl.CERT_NONE)

    if mode=='FR':
        yield '%s<br/>\n' % 'Processing Funding Rounds'
        split_token = "funding_round/"
        db = client_mdb.data.funding_rounds
        db_key = 'name'

        urls = ['https://europe-west6-cb-src-dst.cloudfunctions.net/src_cb_fr_f0',
            'https://asia-northeast1-cb-src-dst.cloudfunctions.net/src_cb_fr_f1',
            'https://asia-northeast2-cb-src-dst.cloudfunctions.net/src_cb_fr_f2',
            'https://asia-northeast3-cb-src-dst.cloudfunctions.net/src_cb_fr_f3']

    elif mode=='ORG':
        yield '%s<br/>\n' % 'Processing Organizations'
        split_token = "organization/"
        db = client_mdb.data.funding_rounds
        db_key = 'name'

        urls = ['https://us-central1-cb-src-dst.cloudfunctions.net/src_cb_f0',
            'https://us-east1-cb-src-dst.cloudfunctions.net/src_cb_f1',
            'https://us-east4-cb-src-dst.cloudfunctions.net/src_cb_f2',
            'https://us-west2-cb-src-dst.cloudfunctions.net/src_cb_f3',
            'https://us-west3-cb-src-dst.cloudfunctions.net/src_cb_f4',
            'https://us-west4-cb-src-dst.cloudfunctions.net/src_cb_f5',
            'https://northamerica-northeast1-cb-src-dst.cloudfunctions.net/src_cb_f6']

    elif mode=='ACQ':
        yield '%s<br/>\n' % 'Processing Acquisitions'
        split_token = "acquisition/"
        db = client_mdb.data.acquisitions
        db_key = 'acquisitions'

        urls = ['https://us-central1-cb-src-dst.cloudfunctions.net/src_cb_acq_f0',
            'https://us-east1-cb-src-dst.cloudfunctions.net/src_cb_acq_f1',
            'https://us-east4-cb-src-dst.cloudfunctions.net/src_cb_acq_f2',
            'https://us-west2-cb-src-dst.cloudfunctions.net/src_cb_acq_f3',
            'https://australia-southeast1-cb-src-dst.cloudfunctions.net/src_cb_acq_f4',
            'https://asia-southeast2-cb-src-dst.cloudfunctions.net/src_cb_acq_f5']

    else:
        raise Exception("Mode not specified")
    
    #print("intersection: " + str(intersection))
    # get the uuid from the URL
    intersection = [i.split(split_token)[1].strip() for i in intersection]

    # Check how many records are in the db

    yield '%s<br/>\n' % "Number of records in file: {}".format(len(intersection))
    to_skip = []
    for i,c_l in enumerate(intersection):
        if db.count_documents({db_key:c_l}, limit = 1)!=0:
            to_skip.append(c_l)
            del(intersection[i])
            #yield '%s<br/>\n' % 'Record {} already in DB; Skipping'.format(c_l)
    #yield '%s<br/>\n' % 'Record {}\n already in DB; Skipping'.format("\n".join(to_skip))
    yield '%s<br/>\n' % "Number of records to get: {}".format(len(intersection))

    if deduplicate:
        yield '%s<br/>\n' % 'Removing duplicates'
        replic = db.aggregate([            # Cursor with all duplicated documents
        {'$group': {
            '_id': {db_key: f'${db_key}'},     # Duplicated field
            'idsUnicos': {'$addToSet': '$_id'},
            'total': {'$sum': 1}
            }
        },
        {'$match': { 
            'total': {'$gt': 1}    # Holds how many duplicates for each group, if you need it.
            }
        }
        ])

        for i in replic:
            for idx, j in enumerate(i['idsUnicos']):             # It holds the ids of all duplicates 
                if idx != 0:                                     # Jump over first element to keep it
                    db.delete_one({'_id': j})     # Remove the rest
    
    x = 0
    retry = []
    suc = []
    for i,c_l in enumerate(intersection):
        yield '%s<br/>\n' % i
        params = {'message':c_l}
        r = requests.post(url = urls[x], json = params)
        if r.text == 'worked':
            yield '%s<br/>\n' % f"worked for {c_l}"
            suc.append(c_l)
        else:
            yield '%s<br/>\n' % f'failed at {urls[x]}, will retry later'
            retry.append(c_l)
        x+=1
        if x>len(urls)-1:
            x = 0

    for i,c_l in enumerate(retry):
        for url in urls:
            params = {'message':c_l}
            r = requests.post(url = url, json = params)
            if r.text == 'worked':
                yield '%s<br/>\n' % f"worked for {c_l}"
                suc.append(c_l)
                break
            else:
                #yield '%s<br/>\n' % 'failed url:',urls[x]
                yield '%s<br/>\n' % f"failed url: {urls[x]}"
    
    yield '%s<br/>\n' % 'Records get {}; Worked: '.format(",".join(suc))

app = Flask(__name__)
app.secret_key = "matilde"

def stream_template(template_name, **context):
    app.update_template_context(context)
    t = app.jinja_env.get_template(template_name)
    rv = t.stream(context)
    rv.enable_buffering(5)
    return rv

@app.route('/', methods=['GET', 'POST'])
def home():
    if request.method == 'POST':
        csv_file = request.files.get("csv")
        mode = request.form.get("mode")
        if request.form.get("deduplicate") == "T":
            deduplicate = True
        elif request.form.get("deduplicate") == "F":
            deduplicate = False

        intersection= csv_file.read().decode('utf-8').splitlines()

        env = Environment(loader=FileSystemLoader('templates'))
        tmpl = env.get_template('result.html')
        return Response(tmpl.generate(result=manual_insert(intersection=intersection, mode=mode, deduplicate=deduplicate)))

        #return Response(stream_template("result.html", result=manual_insert(intersection=intersection, mode=mode, deduplicate=deduplicate)))
    return render_template("index.html")

if __name__ == "__main__":
    app.run(debug=True)