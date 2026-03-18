"""
EPMT static file server module - serves static documentation files.
"""
#!/usr/bin/env python

from flask import Flask, send_from_directory, render_template

# set the project root directory as the static folder
app = Flask( __name__,
             static_folder='epmtdocs/site/',
             template_folder="epmtdocs/site/")

@app.errorhandler(404)
def not_found(e):
    ''' 
    inbuilt function which takes error as parameter 
    '''
    return render_template("404.html")


@app.route('/')
def send_filert():
    '''
    Handle the case where user visits the root of static server
    This is needed as we aren't using index.html files in directories
    '''
    return send_from_directory(app.static_folder, 'Quickstart.html')

@app.route('/<apage>')
def send_file(apage):
    '''
    Serve any requested page from within the site directory
    '''
    return send_from_directory(app.static_folder, apage)


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8000)
