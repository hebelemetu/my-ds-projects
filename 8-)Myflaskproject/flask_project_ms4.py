#import flask and sqlite
from flask import Flask, render_template,request,redirect
import sqlite3 as sql

app = Flask(__name__)
#database connection
con = sql.connect("usda.sql3")
con.row_factory = sql.Row

#Query for first table and make rows for loop through each row
cur = con.cursor()
cur.execute("SELECT * from food_group")
rows = cur.fetchall();

#Home page function


#Food_group list function
@app.route('/')
def list():
    return render_template("db_ms4.html", rows=rows)

#Food table function
@app.route('/food/<urunid>')
def urun(urunid):
   con = sql.connect("usda.sql3")
   con.row_factory = sql.Row
   cur = con.cursor()
   return render_template('urun_ms4.html', urunid=urunid,rows=rows,cur=cur)

#Form function to change properties
@app.route('/<id>/<id2>/entry_form', methods=["GET","POST"])
def form(id,id2):
   con = sql.connect("usda.sql3")
   con.row_factory = sql.Row
   cur = con.cursor()
   cur.execute("SELECT id,food_group_id,long_desc,short_desc,manufac_name,sci_name from food")
   for satir in cur.execute("SELECT short_desc,long_desc,manufac_name,sci_name from food WHERE id=?",[id2]).fetchall():
      sd_1=satir["short_desc"]
      ld = satir["long_desc"]
      mn = satir["manufac_name"]
      sn = satir["sci_name"]
   if request.method=="POST":
      fg_id=request.form.get("Food Group")
      sd_1=request.form.get("Short Description")
      ld=request.form.get("Long Description")
      mn=request.form.get("Manufacturer")
      sn=request.form.get("Scientific Name")
      cur.execute("UPDATE food SET food_group_id = ?, short_desc=?,long_desc=?,manufac_name=?,sci_name=? WHERE id=?", [fg_id,sd_1,ld,mn,sn,id2])
      print(fg_id,sd_1,ld,mn,sn)
      #return redirect(request.url)
   con.commit()
   return render_template('form_ms4.html',cur=cur,id=id,id2=id2,sd_1=sd_1,ld=ld,mn=mn,sn=sn)



if __name__ == '__main__':
   app.run(debug=True)
