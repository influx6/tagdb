#TagDB
 A simple library based on the idea that database interfaces need not be that complicated.
 Current supports mongo,redis and couchdb, couchbase support is still in the mix

##Examples
 A simple mongodb tagdb instance and request approach

 ```

     /* Located in .test/mongo.dart file */

      var mongo = TagDB.create('mongo',{
        'url':"mongodb://127.0.0.1/mydb",
        'port': 27017
      });
      mongo.open().then(Funcs.tag('booting mongodb')).then((f){

        mongo.query({
          '@':'insert',
          'db': 'mydb',
          'data':{'name':'john','age':31}
        }).then(Funcs.tag('mydb-insert'));

        mongo.query({
          '@':'all',
          'db':'mydb',
        }).then(Funcs.tag('mydb-all'));

        mongo.query({
          '@':'findOne',
          'db': 'mydb'
        }).then(Funcs.tag('mydb-findone'));

        mongo.query({
          '@':'update',
          'db': 'mydb',
          'criteria': [{'name':'alex'},{'\$set':{'status':'single'}},{'multiUpdate': true}]
        }).then(Funcs.tag('mydb-update'));

        mongo.query({
          '@':'find',
          'db': 'mydb',
          'criteria':[{'status': 'single'}]
        });

        return mongo.query({
          '@':'drop',
          'db': 'mydb',
        }).then(Funcs.tag('dropped my-db'));
        
      }).then(Funcs.tag('mydb-drop')).then((n){
          mongo.end().then((f)=> print('ending mongo'));
      }).catchError(Funcs.tag('mongodb error'));


 ```
