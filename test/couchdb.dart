library tagdb.spec;

import 'dart:io';
import 'package:tagdb/server.dart';
import 'package:hub/hub.dart';

void main(){

  var couch = TagDB.create('couch',{
    'url':"http://127.0.0.1",
    'port': 5984
  });

  couch.open().then((f){
    print('opened couchdb $f');

    couch.query({
      'id':'drop_db',
      'db':'page_reviews'
    }).then(Funcs.tag('drop_db')).then((n){

        couch.query({
          'id':'create_db',
          'db':'page_reviews'
        }).then(Funcs.tag('create_db')).then((n){
            couch.query({
              'id':'save_doc',
              'db':'page_reviews',
              'data': {
                'id': 'fox_news',
                'article':" we totally wiped them out!"
              }
            }).then(Funcs.tag('save_doc'));


            couch.query({
              'id':'save_doc',
              'db':'page_reviews',
              'doc_id': '32-rivers',
              'data': {
                'id': 'mbc_news',
                'article':" we totally wiped them out!"
              }
            }).then(Funcs.tag('save_doc_rivers')).then((n){
                
                couch.query({
                  'id':'get_doc',
                  'db':'page_reviews',
                  'doc_id': '32-rivers',
                }).then(Funcs.tag('get_doc')).then((n){
                  couch.query({
                    'id':'drop_doc',
                    'db':'page_reviews',
                    'doc_id': '32-rivers',
                    'rev_id': Enums.first(n.metaData['etag'])
                  }).then(Funcs.tag('drop_doc_rivers'));
                });

            });

        });
    });


    couch.query({
      'id':'all_docs',
      'db': "mydb"
    }).then(Funcs.tag('all_docs'));

    couch.query({
      'id':'all_db'
    }).then(Funcs.tag('all_db'));

  });

}
