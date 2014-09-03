library spec;

import 'package:tagdb/client.dart';
import 'package:hub/hubclient.dart';

void main(){

  jazzUp((_){

    _.group('testing lawndb tagdb',($){

        var rd = TagDB.create('lawndb',{
          'id':'lawny',
          'db':'lawnydb'
        });

        $.test('open connection')
        .rackAsync('open',(f,next){
          f.open()
          .then((f) => next());
        })
        .rackAsync('make operations',(t,nxt){

            $.test('validating object construction')
            .rack('is it valid',(f){
              Expects.exists(f);
            })
            .rack('has a configuration',(f){
              Expects.isMap(f.conf.core);
            })
            .rack("db's name is lawnydb",(f){
              Expects.asserts(f.conf.get('db'),'lawnydb');
            })
            .rack("db id name asserts lawny",(f){
              Expects.asserts(f.conf.get('id'),'lawny');
            })
            .emit(t);

            $.test('can i save data')
            .rackAsync('can i save animal: dog?',(f,next){
              f.query({
                '@': 'save_doc',
                'id': 'animal',
                'data': 'dog'
              })
             .then(Funcs.alwaysFn(next));
            })
            .emit(t);

            $.test('can i get data')
            .rackAsync('is animal equal to dog?',(f,next){
              f.query({
                '@':'get_doc',
                'id':'animal'
              })
             .then(Funcs.alwaysFn(nxt))
             .then(Funcs.alwaysFn(next));
            })
            .emit(t);

            $.test('total items')
            .rackAsync('total item is one',(f,next){
              f.query({'@':'all_keys'})
              .then((_) => Expects.asserts(_.size,1))
              .then(Funcs.alwaysFn(next));
            })
            .rackAsync('total item is one',(f,next){
              f.query({'@':'get_docs', 'id':['animal'] })
              .then((_) => Expects.asserts(_.size,1))
              .then(Funcs.alwaysFn(next));
            })
            .emit(t);

            $.test('does item exists?')
            .rackAsync('does animal exists',(f,next){
              f.query({'@':'doc_exists', 'id':'animal'})
              .then((_) => Expects.isTrue(_))
              .then(Funcs.alwaysFn(next));
            })
            .emit(t);

            $.test('can we batch?')
            .rackAsync('batch 2 items',(f,next){
              f.query({
                '@':'insert_all', 
                'data':{'name':'alex','age':'25'}
              })
              .then((_) => Expects.isList(_))
              .then(Funcs.alwaysFn(next));
            })
            .emit(t);

        })
        .rackAsync('can we drop db',(f,next){
          f.query({'@':'nuke_db'})
          .then(Funcs.alwaysFn(next));
        })
        .emit(rd);

    });

    _.group('testing requestdb tagdb',($){

        var rd = TagDB.create('requestdb',{
          'id':'lawny',
          'url':'http://127.0.0.1',
          'port': 3010
        });

        $.test('open connection')
        .rackAsync('open',(f,next){
          f.open()
          .then(Funcs.tag('caller'))
          .then(Funcs.alwaysFn(next));
        })
        .rack('make requests',(f){
          
        })
        .emit(rd);

    });

  });

}
