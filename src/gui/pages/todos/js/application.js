window.Todos = Ember.Application.create();


//Todos.ApplicationAdapter = DS.LSAdapter.extend({namespace: 'todos-emberjs'});
Todos.ApplicationAdapter = DS.WebsocketAdapter.extend({});