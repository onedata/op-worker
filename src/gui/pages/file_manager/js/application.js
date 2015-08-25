window.FileManager = Ember.Application.create();


//FileManager.ApplicationAdapter = DS.FixtureAdapter.extend();
FileManager.ApplicationAdapter = DS.WebsocketAdapter.extend({});