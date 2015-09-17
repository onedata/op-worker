window.FileManager = Ember.Application.create({
    LOG_TRANSITIONS: true,
    LOG_VIEW_LOOKUPS: true,
    LOG_ACTIVE_GENERATION: true
});


FileManager.ApplicationController = Ember.Controller.extend({
    global: {
        user: 'user lol global'
    }
});


//FileManager.ApplicationAdapter = DS.FixtureAdapter.extend();
FileManager.ApplicationAdapter = DS.WebsocketAdapter.extend({});