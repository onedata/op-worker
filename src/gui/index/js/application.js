window.App = Ember.Application.create({
    LOG_TRANSITIONS: true,
    LOG_VIEW_LOOKUPS: true,
    LOG_ACTIVE_GENERATION: true
});


App.ApplicationController = Ember.Controller.extend({
    global: {
        user: 'user lol global'
    }
    
});


//App.ApplicationAdapter = DS.FixtureAdapter.extend();
App.ApplicationAdapter = DS.WebsocketAdapter.extend({});