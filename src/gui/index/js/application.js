window.App = Ember.Application.create({
    LOG_TRANSITIONS: true,
    LOG_VIEW_LOOKUPS: true,
    LOG_ACTIVE_GENERATION: true
});


App.ApplicationController = Ember.Controller.extend({});


//App.ApplicationAdapter = DS.FixtureAdapter.extend();
App.ApplicationAdapter = DS.WebsocketAdapter.extend({});