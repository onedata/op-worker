// Main invocation needed to initialize Ember app. Below params are used to
// enable developer functionalities.
window.App = Ember.Application.create({
    LOG_TRANSITIONS: true,
    LOG_VIEW_LOOKUPS: true,
    LOG_ACTIVE_GENERATION: true
});

App.ApplicationAdapter = DS.WebsocketAdapter.extend({});
