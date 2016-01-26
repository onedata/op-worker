// This is a prototype controller used globally across all sub-pages.

App.GlobalController = Ember.Controller.extend({
    // Static values that are available globally
    userName: null,
    syncMessageStyle: 'color: #2050aa;',
    syncMessage: '',

    // Get store adapter
    getAdapter: function () {
        return App.__container__.lookup('adapter:application')
    },

    // Initialize a value by sending a callback to the server. Value can be
    // e.g. user name which has to be checked once and then is cached
    initializeValue: function (key) {
        var controller = this;
        this.callServer(key, function (returnedValue) {
            controller.set(key, returnedValue);
        });
    },

    // Developer function that performs a callback to the server to
    // make it run sync and update all GUI files that have changed.
    callSync: function () {
        this.set('syncMessageStyle', 'color: #2050aa;');
        this.set('syncMessage', 'syncing...');
        var controller = this;
        var thenFun = function (data) {
            if (data == 'ok') {
                controller.set('syncMessageStyle', 'color: #20aa31;');
                controller.set('syncMessage', 'OK');
                window.location.reload(true);
            } else {
                controller.set('syncMessageStyle', 'color: #bb1354;');
                controller.set('syncMessage', 'Sync failed');
            }
        };
        this.callServer('sync', thenFun);
    },

    // Sends a callback to the server. thenFun is evaluated on response from
    // the server.
    callServer: function (key, thenFun) {
        this.getAdapter().callback('global', key).then(thenFun);
    },

    // Controller init, called automatically by Ember.
    init: function () {
        this.initializeValue('userName');
    }
});