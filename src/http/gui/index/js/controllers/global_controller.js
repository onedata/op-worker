App.GlobalController = Ember.Controller.extend({
    // Static values that are available globally
    userName: null,
    syncMessageStyle: 'color: #2050aa;',
    syncMessage: '',

    getAdapter: function () {
        return App.__container__.lookup('adapter:application')
    },

    // @todo controller dla callbackow
    initializeValue: function (key) {
        var controller = this;
        controller.getAdapter().callback('global', key)
            .then(function (returnedValue) {
                controller.set(key, returnedValue);
            });
    },

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

    callServer: function (key, thenFun) {
        this.getAdapter().callback('global', key).then(thenFun);
    },

    init: function () {
        this.initializeValue('userName');
    }
});