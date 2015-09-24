App.GlobalController = Ember.Controller.extend({
    // Static values that are available globally
    user_name: null,

    getAdapter: function () {
        return App.__container__.lookup('adapter:application')
    },

    initializeValue: function (key) {
        var controller = this;
        controller.getAdapter().getStaticData(key)
            .then(function (returnedValue) {
                controller.set(key, returnedValue);
            });
    },

    init: function () {
        this.initializeValue('user_name');
    }
});