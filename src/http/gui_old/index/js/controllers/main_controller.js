// This is a prototype controller for the main page.

App.MainController = Ember.Controller.extend({
    needs: ['global'],
    global: Ember.computed.alias('controllers.global'),

    actions: {
        syncAndReload: function () {
            this.get('global').callSync();
        }
    }
});