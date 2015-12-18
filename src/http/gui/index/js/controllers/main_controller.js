App.MainController = Ember.Controller.extend({
    needs: ['global'],
    global: Ember.computed.alias('controllers.global'),

    actions: {
        syncAndReload: function () {
            this.get('global').callSync();
        }
    }
});