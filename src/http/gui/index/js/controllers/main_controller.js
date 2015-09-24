App.MainController = Ember.Controller.extend({
    needs: ['global'],
    global: Ember.computed.alias('controllers.global'),
    actions: {
        createNewFile: function () {
            console.log('asdfasdf');
        }
    }
});